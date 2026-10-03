"""患者合并：幂等重复提交、版本核对、并发竞争、三入口一致、去标识化兼容。

日志（INFO 级）会打印每次合并的输入与判定依据，便于核对结论来源。
"""

import json
import logging
import threading

import pytest
from fastapi.testclient import TestClient

from fhir_pipeline import api, cli
from fhir_pipeline.errors import PipelineError, RejectReason
from fhir_pipeline.service import IngestionService

P1 = {"resourceType": "Patient", "id": "P1",
      "gender": "male", "birthDate": "1980-01-01"}
P2 = {"resourceType": "Patient", "id": "P2",
      "gender": "male", "birthDate": "1980-01-02"}
P3 = {"resourceType": "Patient", "id": "P3",
      "gender": "female", "birthDate": "1981-05-06"}
ENC_ON_P2 = {"resourceType": "Encounter", "id": "E1", "status": "finished",
             "subject": {"reference": "Patient/P2"}}
OBS_ON_P2 = {"resourceType": "Observation", "id": "O1", "status": "final",
             "code": {"text": "glucose"},
             "subject": {"reference": "Patient/P2"},
             "encounter": {"reference": "Encounter/E1"},
             "valueQuantity": {"value": 5.5}}


def make_service() -> IngestionService:
    """构造同一初始数据集：P1/P2 两名患者，E1/O1 挂在 P2 下。"""
    svc = IngestionService()
    svc.ingest(P1)
    svc.ingest(P2)
    svc.ingest(ENC_ON_P2)
    svc.ingest(OBS_ON_P2)
    return svc


def merge_kwargs() -> dict:
    return {"expected_survivor_version": 1, "expected_merged_version": 1}


# ---------- 基本语义 ----------

def test_merge_repoints_references_and_marks_merged_record(caplog):
    svc = make_service()
    result = svc.merge_patients("P1", "P2", **merge_kwargs())

    assert result.status.value == "merged"
    assert result.survivor_version == 1
    assert {tuple(sorted(r.items())) for r in result.repointed} == {
        tuple(sorted({"resourceType": "Encounter", "id": "E1",
                      "fromVersion": 1, "toVersion": 2}.items())),
        tuple(sorted({"resourceType": "Observation", "id": "O1",
                      "fromVersion": 1, "toVersion": 2}.items())),
    }

    # 所有引用都改指保留记录，不再有资源指向被合并 id
    for resource in svc.store.all_resources():
        for ref_holder in (resource.body.get("subject"),
                           resource.body.get("encounter")):
            if isinstance(ref_holder, dict):
                assert ref_holder["reference"] != "Patient/P2"
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P1"
    assert svc.get("Observation", "O1").body["subject"]["reference"] == "Patient/P1"

    # 被合并记录未删除：状态可见、指向保留记录、历史可追溯
    merged = svc.get("Patient", "P2")
    assert merged is not None and merged.version == 2
    assert merged.body["active"] is False
    assert merged.body["link"] == [
        {"other": {"reference": "Patient/P1"}, "type": "replaced-by"}
    ]
    history = svc.get_history("Patient", "P2")
    assert [e.version for e in history.entries] == [1, 2]
    assert "link" not in history.entries[0].body  # 合并前版本原样保留
    # 被改指资源的历史同样可追溯
    assert [e.version for e in svc.get_history("Encounter", "E1").entries] == [1, 2]

    # 日志可见输入与判定依据
    texts = [r.getMessage() for r in caplog.records]
    assert any("收到合并请求" in t and "P1" in t and "P2" in t for t in texts)
    assert any("合并结论" in t and "merged" in t for t in texts)


def test_merge_same_patient_rejected():
    svc = make_service()
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "P1", **merge_kwargs())
    assert exc.value.reason is RejectReason.INVALID_VALUE


def test_merge_missing_patient_rejected():
    svc = make_service()
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "NOPE", **merge_kwargs())
    assert exc.value.reason is RejectReason.RESOURCE_NOT_FOUND


# ---------- 幂等重复提交 ----------

def test_merge_duplicate_submission_returns_same_conclusion(caplog):
    svc = make_service()
    first = svc.merge_patients("P1", "P2", **merge_kwargs())
    snapshot = {r.resource_id: r.version for r in svc.store.all_resources()}

    again = svc.merge_patients("P1", "P2", **merge_kwargs())
    assert again.status.value == "duplicate"
    assert again.reason == RejectReason.DUPLICATE_SUBMISSION.value
    # 首次结论原样返回（改指清单一致），且未二次改写任何版本
    assert again.repointed == first.repointed
    assert again.merged_version == first.merged_version
    assert {r.resource_id: r.version for r in svc.store.all_resources()} == snapshot
    assert len(svc.store.all_resources()) == 4

    # 显式幂等键重复提交同样幂等
    svc2 = make_service()
    key = "MERGE-REQ-1"
    a = svc2.merge_patients("P1", "P2", idempotency_key=key, **merge_kwargs())
    b = svc2.merge_patients("P1", "P2", idempotency_key=key, **merge_kwargs())
    assert a.status.value == "merged" and b.status.value == "duplicate"
    assert b.repointed == a.repointed


# ---------- 版本核对 ----------

def test_merge_stale_survivor_version_distinguishable():
    svc = make_service()
    svc.ingest({**P1, "gender": "female"}, expected_version=1)  # P1 -> v2
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "P2", **merge_kwargs())
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    ctx = exc.value.context
    assert ctx["staleResource"] == "survivor"
    assert ctx["currentSurvivorVersion"] == 2
    assert ctx["currentMergedVersion"] == 1
    # 合并被拒绝后数据未被改动
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P2"


def test_merge_stale_merged_version_distinguishable():
    svc = make_service()
    svc.ingest({**P2, "gender": "other"}, expected_version=1)  # P2 -> v2
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "P2", **merge_kwargs())
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    ctx = exc.value.context
    assert ctx["staleResource"] == "merged"
    assert ctx["currentMergedVersion"] == 2
    assert ctx["currentSurvivorVersion"] == 1


def test_merged_patient_cannot_be_merged_elsewhere():
    svc = make_service()
    svc.ingest(P3)
    svc.merge_patients("P1", "P2", **merge_kwargs())
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P3", "P2", expected_survivor_version=1,
                           expected_merged_version=2)
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    assert exc.value.context["mergedInto"] == "P1"


# ---------- 合并后的写入处置 ----------

def test_update_merged_patient_rejected_with_distinguishable_reason():
    svc = make_service()
    svc.merge_patients("P1", "P2", **merge_kwargs())
    with pytest.raises(PipelineError) as exc:
        svc.ingest({**P2, "gender": "female"}, expected_version=2)
    assert exc.value.reason is RejectReason.MERGED_RESOURCE
    assert exc.value.context["mergedInto"] == "P1"


def test_new_resource_referencing_merged_patient_rejected():
    svc = make_service()
    svc.merge_patients("P1", "P2", **merge_kwargs())
    dangling = {"resourceType": "Encounter", "id": "E2", "status": "planned",
                "subject": {"reference": "Patient/P2"}}
    with pytest.raises(PipelineError) as exc:
        svc.ingest(dangling)
    assert exc.value.reason is RejectReason.MERGED_RESOURCE
    assert exc.value.context["mergedInto"] == "P1"
    assert svc.get("Encounter", "E2") is None  # 没有默默写进去


def test_unversioned_overwrite_rejected_on_all_entries():
    svc = make_service()
    with pytest.raises(PipelineError) as exc:
        svc.ingest({**P1, "gender": "female"})  # 内容变了却不带版本
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    assert exc.value.context["missingPrecondition"] == "expected_version"
    assert svc.get("Patient", "P1").body["gender"] == "male"  # 未被覆盖
    # 内容一致的重复提交仍然幂等返回 duplicate
    dup = svc.ingest(P1)
    assert dup.status.value == "duplicate"


# ---------- 并发 ----------

def test_concurrent_merges_only_one_succeeds():
    svc = make_service()
    svc.ingest(P3)
    outcomes, errors = [], []

    def merge(target):
        try:
            outcomes.append(svc.merge_patients(
                target, "P2", expected_survivor_version=1,
                expected_merged_version=1))
        except PipelineError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=merge, args=(t,)) for t in ("P1", "P3")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 两个并发合并只有一个真正执行；另一个被明确拒绝（版本或已合并）
    assert len(outcomes) == 1 and outcomes[0].status.value == "merged"
    assert len(errors) == 1
    assert errors[0].reason is RejectReason.VERSION_CONFLICT
    # 最终不存在指向被合并 id 的悬空引用
    survivor = outcomes[0].survivor_id
    for resource in svc.store.all_resources():
        subject = resource.body.get("subject")
        if isinstance(subject, dict):
            assert subject["reference"] != "Patient/P2"
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == (
        f"Patient/{survivor}")


def test_merge_races_with_update_on_merged_patient():
    """合并与被合并患者的并发更新：必有一方被明确拒绝，互不覆盖。"""
    svc = make_service()
    results, errors = [], []
    barrier = threading.Barrier(2)

    def do_merge():
        barrier.wait()
        try:
            results.append(("merge", svc.merge_patients("P1", "P2",
                                                        **merge_kwargs())))
        except PipelineError as exc:
            errors.append(("merge", exc))

    def do_update():
        barrier.wait()
        try:
            results.append(("update", svc.ingest({**P2, "gender": "female"},
                                                 expected_version=1)))
        except PipelineError as exc:
            errors.append(("update", exc))

    threads = [threading.Thread(target=do_merge), threading.Thread(target=do_update)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 恰好一个成功、一个被拒绝
    assert len(results) == 1 and len(errors) == 1
    winner, loser = results[0][0], errors[0][0]
    assert {winner, loser} == {"merge", "update"}
    if winner == "merge":
        # 合并先完成：更新被合并记录 -> merged_resource
        assert errors[0][1].reason is RejectReason.MERGED_RESOURCE
        assert svc.get("Patient", "P2").body["active"] is False
    else:
        # 更新先完成：合并看到的版本落后 -> version_conflict 且可区分
        assert errors[0][1].reason is RejectReason.VERSION_CONFLICT
        assert errors[0][1].context["staleResource"] == "merged"
        assert svc.get("Patient", "P2").body["gender"] == "female"  # 未被回滚


def test_merge_races_with_new_encounter_on_merged_patient():
    """合并进行中向被合并患者挂新就诊：要么先入库后被改指，要么被明确拒绝，
    合并完成后绝不留指向被合并 id 的悬空引用。"""
    for _ in range(20):  # 多跑几轮覆盖不同交错
        svc = make_service()
        barrier = threading.Barrier(2)
        outcome = {}

        def do_merge(svc=svc, barrier=barrier, outcome=outcome):
            barrier.wait()
            outcome["merge"] = svc.merge_patients("P1", "P2", **merge_kwargs())

        def do_create(svc=svc, barrier=barrier, outcome=outcome):
            barrier.wait()
            try:
                outcome["create"] = svc.ingest(
                    {"resourceType": "Encounter", "id": "E9",
                     "status": "planned",
                     "subject": {"reference": "Patient/P2"}})
            except PipelineError as exc:
                outcome["create_error"] = exc

        t1, t2 = threading.Thread(target=do_merge), threading.Thread(target=do_create)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert outcome["merge"].status.value == "merged"
        if "create_error" in outcome:
            # 合并先完成：新建被明确拒绝，没有默默写入
            assert outcome["create_error"].reason is RejectReason.MERGED_RESOURCE
            assert svc.get("Encounter", "E9") is None
        else:
            # 新建先完成：合并必须把它一并改指，不留悬空引用
            assert outcome["create"].status.value == "created"
            assert svc.get("Encounter", "E9").body["subject"]["reference"] == (
                "Patient/P1")


# ---------- 三入口结论一致 ----------

def _merge_via_python(svc: IngestionService) -> dict:
    return svc.merge_patients("P1", "P2", **merge_kwargs()).to_dict()


def _merge_via_cli(monkeypatch, capsys) -> dict:
    svc = make_service()
    monkeypatch.setattr(cli, "IngestionService", lambda: svc)
    rc = cli.main(["merge", "P1", "P2",
                   "--survivor-version", "1", "--merged-version", "1"])
    assert rc == cli.EXIT_OK
    return json.loads(capsys.readouterr().out)


def _merge_via_http() -> dict:
    api.reset_service()
    client = TestClient(api.app)
    for body in (P1, P2, ENC_ON_P2, OBS_ON_P2):
        r = client.post(f"/fhir/{body['resourceType']}", json=body)
        assert r.status_code == 200
    r = client.post("/fhir/Patient/P1/$merge",
                    json={"mergedId": "P2", "expectedSurvivorVersion": 1,
                          "expectedMergedVersion": 1})
    assert r.status_code == 200
    return r.json()


def test_three_entries_agree_on_merge(monkeypatch, capsys):
    py = _merge_via_python(make_service())
    cl = _merge_via_cli(monkeypatch, capsys)
    http = _merge_via_http()
    for key in ("status", "survivorId", "mergedId",
                "survivorVersion", "mergedVersion", "repointed"):
        assert py[key] == cl[key] == http[key], f"入口结论不一致: {key}"


def test_three_entries_agree_on_stale_rejection(monkeypatch, capsys):
    # Python 入口
    svc = make_service()
    svc.ingest({**P2, "gender": "other"}, expected_version=1)  # P2 -> v2
    with pytest.raises(PipelineError) as py_exc:
        svc.merge_patients("P1", "P2", **merge_kwargs())
    assert py_exc.value.reason is RejectReason.VERSION_CONFLICT
    assert py_exc.value.context["staleResource"] == "merged"

    # CLI 入口：同样的拒绝，非零退出码
    svc2 = make_service()
    svc2.ingest({**P2, "gender": "other"}, expected_version=1)
    monkeypatch.setattr(cli, "IngestionService", lambda: svc2)
    rc = cli.main(["merge", "P1", "P2",
                   "--survivor-version", "1", "--merged-version", "1"])
    assert rc == cli.EXIT_REJECTED
    err = json.loads(capsys.readouterr().err)
    assert err["reason"] == "version_conflict"
    assert err["context"]["staleResource"] == "merged"

    # HTTP 入口：409 + 同样的 reason/context
    api.reset_service()
    client = TestClient(api.app)
    for body in (P1, P2, ENC_ON_P2, OBS_ON_P2):
        client.post(f"/fhir/{body['resourceType']}", json=body)
    client.put("/fhir/Patient/P2", json={**P2, "gender": "other"},
               headers={"If-Match": "1"})
    r = client.post("/fhir/Patient/P1/$merge",
                    json={"mergedId": "P2", "expectedSurvivorVersion": 1,
                          "expectedMergedVersion": 1})
    assert r.status_code == 409
    assert r.json()["reason"] == "version_conflict"
    assert r.json()["context"]["staleResource"] == "merged"


def test_http_merge_requires_versions():
    api.reset_service()
    client = TestClient(api.app)
    client.post("/fhir/Patient", json=P1)
    client.post("/fhir/Patient", json=P2)
    r = client.post("/fhir/Patient/P1/$merge", json={"mergedId": "P2"})
    assert r.status_code == 422
    assert r.json()["reason"] == "missing_field"


# ---------- 去标识化兼容 ----------

def _assert_reference_closure(resources: list[dict]) -> None:
    """去标识化结果中所有引用都能解析到结果集内的资源 id。"""
    ids = {(r["resourceType"], r["id"]) for r in resources}
    for resource in resources:
        refs = [resource.get("subject"), resource.get("encounter")]
        for link in resource.get("link") or []:
            refs.append(link.get("other") if isinstance(link, dict) else None)
        for ref in refs:
            if isinstance(ref, dict) and isinstance(ref.get("reference"), str):
                rtype, rid = ref["reference"].split("/")
                assert (rtype, rid) in ids, (
                    f"悬空引用: {resource['resourceType']}/{resource['id']} "
                    f"-> {ref['reference']}")


def test_deidentify_after_merge_is_closed_and_stable():
    svc = make_service()
    svc.merge_patients("P1", "P2", **merge_kwargs())

    first = svc.deidentify_all()
    # 引用闭合：含被合并患者的 link 在内，没有指向真实 id 的悬空引用
    _assert_reference_closure(first["resources"])
    serialized = json.dumps(first["resources"], ensure_ascii=False)
    assert "Patient/P2" not in serialized and '"P2"' not in serialized

    # 被合并记录仍在（可审计），其 link 指向保留记录的伪标识
    mapping = first["mapping"]
    pseudo_p1 = mapping["Patient/P1"]
    pseudo_p2 = mapping["Patient/P2"]
    merged_res = next(r for r in first["resources"]
                      if r["resourceType"] == "Patient" and r["id"] == pseudo_p2)
    assert merged_res["link"][0]["other"]["reference"] == f"Patient/{pseudo_p1}"
    assert merged_res["active"] is False

    # 重复执行：映射不增、结果不变
    second = svc.deidentify_all()
    assert second["mapping"] == first["mapping"]
    assert second["resources"] == first["resources"]

    # 合并前后历史都可追溯
    assert [e.version for e in svc.get_history("Patient", "P2").entries] == [1, 2]
    assert [e.version for e in svc.get_history("Patient", "P1").entries] == [1]


def test_merge_logs_inputs_and_decision_basis(caplog):
    caplog.set_level(logging.INFO, logger="fhir_pipeline.service")
    svc = make_service()
    svc.merge_patients("P1", "P2", idempotency_key="MERGE-LOG-1", **merge_kwargs())
    svc.merge_patients("P1", "P2", idempotency_key="MERGE-LOG-1", **merge_kwargs())
    texts = [r.getMessage() for r in caplog.records]
    # 输入内容：合并双方、版本、幂等键
    assert any("MERGE-LOG-1" in t and "P1" in t and "P2" in t for t in texts)
    # 判定依据：改指清单与重复提交的判定
    assert any("改指" in t for t in texts)
    assert any("duplicate" in t for t in texts)
