"""患者合并：保留记录资格、幂等重复提交、版本核对、并发竞争、
三入口一致、去标识化兼容。

并发用例全部使用“屏障 + 事件”固定交错顺序，单次运行即可稳定复现，
不靠多跑几轮碰运气。日志（INFO 级）会打印每次请求的输入与判定依据。
"""

import json
import logging
import threading

import pytest
from fastapi.testclient import TestClient

import fhir_pipeline.service as service_mod
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


def assert_no_references_to(svc: IngestionService, patient_id: str) -> None:
    """全局不变量：除该患者自身的合并标记外，没有任何资源引用它。"""
    forbidden = f"Patient/{patient_id}"
    for resource in svc.store.all_resources():
        body = resource.body
        for field in ("subject", "encounter"):
            ref = body.get(field)
            assert not (isinstance(ref, dict) and ref.get("reference") == forbidden), (
                f"悬空引用未清理: {resource.resource_type}/{resource.resource_id} "
                f".{field} -> {forbidden}")


class _IngestGate:
    """在 service 的引用校验处插桩：当目标资源 id 的请求通过校验后阻塞，
    直到测试主线程放行，用于固定“校验通过 -> 合并完成 -> 再落库”的交错。"""

    def __init__(self, target_id: str) -> None:
        self.passed = threading.Event()
        self.release = threading.Event()
        self._target_id = target_id
        self._orig = service_mod.validate_references

    def __enter__(self):
        orig, target_id = self._orig, self._target_id
        passed, release = self.passed, self.release

        def gated(body, store):
            orig(body, store)  # 先按真实规则校验（此刻引用目标仍有效）
            if body.get("id") == target_id:
                passed.set()
                assert release.wait(5), "插桩等待超时，测试本身有误"

        service_mod.validate_references = gated
        return self

    def __exit__(self, *exc):
        service_mod.validate_references = self._orig
        return False


def run_after_gate(gate: _IngestGate, gated_job, concurrent_job):
    """启动 gated_job（在校验点挂起），再执行 concurrent_job，最后放行。

    返回 (gated_result, gated_error)：二者恰有一个非 None。
    """
    result: dict = {}

    def worker():
        try:
            result["ok"] = gated_job()
        except PipelineError as exc:
            result["err"] = exc

    t = threading.Thread(target=worker)
    t.start()
    assert gate.passed.wait(5), "被拦截请求未到达校验点"
    concurrent_job()       # 合并在请求仍挂起时完整提交
    gate.release.set()
    t.join()
    return result.get("ok"), result.get("err")


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


# ---------- 保留记录资格：已作废记录不能当保留记录 ----------

def test_already_merged_patient_cannot_be_survivor(caplog):
    """P2 已并入 P1 后，再拿 P2 当保留记录并 P3：必须以专门原因拒绝，
    且 P3 及其名下资源原封不动，任何引用都不指向已作废的 P2。"""
    svc = make_service()
    svc.ingest(P3)
    svc.ingest({"resourceType": "Encounter", "id": "E3", "status": "finished",
                "subject": {"reference": "Patient/P3"}})
    svc.merge_patients("P1", "P2", **merge_kwargs())

    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P2", "P3", expected_survivor_version=2,
                           expected_merged_version=1)
    assert exc.value.reason is RejectReason.SURVIVOR_MERGED
    ctx = exc.value.context
    assert ctx["survivorId"] == "P2"
    assert ctx["survivorMergedInto"] == "P1"   # 说清正确的保留记录是谁
    assert ctx["survivorCurrentVersion"] == 2
    assert ctx["mergedCurrentVersion"] == 1

    # 被合并方完全没被动过：仍 active、无 replaced-by 标记、版本不变
    p3 = svc.get("Patient", "P3")
    assert p3.version == 1
    assert "link" not in p3.body and p3.body.get("active", True) is not False
    # 原本挂在 P3 上的就诊没有被改指到已作废的 P2
    assert svc.get("Encounter", "E3").body["subject"]["reference"] == "Patient/P3"
    assert_no_references_to(svc, "P2")

    texts = [r.getMessage() for r in caplog.records]
    assert any("survivor_merged" in t for t in texts)


def test_survivor_merged_rejection_is_distinguishable_from_version_conflict():
    """同样是 409 类冲突：保留记录作废 vs 版本落后，原因必须可区分。"""
    svc = make_service()
    svc.ingest(P3)
    svc.merge_patients("P1", "P2", **merge_kwargs())
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P2", "P3", expected_survivor_version=2,
                           expected_merged_version=1)
    assert exc.value.reason is RejectReason.SURVIVOR_MERGED
    assert exc.value.reason is not RejectReason.VERSION_CONFLICT


def test_three_entries_agree_on_survivor_merged_rejection(monkeypatch, capsys):
    """Python/CLI/HTTP 三入口对“作废记录当保留记录”给出一致拒绝结论。"""
    # Python
    svc = make_service()
    svc.ingest(P3)
    svc.merge_patients("P1", "P2", **merge_kwargs())
    with pytest.raises(PipelineError) as py_exc:
        svc.merge_patients("P2", "P3", expected_survivor_version=2,
                           expected_merged_version=1)
    assert py_exc.value.reason is RejectReason.SURVIVOR_MERGED
    assert py_exc.value.context["survivorMergedInto"] == "P1"

    # CLI：非零退出码 + stderr 同一 reason/context
    svc2 = make_service()
    svc2.ingest(P3)
    svc2.merge_patients("P1", "P2", **merge_kwargs())
    monkeypatch.setattr(cli, "IngestionService", lambda: svc2)
    rc = cli.main(["merge", "P2", "P3",
                   "--survivor-version", "2", "--merged-version", "1"])
    assert rc == cli.EXIT_REJECTED
    err = json.loads(capsys.readouterr().err)
    assert err["reason"] == "survivor_merged"
    assert err["context"]["survivorMergedInto"] == "P1"

    # HTTP：409 + 同一 reason/context
    api.reset_service()
    client = TestClient(api.app)
    for body in (P1, P2, ENC_ON_P2, OBS_ON_P2, P3):
        assert client.post(f"/fhir/{body['resourceType']}", json=body).status_code == 200
    assert client.post("/fhir/Patient/P1/$merge",
                       json={"mergedId": "P2", "expectedSurvivorVersion": 1,
                             "expectedMergedVersion": 1}).status_code == 200
    r = client.post("/fhir/Patient/P2/$merge",
                    json={"mergedId": "P3", "expectedSurvivorVersion": 2,
                          "expectedMergedVersion": 1})
    assert r.status_code == 409
    body = r.json()
    assert body["reason"] == "survivor_merged"
    assert body["context"]["survivorMergedInto"] == "P1"
    # 被拒绝后 P3 没有被改一半：仍是版本 1、无合并标记
    hist = client.get("/fhir/Patient/P3/_history").json()
    assert hist["currentVersion"] == 1
    assert "link" not in hist["versions"][0]["resource"]


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


def test_update_on_survivor_first_makes_merge_rejected_deterministically():
    """固定交错：保留记录 P1 先被更新到 v2，随后提交的合并必须因
    保留记录版本落后被明确拒绝（staleResource=survivor，报出双方
    当前版本），且库内数据没有被改一半。"""
    svc = make_service()
    svc.ingest({**P1, "gender": "female"}, expected_version=1)  # P1 -> v2

    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "P2", **merge_kwargs())
    err = exc.value
    assert err.reason is RejectReason.VERSION_CONFLICT
    assert err.context["staleResource"] == "survivor"
    assert err.context["expectedSurvivorVersion"] == 1
    assert err.context["currentSurvivorVersion"] == 2
    assert err.context["currentMergedVersion"] == 1  # 双方当前版本都报出
    # 被拒绝方未产生任何副作用
    assert "link" not in svc.get("Patient", "P2").body
    assert svc.get("Patient", "P2").version == 1
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P2"
    assert svc.get("Patient", "P1").body["gender"] == "female"  # 更新未被回滚


def test_merge_first_then_stale_survivor_update_rejected_deterministically():
    """固定交错的另一半：合并先提交后，拿着旧版本的保留记录更新被
    version_conflict 拒绝；按当前版本重试则成功，两种情况下合并的
    改指结果都完好、不被覆盖。"""
    svc = make_service()
    svc.merge_patients("P1", "P2", **merge_kwargs())  # P1 仍 v1（保留记录不改写）

    # 模拟“另一个人基于更早读到的 v0 更新”：版本落后，必须拒绝且不覆盖
    with pytest.raises(PipelineError) as exc:
        svc.ingest({**P1, "gender": "female"}, expected_version=0)
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    assert exc.value.context["currentVersion"] == 1
    assert svc.get("Patient", "P1").body["gender"] == "male"
    # 合并改指结果不受影响
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P1"
    assert_no_references_to(svc, "P2")

    # 按当前版本重试：成功，合并结果与新更新并存且互不覆盖
    retried = svc.ingest({**P1, "gender": "female"}, expected_version=1)
    assert retried.status.value == "updated" and retried.version == 2
    assert svc.get("Patient", "P1").body["gender"] == "female"
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P1"


def test_late_create_while_merge_commits_is_rejected_deterministically(caplog):
    """固定交错：新建已通过引用校验但尚未落库时，合并完整提交；
    新建在锁内复核处必须被明确拒绝（merged_resource），绝不悄悄写进去。

    这正是此前线上“合并报成功后新资源仍指向作废患者”的交错，
    单次运行即可稳定复现，不依赖调度运气。
    """
    svc = make_service()
    new_encounter = {"resourceType": "Encounter", "id": "E9",
                     "status": "planned",
                     "subject": {"reference": "Patient/P2"}}
    with _IngestGate("E9") as gate:
        ok, err = run_after_gate(
            gate,
            gated_job=lambda: svc.ingest(new_encounter),
            concurrent_job=lambda: svc.merge_patients("P1", "P2", **merge_kwargs()),
        )

    assert ok is None and err is not None
    assert err.reason is RejectReason.MERGED_RESOURCE
    assert err.context["mergedInto"] == "P1"
    assert svc.get("Encounter", "E9") is None  # 没有半写入
    assert_no_references_to(svc, "P2")
    # 日志可见输入与拒绝依据
    texts = [r.getMessage() for r in caplog.records]
    assert any("E9" in t for t in texts)
    assert any("merged_resource" in t for t in texts)


def test_create_committed_before_merge_gets_repointed_deterministically():
    """固定交错的另一半：新建先落库、合并随后提交。

    合并必须把刚写入的资源一并改指（不漏扫），结束后同样没有悬空引用；
    合并也不覆盖新资源的其它字段。这里用顺序执行精确表达这一交错，
    与上一个用例合起来覆盖新建/合并竞争的两种结果，且无需碰运气。
    """
    svc = make_service()
    created = svc.ingest({"resourceType": "Encounter", "id": "E9",
                         "status": "arrived",
                         "subject": {"reference": "Patient/P2"}})
    assert created.status.value == "created"

    result = svc.merge_patients("P1", "P2", **merge_kwargs())
    repointed_ids = {(r["resourceType"], r["id"]) for r in result.repointed}
    assert ("Encounter", "E9") in repointed_ids  # 新写入的也被扫到并改指

    e9 = svc.get("Encounter", "E9")
    assert e9.version == 2  # 改指产生新版本，其它字段保留
    assert e9.body["status"] == "arrived"
    assert e9.body["subject"]["reference"] == "Patient/P1"
    assert_no_references_to(svc, "P2")
    # 历史可追溯：改指前的 v1 仍指向 P2
    assert [e.version for e in svc.get_history("Encounter", "E9").entries] == [1, 2]


def test_late_update_while_merge_commits_is_rejected_deterministically():
    """固定交错：对 E1 的更新通过锁外校验后挂起，合并先提交并把 E1
    改指到 v2；更新带着旧的 If-Match:1 落库时必须被明确拒绝
    （version_conflict，报出双方版本），E1 保持合并改指后的版本，
    既不回滚成旧引用也不被盖掉 status。"""
    svc = make_service()
    updated_e1 = {**ENC_ON_P2, "status": "planned"}  # subject 仍指向 P2
    with _IngestGate("E1") as gate:
        ok, err = run_after_gate(
            gate,
            gated_job=lambda: svc.ingest(updated_e1, expected_version=1),
            concurrent_job=lambda: svc.merge_patients("P1", "P2", **merge_kwargs()),
        )

    assert ok is None and err is not None
    assert err.reason is RejectReason.VERSION_CONFLICT
    # 乐观锁版本信息必须报出来：客户端基于 v1，库内已是合并改指后的 v2
    assert err.context["expectedVersion"] == 1
    assert err.context["currentVersion"] == 2
    # 合并的改指结果没有被后来的更新覆盖
    e1 = svc.get("Encounter", "E1")
    assert e1.version == 2
    assert e1.body["status"] == "finished"
    assert e1.body["subject"]["reference"] == "Patient/P1"
    assert_no_references_to(svc, "P2")


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
