"""患者合并：引用改指、墓碑可追溯、版本冲突、并发安全、幂等、去标识化。"""

import threading

import pytest

from fhir_pipeline.errors import PipelineError, RejectReason
from fhir_pipeline.service import IngestionService

P_KEEP = {"resourceType": "Patient", "id": "P1", "gender": "female",
          "birthDate": "1990-03-04"}
P_DUP = {"resourceType": "Patient", "id": "P2", "gender": "male",
         "birthDate": "1985-07-08"}
ENC_P2 = {"resourceType": "Encounter", "id": "E2", "status": "finished",
          "subject": {"reference": "Patient/P2"}}
OBS_P2 = {"resourceType": "Observation", "id": "O2", "status": "final",
          "code": {"text": "Glucose"}, "subject": {"reference": "Patient/P2"},
          "encounter": {"reference": "Encounter/E2"},
          "valueQuantity": {"value": 7.1, "unit": "mmol/L"}}
ENC_P1 = {"resourceType": "Encounter", "id": "E1", "status": "planned",
          "subject": {"reference": "Patient/P1"}}


def _dataset() -> IngestionService:
    svc = IngestionService()
    svc.ingest(P_KEEP)
    svc.ingest(P_DUP)
    svc.ingest(ENC_P2)
    svc.ingest(OBS_P2)
    svc.ingest(ENC_P1)
    return svc


# ---------- 基本改写 ----------

def test_merge_repoints_resources_and_marks_tombstone(caplog):
    svc = _dataset()
    result = svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    assert result.status.value == "merged"
    repointed = {(r.resource_type, r.resource_id) for r in result.repointed}
    assert repointed == {("Encounter", "E2"), ("Observation", "O2")}
    # 所有资源不再指向被合并 id
    assert svc.get("Encounter", "E2").body["subject"]["reference"] == "Patient/P1"
    assert svc.get("Observation", "O2").body["subject"]["reference"] == "Patient/P1"
    # 原本挂 P1 的资源不受影响
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P1"

    # 被合并记录仍在（未删除），是可识别的墓碑
    tombstone = svc.get("Patient", "P2")
    assert tombstone.body["active"] is False
    assert any(
        l["type"] == "replaced" and l["other"]["reference"] == "Patient/P1"
        for l in tombstone.body["link"]
    )
    # 保留方记录反向链接
    survivor = svc.get("Patient", "P1")
    assert survivor.body["active"] is True
    assert any(
        l["type"] == "replaces" and l["other"]["reference"] == "Patient/P2"
        for l in survivor.body["link"]
    )
    # 改指资源各升一个版本
    assert svc.get("Encounter", "E2").version == 2
    assert svc.get("Observation", "O2").version == 2
    assert any("合并成功" in r.getMessage() for r in caplog.records)


def test_tombstone_and_both_chains_remain_in_history():
    svc = _dataset()
    svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    # 合并前后各自历史都可追溯
    h2 = svc.get_history("Patient", "P2")
    assert [e.version for e in h2.entries] == [1, 2]
    assert "link" not in h2.entries[0].body  # v1 是合并前原状
    assert h2.entries[1].body["active"] is False
    h1 = svc.get_history("Patient", "P1")
    assert h1.entries[-1].body["active"] is True
    # 被改指就诊的历史链上也能看到引用从 P2 变到 P1
    he = svc.get_history("Encounter", "E2")
    assert he.entries[0].body["subject"]["reference"] == "Patient/P2"
    assert he.entries[1].body["subject"]["reference"] == "Patient/P1"


# ---------- 幂等 ----------

def test_repeated_merge_is_idempotent_no_double_rewrite(caplog):
    svc = _dataset()
    first = svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    # 同一请求即使携带最初看到的旧版本重复提交，也不得二次改写
    again = svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    assert first.status.value == "merged"
    assert again.status.value == "duplicate"
    assert again.reason == "duplicate_submission"
    # 版本/历史不再增长
    assert svc.get_history("Patient", "P2").current_version == 2
    assert svc.get_history("Patient", "P1").current_version == 2
    assert svc.get_history("Encounter", "E2").current_version == 2
    assert svc.get_history("Observation", "O2").current_version == 2
    assert {(r.resource_type, r.resource_id) for r in again.repointed} == \
        {(r.resource_type, r.resource_id) for r in first.repointed}
    assert any("重复提交" in r.getMessage() for r in caplog.records)


def test_merge_idempotency_key_misuse_is_rejected():
    svc = _dataset()
    svc.ingest({"resourceType": "Patient", "id": "P3", "gender": "unknown",
                "birthDate": "1970-01-01"})
    svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
        idempotency_key="MERGE-1",
    )
    # 同一幂等键绑定到另一对患者 -> 冲突拒绝
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients(
            surviving_patient_id="P1", merged_patient_id="P3",
            expected_surviving_version=2, expected_merged_version=1,
            idempotency_key="MERGE-1",
        )
    assert exc.value.reason is RejectReason.VERSION_CONFLICT


# ---------- 版本冲突 ----------

def test_stale_survivor_version_rejected_with_distinguishable_sides():
    svc = _dataset()
    svc.ingest({**P_KEEP, "gender": "other"})  # P1 已到 v2
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients(
            surviving_patient_id="P1", merged_patient_id="P2",
            expected_surviving_version=1, expected_merged_version=1,
        )
    assert exc.value.reason is RejectReason.MERGE_VERSION_CONFLICT
    ctx = exc.value.context
    assert ctx["surviving"]["stale"] is True
    assert ctx["surviving"] == {"id": "P1", "expectedVersion": 1,
                                "currentVersion": 2, "stale": True}
    assert ctx["merged"]["stale"] is False
    # 拒绝后没有任何资源被改动
    assert svc.get("Encounter", "E2").body["subject"]["reference"] == "Patient/P2"
    assert svc.get("Patient", "P2").body.get("active", True) is True


def test_stale_merged_patient_version_rejected_with_current_version():
    svc = _dataset()
    svc.ingest({**P_DUP, "gender": "female"})  # P2 已到 v2
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients(
            surviving_patient_id="P1", merged_patient_id="P2",
            expected_surviving_version=1, expected_merged_version=1,
        )
    assert exc.value.reason is RejectReason.MERGE_VERSION_CONFLICT
    ctx = exc.value.context
    assert ctx["surviving"]["stale"] is False
    assert ctx["merged"]["stale"] is True
    assert ctx["merged"]["currentVersion"] == 2


def test_merge_missing_patient_is_not_found():
    svc = IngestionService()
    svc.ingest(P_KEEP)
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients(
            surviving_patient_id="P1", merged_patient_id="NOPE",
            expected_surviving_version=1, expected_merged_version=1,
        )
    assert exc.value.reason is RejectReason.RESOURCE_NOT_FOUND


def test_cannot_merge_patient_into_a_tombstone():
    svc = _dataset()
    svc.ingest({"resourceType": "Patient", "id": "P3", "gender": "unknown",
                "birthDate": "1970-01-01"})
    # 先把 P2 并入 P1，P2 成为墓碑；再试图把 P3 并入 P2
    svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients(
            surviving_patient_id="P2", merged_patient_id="P3",
            expected_surviving_version=2, expected_merged_version=1,
        )
    assert exc.value.reason is RejectReason.PATIENT_ALREADY_MERGED
    assert exc.value.context["replacementId"] == "P1"


# ---------- 合并后对新数据的处置 ----------

def test_new_reference_to_merged_patient_is_rejected(caplog):
    svc = _dataset()
    svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    new_enc = {"resourceType": "Encounter", "id": "E9", "status": "arrived",
               "subject": {"reference": "Patient/P2"}}
    with pytest.raises(PipelineError) as exc:
        svc.ingest(new_enc)
    assert exc.value.reason is RejectReason.PATIENT_ALREADY_MERGED
    assert exc.value.context["replacementId"] == "P1"
    assert not svc.get("Encounter", "E9")  # 没有被默默写进去
    # 改指保留记录后可正常写入
    new_enc["subject"]["reference"] = "Patient/P1"
    result = svc.ingest(new_enc)
    assert result.status.value == "created"
    assert any("Patient/P2" in r.getMessage() for r in caplog.records)


def test_create_using_tombstone_id_is_distinguishably_rejected():
    svc = _dataset()
    svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    # 不带期望版本（语义为新建）再次写入 P2 -> 明确的合并结论
    with pytest.raises(PipelineError) as exc:
        svc.ingest({**P_DUP, "gender": "female"})
    assert exc.value.reason is RejectReason.PATIENT_ALREADY_MERGED
    assert exc.value.context["replacementId"] == "P1"
    assert svc.get("Patient", "P2").body["gender"] == "male"


def test_tombstone_update_is_rejected_even_with_correct_version():
    svc = _dataset()
    svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    # 即使带着墓碑当前版本 v2 也不允许覆盖
    with pytest.raises(PipelineError) as exc:
        svc.ingest({**P_DUP, "gender": "female"}, expected_version=2)
    assert exc.value.reason is RejectReason.PATIENT_ALREADY_MERGED
    assert exc.value.context["replacementId"] == "P1"
    # 内容未被覆盖
    assert svc.get("Patient", "P2").body["gender"] == "male"


# ---------- 并发 ----------

def test_concurrent_identical_merge_requests_only_one_merge():
    """同一对患者的并发重复提交：恰好一次真实合并，其余幂等返回。"""
    svc = _dataset()
    statuses: list[str] = []
    barrier = threading.Barrier(8)

    def merge() -> None:
        barrier.wait()
        result = svc.merge_patients(
            surviving_patient_id="P1", merged_patient_id="P2",
            expected_surviving_version=1, expected_merged_version=1,
        )
        statuses.append(result.status.value)

    threads = [threading.Thread(target=merge) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert statuses.count("merged") == 1
    assert statuses.count("duplicate") == 7
    assert svc.get_history("Patient", "P2").current_version == 2
    assert svc.get_history("Patient", "P1").current_version == 2
    assert svc.get_history("Encounter", "E2").current_version == 2


def test_two_concurrent_distinct_merges_only_one_wins():
    """两个不同方向的并发合并（P1<-P2 与 P2<-P1）只能有一个成功。"""
    svc = _dataset()
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def merge(survivor: str, merged: str) -> None:
        barrier.wait()
        try:
            svc.merge_patients(
                surviving_patient_id=survivor, merged_patient_id=merged,
                expected_surviving_version=1, expected_merged_version=1,
            )
            outcomes.append("merged")
        except PipelineError as exc:
            assert exc.reason in (
                RejectReason.MERGE_VERSION_CONFLICT,
                RejectReason.PATIENT_ALREADY_MERGED,
            )
            outcomes.append("conflict")

    t1 = threading.Thread(target=merge, args=("P1", "P2"))
    t2 = threading.Thread(target=merge, args=("P2", "P1"))
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert outcomes.count("merged") == 1
    assert outcomes.count("conflict") == 1
    # 恰有一条患者成为墓碑
    p1 = svc.get("Patient", "P1").body
    p2 = svc.get("Patient", "P2").body
    tombstone_id = "P2" if p2.get("active", True) is False else "P1"
    survivor_id = "P1" if tombstone_id == "P2" else "P2"
    assert p1.get("active", True) != p2.get("active", True)
    # 所有引用都指向幸存方，无悬空指向墓碑
    for r in svc.store.all_resources():
        subject = r.body.get("subject")
        if isinstance(subject, dict):
            assert subject["reference"] == f"Patient/{survivor_id}"


def test_merge_concurrent_with_update_to_merged_patient_no_lost_data():
    svc = _dataset()
    start = threading.Event()
    errors: list[PipelineError] = []

    def do_merge() -> None:
        start.wait()
        try:
            svc.merge_patients(
                surviving_patient_id="P1", merged_patient_id="P2",
                expected_surviving_version=1, expected_merged_version=1,
            )
        except PipelineError as exc:
            errors.append(exc)

    def do_update() -> None:
        start.wait()
        try:
            svc.ingest({**P_DUP, "gender": "female"}, expected_version=1)
        except PipelineError as exc:
            errors.append(exc)

    tm = threading.Thread(target=do_merge)
    tu = threading.Thread(target=do_update)
    tm.start(); tu.start()
    start.set()
    tm.join(); tu.join()

    # 恰好一方成功；另一方必须得到明确冲突，而不是静默覆盖
    assert len(errors) == 1
    p2 = svc.get("Patient", "P2")
    if any(e.reason is RejectReason.MERGE_VERSION_CONFLICT for e in errors):
        # 更新赢：P2 是普通 v2，内容为更新者写入的 female，引用仍挂 P2
        assert p2.version == 2 and p2.body["gender"] == "female"
        assert svc.get("Encounter", "E2").body["subject"]["reference"] == "Patient/P2"
    else:
        # 合并赢：更新必被墓碑保护拒绝，且 P2 已墓碑、引用改指 P1
        assert p2.body["active"] is False
        assert p2.body["gender"] == "male"  # 更新内容没被悄悄带进墓碑
        assert svc.get("Encounter", "E2").body["subject"]["reference"] == "Patient/P1"
        assert errors[0].reason is RejectReason.PATIENT_ALREADY_MERGED


def test_merge_concurrent_with_new_encounter_no_dangling_reference():
    svc = IngestionService()
    svc.ingest(P_KEEP)
    svc.ingest(P_DUP)
    start = threading.Event()
    errors: list[PipelineError] = []

    def do_merge() -> None:
        start.wait()
        try:
            svc.merge_patients(
                surviving_patient_id="P1", merged_patient_id="P2",
                expected_surviving_version=1, expected_merged_version=1,
            )
        except PipelineError as exc:
            errors.append(exc)

    def do_encounter() -> None:
        start.wait()
        try:
            svc.ingest(ENC_P2)
        except PipelineError as exc:
            errors.append(exc)

    tm = threading.Thread(target=do_merge)
    te = threading.Thread(target=do_encounter)
    tm.start(); te.start()
    start.set()
    tm.join(); te.join()

    # 关键不变量：合并完成后不存在任何当前资源指向被合并 id P2
    for r in svc.store.all_resources():
        subject = r.body.get("subject")
        if isinstance(subject, dict):
            assert subject["reference"] != "Patient/P2", (
                f"{r.resource_type}/{r.resource_id} 仍悬空指向 Patient/P2"
            )
    # 若就诊写入成功（发生在合并前），它一定已被合并改指 P1；
    # 若发生在合并后，则被 PATIENT_ALREADY_MERGED 拒绝
    enc = svc.get("Encounter", "E2")
    if enc is not None:
        assert enc.body["subject"]["reference"] == "Patient/P1"
    else:
        assert any(e.reason is RejectReason.PATIENT_ALREADY_MERGED for e in errors)


# ---------- 去标识化 ----------

def test_deid_after_merge_references_closed_and_stable():
    svc = _dataset()
    svc.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    out1 = svc.deidentify_all()
    resources = out1["resources"]

    # 所有引用目标都在去标识化集合内（闭合，可解析）
    ids = {r["id"] for r in resources}
    for r in resources:
        for field_name in ("subject", "encounter"):
            ref = r.get(field_name)
            if isinstance(ref, dict):
                assert ref["reference"].split("/", 1)[1] in ids, (
                    f"{r['id']}.{field_name} -> {ref['reference']} 悬空"
                )
        for link in r.get("link") or []:
            assert link["other"]["reference"].split("/", 1)[1] in ids

    # 墓碑链接与保留方互链在伪标识空间中仍然对齐
    tombstone = next(r for r in resources if r["active"] is False)
    survivor = next(r for r in resources
                    if r["resourceType"] == "Patient" and r["active"] is True)
    assert tombstone["link"][0]["other"]["reference"] == \
        f"Patient/{survivor['id']}"
    assert any(
        l["other"]["reference"] == f"Patient/{tombstone['id']}"
        and l["type"] == "replaces"
        for l in survivor["link"]
    )

    # 重复执行：映射不增加、结果不变
    mapping_before = set(out1["mapping"])
    out2 = svc.deidentify_all()
    assert set(out2["mapping"]) == mapping_before
    assert out2["resources"] == out1["resources"]
    # 被改指的就诊仍指向保留患者的同一伪标识
    e2 = next(r for r in out2["resources"]
              if r["resourceType"] == "Encounter" and r.get("status") == "finished")
    assert e2["subject"]["reference"] == f"Patient/{survivor['id']}"
