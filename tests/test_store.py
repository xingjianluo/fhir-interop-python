"""版本化存储：幂等去重、乐观锁冲突、并发安全、历史追溯。"""

import threading

import pytest

from fhir_pipeline.errors import PipelineError, RejectReason
from fhir_pipeline.store import ResourceStore


PATIENT_BODY = {"resourceType": "Patient", "id": "P1", "gender": "male",
                "birthDate": "1980-01-01"}


def test_create_then_duplicate_submission_yields_single_record():
    store = ResourceStore()
    first = store.create("Patient", "P1", PATIENT_BODY, "hash-1")
    again = store.create("Patient", "P1", PATIENT_BODY, "hash-1")

    assert first.status.value == "created"
    assert again.status.value == "duplicate"
    assert again.reason == RejectReason.DUPLICATE_SUBMISSION.value
    # 不得出现重复记录 / 新版本
    assert store.history("Patient", "P1").current_version == 1
    assert len(store.all_resources()) == 1


def test_create_existing_id_is_rejected_with_conflict():
    store = ResourceStore()
    store.create("Patient", "P1", PATIENT_BODY, "hash-1")
    with pytest.raises(PipelineError) as exc:
        store.create("Patient", "P1", {**PATIENT_BODY, "gender": "female"}, "hash-2")
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    assert exc.value.context["existingVersion"] == 1


def test_update_increments_version_and_appends_history():
    store = ResourceStore()
    store.create("Patient", "P1", PATIENT_BODY, "hash-1")
    result = store.update("Patient", "P1", {**PATIENT_BODY, "gender": "female"},
                          "hash-2", expected_version=1)
    assert result.status.value == "updated"
    assert result.version == 2
    history = store.history("Patient", "P1")
    assert [e.version for e in history.entries] == [1, 2]
    assert history.entries[0].body["gender"] == "male"  # 旧版本内容可追溯
    assert store.get("Patient", "P1").body["gender"] == "female"


def test_stale_version_update_is_conflict_and_data_unchanged():
    store = ResourceStore()
    store.create("Patient", "P1", PATIENT_BODY, "hash-1")
    store.update("Patient", "P1", {**PATIENT_BODY, "gender": "female"},
                 "hash-2", expected_version=1)
    # 另一个客户端仍基于 v1 提交 -> 必须明确冲突拒绝
    with pytest.raises(PipelineError) as exc:
        store.update("Patient", "P1", {**PATIENT_BODY, "gender": "other"},
                     "hash-3", expected_version=1)
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    assert exc.value.context == {"resourceType": "Patient", "id": "P1",
                                 "expectedVersion": 1, "currentVersion": 2}
    # 数据保持为 v2，未被 v1 基础上的写入覆盖（不丢更新）
    assert store.get("Patient", "P1").body["gender"] == "female"


def test_resubmitting_same_update_is_idempotent():
    store = ResourceStore()
    store.create("Patient", "P1", PATIENT_BODY, "hash-1")
    new_body = {**PATIENT_BODY, "gender": "female"}
    first = store.update("Patient", "P1", new_body, "hash-2", expected_version=1)
    retry = store.update("Patient", "P1", new_body, "hash-2", expected_version=2)
    assert first.status.value == "updated"
    assert retry.status.value == "duplicate"
    assert store.history("Patient", "P1").current_version == 2


def test_concurrent_updates_exactly_one_wins():
    store = ResourceStore()
    store.create("Patient", "P1", PATIENT_BODY, "hash-1")
    outcomes: list[str] = []

    def worker(suffix: str) -> None:
        try:
            store.update("Patient", "P1",
                         {**PATIENT_BODY, "birthDate": f"198{suffix}-01-01"},
                         f"hash-{suffix}", expected_version=1)
            outcomes.append("updated")
        except PipelineError as exc:
            assert exc.reason is RejectReason.VERSION_CONFLICT
            outcomes.append("conflict")

    threads = [threading.Thread(target=worker, args=(str(i),)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count("updated") == 1
    assert outcomes.count("conflict") == 9
    assert store.history("Patient", "P1").current_version == 2


def test_idempotency_key_bound_to_other_resource_conflicts():
    store = ResourceStore()
    store.create("Patient", "P1", PATIENT_BODY, "hash-1", idempotency_key="KEY-1")
    with pytest.raises(PipelineError) as exc:
        store.create("Patient", "P2", PATIENT_BODY, "hash-9", idempotency_key="KEY-1")
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
