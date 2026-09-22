"""端到端服务：合入分流、重复提交、版本冲突、引用拒绝、日志可解释。"""

import json
import logging
import threading

import pytest

from fhir_pipeline.errors import PipelineError, RejectReason
from fhir_pipeline.service import IngestionService

PATIENT = {"resourceType": "Patient", "id": "P1", "gender": "female",
           "birthDate": "1990-03-04"}
ENCOUNTER = {"resourceType": "Encounter", "id": "E1", "status": "finished",
             "subject": {"reference": "Patient/P1"}}
OBS = {"resourceType": "Observation", "id": "O1", "status": "final",
       "code": {"coding": [{"code": "G"}]}, "subject": {"reference": "Patient/P1"},
       "encounter": {"reference": "Encounter/E1"},
       "valueQuantity": {"value": 5.4, "unit": "mmol/L"}}


@pytest.fixture
def svc_with_dataset():
    svc = IngestionService()
    svc.ingest(PATIENT)
    svc.ingest(ENCOUNTER)
    svc.ingest(OBS)
    return svc


def test_reference_to_missing_target_is_rejected(caplog):
    svc = IngestionService()
    with pytest.raises(PipelineError) as exc:
        svc.ingest(ENCOUNTER)  # Patient/P1 尚不存在
    assert exc.value.reason is RejectReason.REFERENCE_NOT_FOUND
    assert exc.value.context["targetId"] == "P1"
    # 日志必须打印输入
    assert any("Patient/P1" in r.getMessage() for r in caplog.records)


def test_full_ingest_flow(svc_with_dataset):
    svc = svc_with_dataset
    assert svc.get("Observation", "O1").version == 1
    history = svc.get_history("Observation", "O1")
    assert [e.version for e in history.entries] == [1]


def test_duplicate_submission_does_not_duplicate(svc_with_dataset, caplog):
    svc = svc_with_dataset
    result = svc.ingest(OBS)
    assert result.status.value == "duplicate"
    assert result.reason == "duplicate_submission"
    assert svc.get_history("Observation", "O1").current_version == 1
    # 判定依据写入日志
    assert any("重复提交" in r.getMessage() for r in caplog.records)


def test_version_conflict_is_explicit_and_data_consistent(svc_with_dataset, caplog):
    svc = svc_with_dataset
    changed = {**OBS, "valueQuantity": {"value": 6.0, "unit": "mmol/L"}}
    first = svc.ingest(changed, expected_version=1)
    assert first.status.value == "updated" and first.version == 2

    # 另一个请求仍基于 v1 提交不同内容 -> 冲突，且数据不能被回退覆盖
    stale = {**OBS, "valueQuantity": {"value": 7.0, "unit": "mmol/L"}}
    with pytest.raises(PipelineError) as exc:
        svc.ingest(stale, expected_version=1)
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    assert exc.value.context["currentVersion"] == 2
    assert svc.get("Observation", "O1").body["valueQuantity"]["value"] == 6.0
    assert any("版本冲突" in r.getMessage() for r in caplog.records)


def test_distinct_reasons_for_distinct_failures(svc_with_dataset):
    svc = svc_with_dataset
    with pytest.raises(PipelineError) as e1:
        svc.ingest('{"bad json')
    with pytest.raises(PipelineError) as e2:
        svc.ingest({**OBS, "subject": {"reference": "Patient/NOPE"}})
    with pytest.raises(PipelineError) as e3:
        svc.ingest({**OBS, "valueQuantity": {"value": 9}}, expected_version=1)
        svc.ingest({**OBS, "valueQuantity": {"value": 8}}, expected_version=1)
    reasons = {e1.value.reason, e2.value.reason, e3.value.reason}
    assert reasons == {RejectReason.MALFORMED_JSON,
                       RejectReason.REFERENCE_NOT_FOUND,
                       RejectReason.VERSION_CONFLICT}


def test_concurrent_ingests_no_lost_update(svc_with_dataset):
    svc = svc_with_dataset
    errors: list[PipelineError] = []

    def worker(value: float) -> None:
        body = {**OBS, "valueQuantity": {"value": value, "unit": "mmol/L"}}
        try:
            svc.ingest(body, expected_version=1)
        except PipelineError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(v,))
               for v in (6.1, 6.2, 6.3, 6.4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 3
    assert all(e.reason is RejectReason.VERSION_CONFLICT for e in errors)
    assert svc.get_history("Observation", "O1").current_version == 2


def test_deidentify_end_to_end_references_resolve_and_is_stable(svc_with_dataset, caplog):
    svc = svc_with_dataset
    caplog.set_level(logging.INFO)
    out1 = svc.deidentify_all()
    by_type = {r["resourceType"]: r for r in out1["resources"]}
    pseudo_patient = by_type["Patient"]["id"]
    assert by_type["Encounter"]["subject"]["reference"] == f"Patient/{pseudo_patient}"
    assert by_type["Observation"]["encounter"]["reference"] == \
        f"Encounter/{by_type['Encounter']['id']}"

    # 再次执行：映射不增加、引用不破坏
    mapping_keys_1 = set(out1["mapping"])
    out2 = svc.deidentify_all()
    assert set(out2["mapping"]) == mapping_keys_1
    assert out2["resources"] == out1["resources"]


def test_string_json_input_supported(svc_with_dataset):
    svc = IngestionService()
    result = svc.ingest(json.dumps(PATIENT))
    assert result.status.value == "created"
