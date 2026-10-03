"""患者合并在 Python API / HTTP / CLI 三个入口给出一致结论。

三入口共用同一个 IngestionService：本文件对同一输入序列分别走
- Python：直接调 svc.merge_patients / svc.ingest；
- HTTP：TestClient POST /fhir/Patient/$merge；
- CLI：cli._handle_merge（真实 argparse 解析由 test_cli_main 覆盖），
比较三者的 status/reason/版本/改指清单与合并后的库内事实。
"""

import io
import json

import pytest
from fastapi.testclient import TestClient

from fhir_pipeline import api, cli
from fhir_pipeline.errors import PipelineError, RejectReason
from fhir_pipeline.service import IngestionService

P1 = {"resourceType": "Patient", "id": "P1", "gender": "female",
      "birthDate": "1990-03-04"}
P2 = {"resourceType": "Patient", "id": "P2", "gender": "male",
      "birthDate": "1985-07-08"}
E2 = {"resourceType": "Encounter", "id": "E2", "status": "finished",
      "subject": {"reference": "Patient/P2"}}

MERGE_BODY = {
    "survivingPatientId": "P1",
    "mergedPatientId": "P2",
    "expectedSurvivingVersion": 1,
    "expectedMergedVersion": 1,
}


@pytest.fixture
def client():
    api.reset_service()
    return TestClient(api.app)


def _seed(svc: IngestionService) -> None:
    svc.ingest(P1)
    svc.ingest(P2)
    svc.ingest(E2)

def _cli_merge(svc: IngestionService) -> tuple[int, dict]:
    import argparse

    args = argparse.Namespace(
        survivor="P1", merged="P2", survivor_version=1, merged_version=1,
        idempotency_key=None,
    )
    buf = io.StringIO()
    old_stdout = cli.sys.stdout
    cli.sys.stdout = buf
    try:
        code = cli._handle_merge(svc, args)
    finally:
        cli.sys.stdout = old_stdout
    return code, json.loads(buf.getvalue())


def test_successful_merge_consistent_across_entrypoints():
    # Python 入口
    svc_py = IngestionService()
    _seed(svc_py)
    py_result = svc_py.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    ).to_dict()

    # HTTP 入口
    api.reset_service()
    client = TestClient(api.app)
    client.post("/fhir/Patient", json=P1)
    client.post("/fhir/Patient", json=P2)
    client.post("/fhir/Encounter", json=E2)
    http_result = client.post("/fhir/Patient/$merge", json=MERGE_BODY).json()

    # CLI 入口
    svc_cli = IngestionService()
    _seed(svc_cli)
    code, cli_result = _cli_merge(svc_cli)

    assert code == 0
    for key in ("status", "survivingPatientId", "mergedPatientId",
                "survivingVersion", "mergedVersion", "repointed"):
        assert py_result[key] == http_result[key] == cli_result[key]
    assert py_result["status"] == "merged"
    assert py_result["repointed"] == [
        {"resourceType": "Encounter", "id": "E2", "version": 2}
    ]
    # 三入口下改指后的库内事实一致
    assert svc_py.get("Encounter", "E2").body["subject"]["reference"] == "Patient/P1"
    assert client.get("/fhir/Encounter/E2").json()["resource"]["subject"] == \
        {"reference": "Patient/P1"}
    assert svc_cli.get("Encounter", "E2").body["subject"]["reference"] == "Patient/P1"


def test_duplicate_merge_consistent_across_entrypoints():
    svc_py = IngestionService()
    _seed(svc_py)
    svc_py.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    py_again = svc_py.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    ).to_dict()

    api.reset_service()
    client = TestClient(api.app)
    client.post("/fhir/Patient", json=P1)
    client.post("/fhir/Patient", json=P2)
    client.post("/fhir/Encounter", json=E2)
    client.post("/fhir/Patient/$merge", json=MERGE_BODY)
    r = client.post("/fhir/Patient/$merge", json=MERGE_BODY)
    http_again = r.json()

    svc_cli = IngestionService()
    _seed(svc_cli)
    _cli_merge(svc_cli)
    _, cli_again = _cli_merge(svc_cli)

    assert r.status_code == 200
    for result in (py_again, http_again, cli_again):
        assert result["status"] == "duplicate"
        assert result["reason"] == "duplicate_submission"
        assert result["repointed"] == [
            {"resourceType": "Encounter", "id": "E2", "version": 2}
        ]
    assert client.get("/fhir/Patient/P2").json()["versionId"] == "2"
    assert svc_cli.get_history("Patient", "P2").current_version == 2


def test_stale_version_conflict_consistent_across_entrypoints():
    svc_py = IngestionService()
    _seed(svc_py)
    svc_py.ingest({**P1, "gender": "other"})  # P1 -> v2
    with pytest.raises(PipelineError) as py_exc:
        svc_py.merge_patients(
            surviving_patient_id="P1", merged_patient_id="P2",
            expected_surviving_version=1, expected_merged_version=1,
        )
    assert py_exc.value.reason is RejectReason.MERGE_VERSION_CONFLICT
    assert py_exc.value.context["surviving"] == {
        "id": "P1", "expectedVersion": 1, "currentVersion": 2, "stale": True}
    assert py_exc.value.context["merged"]["stale"] is False

    api.reset_service()
    client = TestClient(api.app)
    client.post("/fhir/Patient", json=P1)
    client.post("/fhir/Patient", json=P2)
    client.post("/fhir/Encounter", json=E2)
    client.put("/fhir/Patient/P1",
               json={"gender": "other", "birthDate": "1990-03-04"},
               headers={"If-Match": "1"})
    r = client.post("/fhir/Patient/$merge", json=MERGE_BODY)
    assert r.status_code == 409
    body = r.json()
    assert body["reason"] == "merge_version_conflict"
    assert body["context"]["surviving"]["currentVersion"] == 2
    assert body["context"]["surviving"]["stale"] is True
    assert body["context"]["merged"]["stale"] is False

    # CLI handler 对同一事实抛同一异常（退出码 1 由 main 保证）
    svc_cli = IngestionService()
    _seed(svc_cli)
    svc_cli.ingest({**P1, "gender": "other"})
    import argparse
    args = argparse.Namespace(
        survivor="P1", merged="P2", survivor_version=1, merged_version=1,
        idempotency_key=None,
    )
    with pytest.raises(PipelineError) as cli_exc:
        cli._handle_merge(svc_cli, args)
    assert cli_exc.value.reason is RejectReason.MERGE_VERSION_CONFLICT
    assert cli_exc.value.context["surviving"]["currentVersion"] == 2


def test_reference_to_tombstone_rejected_consistently():
    svc_py = IngestionService()
    _seed(svc_py)
    svc_py.merge_patients(
        surviving_patient_id="P1", merged_patient_id="P2",
        expected_surviving_version=1, expected_merged_version=1,
    )
    new_enc_body = {"resourceType": "Encounter", "id": "E9",
                    "status": "arrived",
                    "subject": {"reference": "Patient/P2"}}
    with pytest.raises(PipelineError) as exc:
        svc_py.ingest(new_enc_body)
    assert exc.value.reason is RejectReason.PATIENT_ALREADY_MERGED
    assert exc.value.context["replacementId"] == "P1"

    api.reset_service()
    client = TestClient(api.app)
    client.post("/fhir/Patient", json=P1)
    client.post("/fhir/Patient", json=P2)
    client.post("/fhir/Encounter", json=E2)
    client.post("/fhir/Patient/$merge", json=MERGE_BODY)
    r = client.post("/fhir/Encounter", json={
        "id": "E9", "status": "arrived",
        "subject": {"reference": "Patient/P2"}})
    assert r.status_code == 409
    assert r.json()["reason"] == "patient_already_merged"
    assert r.json()["context"]["replacementId"] == "P1"


def test_merge_endpoint_missing_version_field_is_422(client):
    r = client.post("/fhir/Patient/$merge", json={
        "survivingPatientId": "P1", "mergedPatientId": "P2",
        "expectedSurvivingVersion": 1,
        # 缺少 expectedMergedVersion：不允许不核对版本就合并
    })
    assert r.status_code == 422
    assert r.json()["reason"] == "missing_field"


def test_cli_main_merge_success_and_conflict_exit_code(capsys, monkeypatch):
    """走完整 argparse + main：成功/幂等退出码 0；版本冲突退出码 1 且 stderr 可区分。"""
    seeded = IngestionService()
    _seed(seeded)
    monkeypatch.setattr(cli, "IngestionService", lambda: seeded)

    code = cli.main([
        "merge", "--survivor", "P1", "--merged", "P2",
        "--survivor-version", "1", "--merged-version", "1",
    ])
    assert code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "merged"

    # 同一请求重复提交 -> 幂等 duplicate，退出码仍为 0
    code = cli.main([
        "merge", "--survivor", "P1", "--merged", "P2",
        "--survivor-version", "1", "--merged-version", "1",
    ])
    assert code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "duplicate"

    # 落后版本的合并（对另一对患者）-> 退出码 1，stderr 给出可区分原因
    seeded.ingest({"resourceType": "Patient", "id": "P3", "gender": "unknown",
                   "birthDate": "1970-01-01"})
    code = cli.main([
        "merge", "--survivor", "P1", "--merged", "P3",
        "--survivor-version", "1",  # P1 合并后已为 v2，期望 v1 落后
        "--merged-version", "1",
    ])
    assert code == 1
    err = json.loads(capsys.readouterr().err)
    assert err["reason"] == "merge_version_conflict"
    assert err["context"]["surviving"]["expectedVersion"] == 1
    assert err["context"]["surviving"]["currentVersion"] == 2
    assert err["context"]["surviving"]["stale"] is True
