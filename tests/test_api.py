"""HTTP 入口：与服务层一致的判定，通过状态码与 reason 字段可区分。"""

import pytest
from fastapi.testclient import TestClient

from fhir_pipeline import api


@pytest.fixture
def client():
    api.reset_service()
    return TestClient(api.app)


PATIENT = {"id": "P1", "gender": "female", "birthDate": "1990-03-04"}
ENCOUNTER = {"id": "E1", "status": "finished",
             "subject": {"reference": "Patient/P1"}}


def test_create_duplicate_conflict_history(client):
    r = client.post("/fhir/Patient", json=PATIENT)
    assert r.status_code == 200 and r.json()["status"] == "created"

    # 重复提交：200 duplicate，不新增版本
    r = client.post("/fhir/Patient", json=PATIENT)
    body = r.json()
    assert body["status"] == "duplicate"
    assert body["reason"] == "duplicate_submission"

    # 路径类型不一致 -> 422（不会建出任何资源）
    r = client.post("/fhir/Encounter",
                    json={"id": "X",
                          **{k: v for k, v in PATIENT.items() if k != "id"},
                          "resourceType": "Patient"})
    assert r.status_code == 422

    # 引用指向不存在的就诊目标 -> 422 reference_not_found
    obs_missing = {
        "id": "O9", "status": "final", "code": {"text": "G"},
        "subject": {"reference": "Patient/P1"},
        "encounter": {"reference": "Encounter/NOPE"},
        "valueQuantity": {"value": 1.0},
    }
    r = client.post("/fhir/Observation", json=obs_missing)
    assert r.status_code == 422
    assert r.json()["reason"] == "reference_not_found"

    # 建就诊 -> 成功（其引用 Patient/P1 已存在）
    r = client.post("/fhir/Encounter", json=ENCOUNTER)
    assert r.status_code == 200 and r.json()["status"] == "created"

    # 乐观锁更新成功
    r = client.put("/fhir/Patient/P1",
                   json={"gender": "male", "birthDate": "1990-03-04"},
                   headers={"If-Match": "W/1"})
    assert r.status_code == 200 and r.json()["status"] == "updated"

    # 旧版本再写 -> 409 version_conflict
    r = client.put("/fhir/Patient/P1",
                   json={"gender": "other", "birthDate": "1990-03-04"},
                   headers={"If-Match": "1"})
    assert r.status_code == 409
    assert r.json()["reason"] == "version_conflict"
    assert r.json()["context"]["currentVersion"] == 2

    # 历史可追溯
    r = client.get("/fhir/Patient/P1/_history")
    assert [v["version"] for v in r.json()["versions"]] == [1, 2]


def test_malformed_and_missing_field_status_codes(client):
    r = client.post("/fhir/Patient", content=b'{"id":',
                    headers={"Content-Type": "application/json"})
    assert r.status_code == 400 and r.json()["reason"] == "malformed_json"

    r = client.post("/fhir/Patient", json={"id": "P9", "gender": "male"})
    assert r.status_code == 422 and r.json()["reason"] == "missing_field"


def test_idempotency_key_header_prevents_duplicate(client):
    payload = {"id": "P8", "gender": "male", "birthDate": "1977-07-07"}
    r1 = client.post("/fhir/Patient", json=payload,
                     headers={"Idempotency-Key": "REQ-8"})
    r2 = client.post("/fhir/Patient", json=payload,
                     headers={"Idempotency-Key": "REQ-8"})
    assert r1.json()["status"] == "created"
    assert r2.json()["status"] == "duplicate"
    assert r2.json()["version"] == 1


def test_deidentify_endpoint(client):
    client.post("/fhir/Patient", json=PATIENT)
    client.post("/fhir/Encounter", json=ENCOUNTER)
    r = client.post("/deidentify")
    assert r.status_code == 200
    resources = r.json()["resources"]
    patient = next(x for x in resources if x["resourceType"] == "Patient")
    encounter = next(x for x in resources if x["resourceType"] == "Encounter")
    assert encounter["subject"]["reference"] == f"Patient/{patient['id']}"
