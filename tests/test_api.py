"""HTTP 入口单测：经 TestClient 验证与 Python 入口结论一致、状态码可区分。"""

import logging

import pytest
from fastapi.testclient import TestClient

from fhir_pipe import api
from fhir_pipe.errors import RejectReason

from conftest import encounter, observation, patient

log = logging.getLogger("fhir_pipe.test")


@pytest.fixture
def client() -> TestClient:
    # 每个用例重置模块级单例服务，避免用例间状态串扰
    api.service = api.IngestionService()
    return TestClient(api.app)


def _post(client: TestClient, rtype: str, payload: dict):
    log.info("HTTP POST /fhir/%s 输入=%r", rtype, payload)
    return client.post(f"/fhir/{rtype}", json=payload)


def test_http_create_and_get(client: TestClient) -> None:
    resp = _post(client, "Patient", patient())
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == "P1" and body["meta"]["versionId"] == "1"
    got = client.get("/fhir/Patient/P1")
    assert got.status_code == 200 and got.json()["name"] == "张三"
    log.info("test_http_create 判定=200，返回版本与读取一致")


def test_http_validation_errors_are_distinguishable(client: TestClient) -> None:
    # 422 缺字段（不静默丢弃）
    r1 = _post(client, "Patient", {"resourceType": "Patient", "id": "P1"})
    assert r1.status_code == 422
    assert r1.json()["error"]["reason"] == RejectReason.MISSING_FIELD.value

    # 422 坏取值
    r2 = _post(client, "Patient", patient(gender="nope"))
    assert r2.json()["error"]["reason"] == RejectReason.BAD_VALUE.value

    # 400 非 JSON
    r3 = client.post(
        "/fhir/Patient", content=b"{not-json",
        headers={"content-type": "application/json"},
    )
    assert r3.status_code == 400
    assert r3.json()["error"]["reason"] == RejectReason.MALFORMED_JSON.value
    log.info("test_http_validation 判定=422 missing/bad 与 400 malformed 可区分")


def test_http_duplicate_and_version_conflict(client: TestClient) -> None:
    _post(client, "Patient", patient())
    # 重复创建 -> 409 duplicate_id
    dup = _post(client, "Patient", patient())
    assert dup.status_code == 409
    assert dup.json()["error"]["reason"] == RejectReason.DUPLICATE_ID.value

    # 更新到 v2
    upd = client.put(
        "/fhir/Patient/P1", json=patient(name="张三丰"),
        headers={"If-Match": 'W/"1"'},
    )
    assert upd.status_code == 200 and upd.json()["meta"]["versionId"] == "2"

    # 仍带 v1 提交 -> 409 version_conflict（与 duplicate_id 可区分）
    stale = client.put(
        "/fhir/Patient/P1", json=patient(name="旧版改名"),
        headers={"If-Match": 'W/"1"'},
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["reason"] == RejectReason.VERSION_CONFLICT.value

    # 缺 If-Match -> 版本按 0 -> version_conflict，不允许裸 PUT 覆盖
    no_match = client.put("/fhir/Patient/P1", json=patient(name="x"))
    assert no_match.status_code == 409

    # 更新不存在资源 -> 404 not_found
    ghost = client.put(
        "/fhir/Patient/GHOST", json=patient(id="GHOST"),
        headers={"If-Match": 'W/"1"'},
    )
    assert ghost.status_code == 404
    assert ghost.json()["error"]["reason"] == RejectReason.NOT_FOUND.value
    log.info("test_http_conflict 判定=duplicate/conflict=409 且 reason 不同，missing=404")


def test_http_idempotent_replay_does_not_bump(client: TestClient) -> None:
    _post(client, "Patient", patient())
    r1 = client.put("/fhir/Patient/P1", json=patient(),
                    headers={"If-Match": 'W/"1"'})
    r2 = client.put("/fhir/Patient/P1", json=patient(),
                    headers={"If-Match": 'W/"1"'})
    assert r1.json()["meta"]["ingest"]["replayed"] is True
    assert r2.json()["meta"]["ingest"]["replayed"] is True
    hist = client.get("/fhir/Patient/P1/_history").json()
    assert len(hist["entry"]) == 1
    log.info("test_http_replay 判定=重复 PUT 同版同内容 replayed，历史仅 1 条")


def test_http_broken_reference_rejected(client: TestClient) -> None:
    # 先提 Encounter，Patient 尚不存在
    r = _post(client, "Encounter", encounter())
    assert r.status_code == 422
    assert r.json()["error"]["reason"] == RejectReason.BROKEN_REFERENCE.value
    # 补齐 Patient、Encounter 后 Observation 成功
    _post(client, "Patient", patient())
    _post(client, "Encounter", encounter())
    ok = _post(client, "Observation", observation())
    assert ok.status_code == 200
    log.info("test_http_refs 判定=断引用 422 broken_reference，补齐后可合入")


def test_http_history_traceable(client: TestClient) -> None:
    _post(client, "Patient", patient())
    client.put("/fhir/Patient/P1", json=patient(name="张三丰"),
               headers={"If-Match": 'W/"1"'})
    hist = client.get("/fhir/Patient/P1/_history").json()
    assert [e["meta"]["versionId"] for e in hist["entry"]] == ["1", "2"]
    log.info("test_http_history 判定=历史 v1/v2 可追溯")


def test_http_deidentify_bundle_consistent(client: TestClient) -> None:
    _post(client, "Patient", patient())
    _post(client, "Encounter", encounter())
    _post(client, "Observation", observation())

    b1 = client.post("/fhir/$deidentify").json()
    b2 = client.post("/fhir/$deidentify").json()
    ids1 = [e["id"] for e in b1["entry"]]
    ids2 = [e["id"] for e in b2["entry"]]
    assert ids1 == ids2  # 重复执行不产生新映射

    by_id = {e["id"]: e for e in b1["entry"]}
    enc = next(e for e in b1["entry"] if e["resourceType"] == "Encounter")
    obs = next(e for e in b1["entry"] if e["resourceType"] == "Observation")
    assert enc["subject"]["reference"].split("/")[1] in by_id
    assert obs["subject"]["reference"] == enc["subject"]["reference"]
    assert obs["encounter"]["reference"].split("/")[1] in by_id
    # 原始资源仍可按真实 ID 读取（去标识化是视图）
    assert client.get("/fhir/Patient/P1").json()["name"] == "张三"
    log.info("test_http_deid 判定=两次 Bundle 伪 ID 一致、跨资源引用可解析")


def test_http_path_body_mismatch_rejected(client: TestClient) -> None:
    _post(client, "Patient", patient())
    r = client.put("/fhir/Patient/OTHER", json=patient(),
                   headers={"If-Match": 'W/"1"'})
    assert r.status_code == 422
    assert r.json()["error"]["reason"] == RejectReason.BAD_VALUE.value
    log.info("test_http_mismatch 判定=路径/体内 id 不一致 -> 422 bad_value")
