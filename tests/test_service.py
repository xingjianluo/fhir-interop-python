"""service 端到端单测：合入顺序/引用完整性/重复提交/版本冲突/去标识化。"""

import logging

import pytest

from fhir_pipe.errors import RejectReason
from fhir_pipe.service import IngestionService

from conftest import assert_rejected, encounter, observation, patient

log = logging.getLogger("fhir_pipe.test")


def test_ingest_order_patient_first(
    svc: IngestionService, caplog: pytest.LogCaptureFixture
) -> None:
    # 先到 Encounter（目标 Patient 尚不存在）必须被拒绝而非丢弃
    e = encounter()
    log.info("test_order 输入=先提交 Encounter: %r", e)
    assert_rejected(
        caplog, lambda: svc.ingest(e),
        RejectReason.BROKEN_REFERENCE,
        label="先于 Patient 提交 Encounter", payload=e,
    )

    # 合入 Patient 后再提交 Encounter/Observation 成功
    r_p = svc.ingest(patient())
    r_e = svc.ingest(encounter())
    r_o = svc.ingest(observation())
    assert r_p.created and r_e.created and r_o.created
    assert svc.get("Observation", "O1").subject.ref == "Patient/P1"
    log.info("test_order 判定=按 Patient->Encounter->Observation 合入成功")


def test_observation_missing_encounter_target_rejected(
    svc: IngestionService, caplog: pytest.LogCaptureFixture
) -> None:
    svc.ingest(patient())
    # encounter 引用指向不存在的 E9
    bad = observation(eid="E9")
    assert_rejected(
        caplog, lambda: svc.ingest(bad),
        RejectReason.BROKEN_REFERENCE, label="Obs 引用缺失 Encounter/E9",
        payload=bad,
    )


def test_duplicate_submit_consistent(
    svc: IngestionService, caplog: pytest.LogCaptureFixture
) -> None:
    data = patient()
    first = svc.ingest(data)
    log.info("test_dup 输入=重复 POST 同一资源 %r", data)
    # 第二次以“创建”语义提交 -> DUPLICATE_ID，数据不产生重复记录
    assert_rejected(
        caplog, lambda: svc.ingest(data),
        RejectReason.DUPLICATE_ID, label="重复创建", payload=data,
    )
    # 以更新语义带相同版本+相同内容重放 -> 幂等成功，不升版
    replay = svc.ingest(data, expected_version=first.version_id)
    assert replay.replayed and replay.version_id == 1
    assert len(svc.history("Patient", "P1")) == 1
    log.info("test_dup 判定=创建重复被 DUPLICATE_ID 拒绝；同版重放 replayed，无重复记录")


def test_version_conflict_is_explicit(
    svc: IngestionService, caplog: pytest.LogCaptureFixture
) -> None:
    svc.ingest(patient(name="张三"))
    svc.ingest(patient(name="张三丰"), expected_version=1)  # 升到 v2

    stale = patient(name="基于旧版的改名")
    log.info("test_conflict 输入=基于 v1 的更新，而当前为 v2: %r", stale)
    assert_rejected(
        caplog, lambda: svc.ingest(stale, expected_version=1),
        RejectReason.VERSION_CONFLICT, label="过期版本更新", payload=stale,
    )
    # 失败原因可与“资源不存在”区分：更新不存在的资源 -> NOT_FOUND
    ghost = patient(id="GHOST")
    assert_rejected(
        caplog, lambda: svc.ingest(ghost, expected_version=1),
        RejectReason.NOT_FOUND, label="更新不存在资源", payload=ghost,
    )
    # 冲突未污染数据：仍是 v2 张三丰
    cur = svc.get("Patient", "P1")
    assert cur.meta.version_id == 2 and cur.name == "张三丰"
    log.info("test_conflict 判定=VERSION_CONFLICT 与 NOT_FOUND 可区分，数据保持 v2")


def test_history_traceable_across_updates(svc: IngestionService) -> None:
    svc.ingest(patient(name="张三"))
    svc.ingest(patient(name="张三丰"), expected_version=1)
    svc.ingest(patient(name="张三先生"), expected_version=2)
    hist = svc.history("Patient", "P1")
    assert [h.meta.version_id for h in hist] == [1, 2, 3]
    assert [h.name for h in hist] == ["张三", "张三丰", "张三先生"]
    log.info("test_history 判定=历史版本 v1/v2/v3 均可追溯")


def test_deidentify_view_consistent_and_repeatable(
    svc: IngestionService,
) -> None:
    svc.ingest(patient())
    svc.ingest(encounter())
    svc.ingest(observation())

    bundle1 = svc.deidentify_all()
    ids1 = {(r.resource_type, r.id) for r in bundle1}
    mappings1 = dict(svc.table.mappings())

    bundle2 = svc.deidentify_all()
    assert [r.id for r in bundle2] == [r.id for r in bundle1]
    assert svc.table.mappings() == mappings1
    assert ids1 == {(r.resource_type, r.id) for r in bundle2}

    # 引用仍可解析
    by_id = {r.id: r for r in bundle2}
    enc = next(r for r in bundle2 if r.resource_type == "Encounter")
    obs = next(r for r in bundle2 if r.resource_type == "Observation")
    pat = next(r for r in bundle2 if r.resource_type == "Patient")
    assert enc.subject.resource_id in by_id
    assert obs.subject.resource_id == pat.id
    assert obs.encounter.resource_id == enc.id

    # 原始存储不被污染
    assert svc.get("Patient", "P1").name == "张三"
    log.info(
        "test_deid 判定=两次去标识化结果一致（%d 条映射），引用可解析，原始数据未改写",
        len(mappings1),
    )


def test_ingest_with_deidentify_flag_is_stable(svc: IngestionService) -> None:
    svc.ingest(patient())
    r1 = svc.ingest(encounter(), deidentify=True)
    r2 = svc.ingest(encounter(), expected_version=1, deidentify=True)
    assert r1.resource.id == r2.resource.id
    assert r1.deidentified and r2.replayed
    log.info("test_ingest_deid 判定=合入时去标识化视图在重复提交下保持稳定 %s",
             r1.resource.id)
