"""parser 单测：合法归一化 + 各类可区分拒绝原因。"""

import logging

import pytest

from fhir_pipe.errors import RejectReason
from fhir_pipe.models import Encounter, Observation, Patient
from fhir_pipe.parser import parse_resource

from conftest import assert_rejected, encounter, observation, patient

log = logging.getLogger("fhir_pipe.test")


def test_parse_patient_ok() -> None:
    data = patient()
    log.info("test_parse_patient_ok 输入=%r", data)
    p = parse_resource(data)
    assert isinstance(p, Patient)
    assert p.id == "P1" and p.name == "张三"
    assert p.meta.version_id == 1
    log.info("test_parse_patient_ok 判定=通过，归一化为 %r", p)


def test_parse_encounter_ok_and_optional_end() -> None:
    data = encounter()
    log.info("test_parse_encounter_ok 输入=%r", data)
    e = parse_resource(data)
    assert isinstance(e, Encounter)
    assert e.subject.ref == "Patient/P1" and e.period_end == "2026-02-03"

    no_end = encounter(eid="E2", period={"start": "2026-03-01"})
    e2 = parse_resource(no_end)
    assert e2.period_end is None
    log.info("test_parse_encounter_ok 判定=通过 end 可空")


def test_parse_observation_ok_without_encounter() -> None:
    data = observation(eid=None)
    log.info("test_parse_observation_ok 输入=%r", data)
    o = parse_resource(data)
    assert isinstance(o, Observation) and o.encounter is None and o.value == 6.7
    log.info("test_parse_observation_ok 判定=通过")


@pytest.mark.parametrize(
    "label,payload,reason",
    [
        ("载荷是数组", [], RejectReason.MALFORMED_JSON),
        ("载荷是字符串", "x", RejectReason.MALFORMED_JSON),
        ("缺 resourceType", {"id": "P9"}, RejectReason.MISSING_FIELD),
        ("未知类型", patient(resourceType="Alien"), RejectReason.UNKNOWN_RESOURCE_TYPE),
        ("缺 id", {k: v for k, v in patient().items() if k != "id"},
         RejectReason.MISSING_FIELD),
        ("id 含非法字符", patient(id="bad id!"), RejectReason.BAD_VALUE),
        ("缺 name", {k: v for k, v in patient().items() if k != "name"},
         RejectReason.MISSING_FIELD),
        ("空 name", patient(name=""), RejectReason.BAD_VALUE),
        ("gender 越界", patient(gender="n/a"), RejectReason.BAD_VALUE),
        ("生日格式错", patient(birthDate="1980/05/12"), RejectReason.BAD_VALUE),
        ("生日不存在", patient(birthDate="1980-13-40"), RejectReason.BAD_VALUE),
        ("subject 格式错", encounter(subject="P1"), RejectReason.BAD_VALUE),
        ("subject 类型错", encounter(subject={"reference": "Observation/O1"}),
         RejectReason.BAD_VALUE),
        ("encounter 状态越界", encounter(status="done"), RejectReason.BAD_VALUE),
        ("缺 period", {k: v for k, v in encounter().items() if k != "period"},
         RejectReason.MISSING_FIELD),
        ("period 缺 start", encounter(period={"end": "2026-02-03"}),
         RejectReason.MISSING_FIELD),
        ("end 早于 start", encounter(period={"start": "2026-02-05", "end": "2026-02-01"}),
         RejectReason.BAD_VALUE),
        ("obs 缺 code", {k: v for k, v in observation().items() if k != "code"},
         RejectReason.MISSING_FIELD),
        ("code.coding 空", observation(code={"coding": []}),
         RejectReason.MISSING_FIELD),
        ("value 非数值", observation(valueQuantity={"value": "高", "unit": "%"}),
         RejectReason.BAD_VALUE),
        ("value 为布尔", observation(valueQuantity={"value": True, "unit": "%"}),
         RejectReason.BAD_VALUE),
        ("缺 unit", observation(valueQuantity={"value": 6.7}),
         RejectReason.MISSING_FIELD),
        ("obs 状态越界", observation(status="x"), RejectReason.BAD_VALUE),
        ("缺 effective", {k: v for k, v in observation().items()
                          if k != "effectiveDateTime"},
         RejectReason.MISSING_FIELD),
        ("encounter 引用类型错",
         observation(encounter={"reference": "Patient/P1"}),
         RejectReason.BAD_VALUE),
    ],
)
def test_parse_rejections(
    caplog: pytest.LogCaptureFixture,
    label: str, payload: object, reason: RejectReason,
) -> None:
    # 数据不被静默丢弃：每种坏数据都必须得到可区分的拒绝原因
    assert_rejected(
        caplog, lambda: parse_resource(payload), reason,
        label=label, payload=payload,
    )
