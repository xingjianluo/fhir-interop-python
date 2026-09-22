"""解析与归一化：字段缺失、坏 JSON、非法取值必须以可区分原因拒绝。"""

import logging

import pytest

from fhir_pipeline.errors import PipelineError, RejectReason
from fhir_pipeline.normalizer import (
    compute_payload_hash,
    normalize,
    parse_payload,
)


def test_patient_accepted():
    body = normalize(
        {"resourceType": "Patient", "id": "P1", "gender": "male",
         "birthDate": "1980-05-01"}
    )
    assert body["resourceType"] == "Patient"
    assert body["id"] == "P1"


def test_hash_is_deterministic_regardless_of_key_order():
    a = normalize({"resourceType": "Patient", "id": "P1", "gender": "male",
                   "birthDate": "1980-05-01"})
    b = normalize({"birthDate": "1980-05-01", "gender": "male", "id": "P1",
                   "resourceType": "Patient"})
    assert compute_payload_hash(a) == compute_payload_hash(b)


@pytest.mark.parametrize(
    "raw,reason",
    [
        ('{"resourceType"', RejectReason.MALFORMED_JSON),
        ([1, 2], RejectReason.NOT_AN_OBJECT),
        ("{}", RejectReason.MISSING_FIELD),                  # 缺 resourceType
        ('{"resourceType":"Doctor","id":"x"}', RejectReason.UNSUPPORTED_RESOURCE_TYPE),
        ('{"resourceType":"Patient"}', RejectReason.MISSING_FIELD),  # 缺 id
        ('{"resourceType":"Patient","id":"P 9","gender":"male","birthDate":"1980-01-01"}',
         RejectReason.INVALID_VALUE),                        # id 含空格
        ('{"resourceType":"Patient","id":"P2","gender":"x","birthDate":"1980-01-01"}',
         RejectReason.INVALID_VALUE),                        # gender 非法
        ('{"resourceType":"Patient","id":"P3","gender":"male","birthDate":"1980-13-40"}',
         RejectReason.INVALID_VALUE),                        # 日期不真实
        ('{"resourceType":"Encounter","id":"E","status":"finished","subject":{"reference":"Patient"}}',
         RejectReason.REFERENCE_BAD_FORMAT),                 # 引用格式坏
        ('{"resourceType":"Observation","id":"O","status":"final",'
         '"code":{"coding":[{"code":"G"}]},"subject":{"reference":"Patient/P1"}}',
         RejectReason.MISSING_FIELD),                        # 缺检验结果值
    ],
)
def test_rejections_are_distinguishable(raw, reason, caplog):
    logging.getLogger("test.normalizer").info(
        "判定输入=%s 期望原因=%s", raw, reason.value
    )
    with pytest.raises(PipelineError) as exc_info:
        normalize(parse_payload(raw))
    assert exc_info.value.reason is reason
    # 判定依据：错误对象必须带人类可读说明
    assert exc_info.value.message


def test_encounter_missing_subject_field_rejected():
    with pytest.raises(PipelineError) as exc_info:
        normalize({"resourceType": "Encounter", "id": "E1", "status": "finished"})
    assert exc_info.value.reason is RejectReason.MISSING_FIELD
    assert "subject" in exc_info.value.message
