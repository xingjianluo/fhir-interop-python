"""pytest 公共夹具与辅助。

- 打开 fhir_pipe logger 的 INFO 输出，使每个用例都能在单测日志中
  看到「输入」与「判定依据」。
- 提供标准样例数据与“断言拒绝原因”的辅助函数（同样记录日志）。
"""

import logging

import pytest

from fhir_pipe.errors import RejectReason, RejectionError
from fhir_pipe.service import IngestionService


@pytest.fixture(autouse=True)
def _enable_pipe_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="fhir_pipe")


@pytest.fixture
def svc() -> IngestionService:
    return IngestionService()


# ---- 标准样例 ---------------------------------------------------------

def patient(pid: str = "P1", **over) -> dict:
    d = {
        "resourceType": "Patient", "id": pid,
        "name": "张三", "gender": "male", "birthDate": "1980-05-12",
    }
    d.update(over)
    return d


def encounter(eid: str = "E1", pid: str = "P1", **over) -> dict:
    d = {
        "resourceType": "Encounter", "id": eid,
        "subject": {"reference": f"Patient/{pid}"},
        "status": "finished",
        "period": {"start": "2026-02-01", "end": "2026-02-03"},
    }
    d.update(over)
    return d


def observation(oid: str = "O1", pid: str = "P1", eid: str | None = "E1", **over) -> dict:
    d = {
        "resourceType": "Observation", "id": oid, "status": "final",
        "code": {"coding": [{"code": "4548-4"}]},
        "subject": {"reference": f"Patient/{oid and pid}"},
        "encounter": ({"reference": f"Encounter/{eid}"} if eid else None),
        "valueQuantity": {"value": 6.7, "unit": "%"},
        "effectiveDateTime": "2026-02-02",
    }
    d.update(over)
    return d


def assert_rejected(
    caplog: pytest.LogCaptureFixture,
    fn,
    reason: RejectReason,
    *,
    label: str,
    payload: object,
) -> RejectionError:
    """执行 fn，断言其以指定 reason 拒绝；打印输入与判定依据。"""
    logging.getLogger("fhir_pipe.test").info(
        "用例[%s] 输入=%r 期望判定=%s", label, payload, reason.value
    )
    with pytest.raises(RejectionError) as exc_info:
        fn()
    err = exc_info.value
    assert err.reason is reason, (
        f"用例[{label}] 期望 {reason.value}，实际 {err.reason.value}: {err}"
    )
    logging.getLogger("fhir_pipe.test").info(
        "用例[%s] 实际判定=%s 字段=%s 依据=%s",
        label, err.reason.value, err.field, err,
    )
    return err
