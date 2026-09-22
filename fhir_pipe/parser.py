"""FHIR 风格 JSON 的解析与归一化。

职责：
- 校验载荷是否为 JSON 对象；
- 校验 resourceType 是否受支持；
- 逐字段做必填与约束校验，缺失/非法一律抛 ``RejectionError``，
  绝不静默丢弃；
- 输出 ``models`` 中的归一化 dataclass。

注意：引用的“目标存在性”不属于本模块职责（解析时存储可能尚不可见），
由 service 在合入顺序中校验 ``BROKEN_REFERENCE``。
本模块只校验引用的“格式”与“引用类型是否匹配”。
"""

from __future__ import annotations

import datetime as _dt
import re
from collections.abc import Mapping
from typing import Any

from .errors import RejectReason, RejectionError
from .models import (
    Encounter,
    Observation,
    Patient,
    Reference,
    Resource,
    ResourceType,
)

_GENDERS = {"male", "female", "other", "unknown"}
_ENCOUNTER_STATUS = {
    "planned", "arrived", "in-progress", "finished", "cancelled",
}
_OBSERVATION_STATUS = {
    "registered", "preliminary", "final", "amended", "cancelled",
}
_SUPPORTED_TYPES = {t.value for t in ResourceType}
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-]{0,63}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_resource(payload: Any) -> Resource:
    """把任意输入解析为归一化资源；非法输入抛 RejectionError。"""
    if not isinstance(payload, Mapping):
        raise RejectionError(
            RejectReason.MALFORMED_JSON,
            f"载荷必须是 JSON 对象，实际为 {type(payload).__name__}",
        )

    rtype = payload.get("resourceType")
    if not rtype:
        raise RejectionError(
            RejectReason.MISSING_FIELD, "缺少必填字段 resourceType", field="resourceType"
        )
    if not isinstance(rtype, str) or rtype not in _SUPPORTED_TYPES:
        raise RejectionError(
            RejectReason.UNKNOWN_RESOURCE_TYPE,
            f"不支持的 resourceType={rtype!r}，支持：{sorted(_SUPPORTED_TYPES)}",
            field="resourceType",
        )

    rid = _require_id(payload)

    if rtype == ResourceType.PATIENT.value:
        return _parse_patient(payload, rid)
    if rtype == ResourceType.ENCOUNTER.value:
        return _parse_encounter(payload, rid)
    return _parse_observation(payload, rid)


def _require_id(data: Mapping[str, Any]) -> str:
    rid = data.get("id")
    if rid is None or (isinstance(rid, str) and rid.strip() == ""):
        raise RejectionError(
            RejectReason.MISSING_FIELD, "缺少必填字段 id", field="id"
        )
    if not isinstance(rid, str):
        raise RejectionError(
            RejectReason.BAD_VALUE, f"id 必须是字符串，实际为 {type(rid).__name__}",
            field="id",
        )
    if not _ID_RE.match(rid):
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"id={rid!r} 不符合 ^[A-Za-z0-9][A-Za-z0-9.\\-]{{0,63}}$",
            field="id",
        )
    return rid


def _require_str(data: Mapping[str, Any], field: str) -> str:
    if field not in data or data[field] is None:
        raise RejectionError(
            RejectReason.MISSING_FIELD, f"缺少必填字段 {field}", field=field
        )
    value = data[field]
    if not isinstance(value, str) or value.strip() == "":
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"字段 {field} 必须是非空字符串，实际为 {value!r}",
            field=field,
        )
    return value


def _check_choice(value: str, allowed: set[str], *, field: str) -> str:
    if value not in allowed:
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"字段 {field}={value!r} 不在允许集合 {sorted(allowed)} 中",
            field=field,
        )
    return value


def _check_date(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not _DATE_RE.match(value):
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"字段 {field}={value!r} 必须是 YYYY-MM-DD 日期",
            field=field,
        )
    try:
        _dt.date.fromisoformat(value)
    except ValueError:
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"字段 {field}={value!r} 不是合法日历日期",
            field=field,
        ) from None
    return value


def _parse_reference(
    value: Any, *, field: str, expected_type: str, required: bool = True
) -> Reference | None:
    if value is None:
        if required:
            raise RejectionError(
                RejectReason.MISSING_FIELD, f"缺少必填引用 {field}", field=field
            )
        return None

    # 接受 FHIR 的 {"reference": "Patient/P1"}，也接受裸字符串
    if isinstance(value, Mapping):
        raw = value.get("reference")
    elif isinstance(value, str):
        raw = value
    else:
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"引用 {field} 必须是字符串或 {{'reference': ...}} 对象",
            field=field,
        )

    if not isinstance(raw, str) or "/" not in raw:
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"引用 {field}={raw!r} 必须形如 {expected_type}/<id>",
            field=field,
        )
    rtype, sep, rid = raw.partition("/")
    if not sep or not rtype or not rid:
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"引用 {field}={raw!r} 必须形如 {expected_type}/<id>",
            field=field,
        )
    if rtype not in _SUPPORTED_TYPES:
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"引用 {field} 指向未知资源类型 {rtype!r}",
            field=field,
        )
    if rtype != expected_type:
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"引用 {field} 必须指向 {expected_type}，实际指向 {rtype}",
            field=field,
        )
    if not _ID_RE.match(rid):
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"引用 {field} 的目标 id={rid!r} 不合法",
            field=field,
        )
    return Reference(raw)


def _parse_patient(data: Mapping[str, Any], rid: str) -> Patient:
    name = _require_str(data, "name")
    gender = _check_choice(
        _require_str(data, "gender"), _GENDERS, field="gender"
    )
    birth_date = _check_date(_require_str(data, "birthDate"), field="birthDate")
    return Patient(id=rid, name=name, gender=gender, birth_date=birth_date)


def _parse_encounter(data: Mapping[str, Any], rid: str) -> Encounter:
    subject = _parse_reference(
        data.get("subject"), field="subject", expected_type="Patient"
    )
    assert subject is not None  # required=True 时不会为 None
    status = _check_choice(
        _require_str(data, "status"), _ENCOUNTER_STATUS, field="status"
    )
    period = data.get("period")
    if not isinstance(period, Mapping):
        raise RejectionError(
            RejectReason.MISSING_FIELD,
            "缺少必填对象 period（需含 start）",
            field="period",
        )
    start = _check_date(_require_str(period, "start"), field="period.start")
    end_ref = period.get("end")
    end = None
    if end_ref is not None:
        end = _check_date(end_ref, field="period.end")
        if end < start:
            raise RejectionError(
                RejectReason.BAD_VALUE,
                f"period.end={end} 早于 period.start={start}",
                field="period.end",
            )
    return Encounter(
        id=rid, subject=subject, status=status,
        period_start=start, period_end=end,
    )


def _parse_observation(data: Mapping[str, Any], rid: str) -> Observation:
    status = _check_choice(
        _require_str(data, "status"), _OBSERVATION_STATUS, field="status"
    )
    code = data.get("code")
    if not isinstance(code, Mapping):
        raise RejectionError(
            RejectReason.MISSING_FIELD,
            "缺少必填对象 code（需含 coding[0].code）",
            field="code",
        )
    coding = code.get("coding")
    if not isinstance(coding, list) or not coding:
        raise RejectionError(
            RejectReason.MISSING_FIELD, "code.coding 必须是非空数组", field="code.coding"
        )
    first = coding[0]
    if not isinstance(first, Mapping) or not first.get("code"):
        raise RejectionError(
            RejectReason.MISSING_FIELD,
            "code.coding[0].code 缺失或为空",
            field="code.coding[0].code",
        )
    code_value = first["code"]
    if not isinstance(code_value, str):
        raise RejectionError(
            RejectReason.BAD_VALUE, "code.coding[0].code 必须是字符串",
            field="code.coding[0].code",
        )

    subject = _parse_reference(
        data.get("subject"), field="subject", expected_type="Patient"
    )
    assert subject is not None  # required=True 时不会为 None
    encounter = _parse_reference(
        data.get("encounter"), field="encounter",
        expected_type="Encounter", required=False,
    )

    vq = data.get("valueQuantity")
    if not isinstance(vq, Mapping):
        raise RejectionError(
            RejectReason.MISSING_FIELD,
            "缺少必填对象 valueQuantity（需含 value、unit）",
            field="valueQuantity",
        )
    value = vq.get("value")
    # 注意：bool 是 int 子类，检验数值时显式排除
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"valueQuantity.value={value!r} 必须是数值",
            field="valueQuantity.value",
        )
    unit = vq.get("unit")
    if not isinstance(unit, str) or unit.strip() == "":
        raise RejectionError(
            RejectReason.MISSING_FIELD,
            "valueQuantity.unit 缺失或为空",
            field="valueQuantity.unit",
        )

    effective = _check_date(
        _require_str(data, "effectiveDateTime"), field="effectiveDateTime"
    )
    return Observation(
        id=rid, status=status, code=code_value, subject=subject,
        encounter=encounter, value=float(value), unit=unit, effective=effective,
    )
