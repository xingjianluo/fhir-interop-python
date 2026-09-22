"""原始载荷的解析与归一化。

规则：
- 输入必须是 JSON 对象；语法错误 -> MALFORMED_JSON，非对象 -> NOT_AN_OBJECT；
- resourceType 缺失 -> MISSING_FIELD；不支持 -> UNSUPPORTED_RESOURCE_TYPE；
- 必填字段缺失 -> MISSING_FIELD；取值不满足约束 -> INVALID_VALUE；
- 引用字符串格式（"Type/id"）非法 -> REFERENCE_BAD_FORMAT；
  引用目标是否存在由 validator 负责（REFERENCE_NOT_FOUND）。
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

from .errors import PipelineError, RejectReason
from .models import SUPPORTED_TYPES

_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_GENDERS = {"male", "female", "other", "unknown"}
_ENCOUNTER_STATUS = {
    "planned", "arrived", "triaged", "in-progress", "onleave",
    "finished", "cancelled",
}
_OBSERVATION_STATUS = {
    "registered", "preliminary", "final", "amended", "cancelled", "unknown",
}

# 各资源类型的引用字段：字段名 -> (是否必填, 允许的目标类型)
_REFERENCE_FIELDS: dict[str, dict[str, tuple[bool, tuple[str, ...]]]] = {
    "Patient": {},
    "Encounter": {
        "subject": (True, ("Patient",)),
    },
    "Observation": {
        "subject": (True, ("Patient",)),
        "encounter": (False, ("Encounter",)),
    },
}


def canonical_json(body: dict[str, Any]) -> str:
    """将字典递归按键排序后序列化为确定性 JSON。"""
    return json.dumps(
        _sort_keys(body), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )


def compute_payload_hash(body: dict[str, Any]) -> str:
    """规范载荷的 SHA-256 十六进制摘要。"""
    import hashlib

    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()


def parse_payload(raw: Any) -> dict[str, Any]:
    """解析输入为字典；无法解析时抛出可区分的 PipelineError。"""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (str, bytes)):
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise PipelineError(
                RejectReason.MALFORMED_JSON, f"载荷不是合法 JSON: {exc}",
                context={"snippet": _snippet(raw)},
            ) from exc
    else:
        if isinstance(raw, list):
            raise PipelineError(
                RejectReason.NOT_AN_OBJECT,
                "FHIR 资源顶层必须是 JSON 对象，而不是数组",
                context={"actual_type": "list"},
            )
        raise PipelineError(
            RejectReason.MALFORMED_JSON,
            f"不支持的输入类型: {type(raw).__name__}",
        )
    if not isinstance(data, dict):
        raise PipelineError(
            RejectReason.NOT_AN_OBJECT,
            "FHIR 资源顶层必须是 JSON 对象",
            context={"actual_type": type(data).__name__},
        )
    return data


def normalize(body: dict[str, Any]) -> dict[str, Any]:
    """校验结构与取值，返回规范化（键有序）的资源字典。"""
    resource_type = body.get("resourceType")
    if "resourceType" not in body or resource_type is None:
        raise PipelineError(
            RejectReason.MISSING_FIELD, "缺少必填字段 resourceType"
        )
    if not isinstance(resource_type, str):
        raise PipelineError(
            RejectReason.INVALID_VALUE, "resourceType 必须是字符串",
            context={"actual_type": type(resource_type).__name__},
        )
    if resource_type not in SUPPORTED_TYPES:
        raise PipelineError(
            RejectReason.UNSUPPORTED_RESOURCE_TYPE,
            f"不支持的资源类型: {resource_type!r}，允许: {', '.join(SUPPORTED_TYPES)}",
            context={"resourceType": resource_type},
        )

    normalized = dict(body)

    resource_id = normalized.get("id")
    if resource_id is None or "id" not in normalized:
        raise PipelineError(
            RejectReason.MISSING_FIELD, "缺少必填字段 id",
            context={"resourceType": resource_type},
        )
    if not isinstance(resource_id, str) or not _ID_RE.match(resource_id):
        raise PipelineError(
            RejectReason.INVALID_VALUE,
            "id 必须为 1-64 位字母/数字/点/下划线/短横线",
            context={"id": resource_id},
        )

    if resource_type == "Patient":
        _normalize_patient(normalized)
    elif resource_type == "Encounter":
        _normalize_encounter(normalized)
    elif resource_type == "Observation":
        _normalize_observation(normalized)

    _normalize_references(normalized, _REFERENCE_FIELDS[resource_type])

    return _sort_keys(normalized)


# ---------- 类型专属校验 ----------

def _normalize_patient(n: dict[str, Any]) -> None:
    gender = _require_field(n, "gender", "Patient")
    if gender not in _GENDERS:
        raise PipelineError(
            RejectReason.INVALID_VALUE,
            f"Patient.gender 取值非法: {gender!r}，允许: {sorted(_GENDERS)}",
            context={"field": "gender", "value": gender},
        )
    birth = _require_field(n, "birthDate", "Patient")
    if not isinstance(birth, str) or not _DATE_RE.match(birth):
        raise PipelineError(
            RejectReason.INVALID_VALUE,
            "Patient.birthDate 必须为 YYYY-MM-DD 字符串",
            context={"field": "birthDate", "value": birth},
        )
    try:
        date.fromisoformat(birth)
    except ValueError as exc:
        raise PipelineError(
            RejectReason.INVALID_VALUE,
            f"Patient.birthDate 不是真实日期: {birth!r}",
            context={"field": "birthDate", "value": birth},
        ) from exc
    if "name" in n and n["name"] is not None:
        if not isinstance(n["name"], list) or not n["name"]:
            raise PipelineError(
                RejectReason.INVALID_VALUE,
                "Patient.name 若提供必须是非空数组",
                context={"field": "name"},
            )
    if "identifier" in n and n["identifier"] is not None:
        if not isinstance(n["identifier"], list):
            raise PipelineError(
                RejectReason.INVALID_VALUE,
                "Patient.identifier 必须为数组",
                context={"field": "identifier"},
            )
        for idx, ident in enumerate(n["identifier"]):
            if (
                not isinstance(ident, dict)
                or not isinstance(ident.get("value"), str)
                or not ident["value"]
            ):
                raise PipelineError(
                    RejectReason.INVALID_VALUE,
                    f"Patient.identifier[{idx}] 必须含非空字符串 value",
                    context={"field": f"identifier[{idx}].value"},
                )


def _normalize_encounter(n: dict[str, Any]) -> None:
    status = _require_field(n, "status", "Encounter")
    if status not in _ENCOUNTER_STATUS:
        raise PipelineError(
            RejectReason.INVALID_VALUE,
            f"Encounter.status 取值非法: {status!r}",
            context={"field": "status", "value": status},
        )
    if "period" in n and n["period"] is not None:
        period = n["period"]
        if not isinstance(period, dict):
            raise PipelineError(
                RejectReason.INVALID_VALUE, "Encounter.period 必须为对象",
                context={"field": "period"},
            )
        for key in ("start", "end"):
            if key in period and period[key] is not None:
                _require_datetime(period[key], f"period.{key}")


def _normalize_observation(n: dict[str, Any]) -> None:
    status = _require_field(n, "status", "Observation")
    if status not in _OBSERVATION_STATUS:
        raise PipelineError(
            RejectReason.INVALID_VALUE,
            f"Observation.status 取值非法: {status!r}",
            context={"field": "status", "value": status},
        )
    code = _require_field(n, "code", "Observation")
    if not isinstance(code, dict):
        raise PipelineError(
            RejectReason.INVALID_VALUE, "Observation.code 必须为对象",
            context={"field": "code"},
        )
    coding = code.get("coding")
    has_coding = (
        isinstance(coding, list)
        and len(coding) > 0
        and all(isinstance(c, dict) and isinstance(c.get("code"), str) and c["code"]
                for c in coding)
    )
    has_text = isinstance(code.get("text"), str) and bool(code["text"])
    if not has_coding and not has_text:
        raise PipelineError(
            RejectReason.MISSING_FIELD,
            "Observation.code 必须含非空 coding[].code 或 text",
            context={"field": "code"},
        )
    if not (
        isinstance(n.get("valueQuantity"), dict)
        or isinstance(n.get("valueString"), str)
    ):
        raise PipelineError(
            RejectReason.MISSING_FIELD,
            "Observation 必须提供 valueQuantity 或 valueString 作为检验结果",
            context={"resourceType": "Observation", "id": n.get("id")},
        )
    vq = n.get("valueQuantity")
    if isinstance(vq, dict) and "value" in vq:
        if isinstance(vq["value"], bool) or not isinstance(vq["value"], (int, float)):
            raise PipelineError(
                RejectReason.INVALID_VALUE,
                "Observation.valueQuantity.value 必须为数值",
                context={"field": "valueQuantity.value", "value": vq["value"]},
            )
    if "effectiveDateTime" in n and n["effectiveDateTime"] is not None:
        _require_datetime(n["effectiveDateTime"], "effectiveDateTime")


def _normalize_references(
    n: dict[str, Any],
    fields: dict[str, tuple[bool, tuple[str, ...]]],
) -> None:
    for field_name, (required, allowed) in fields.items():
        ref = n.get(field_name)
        if ref is None:
            if required:
                raise PipelineError(
                    RejectReason.MISSING_FIELD,
                    f"{n['resourceType']} 缺少必填引用字段 {field_name}",
                    context={"field": field_name},
                )
            continue
        if not isinstance(ref, dict) or not isinstance(ref.get("reference"), str):
            raise PipelineError(
                RejectReason.REFERENCE_BAD_FORMAT,
                f"{n['resourceType']}.{field_name} 必须为含 reference 字符串的对象",
                context={"field": field_name, "value": ref},
            )
        target_type, target_id = _split_reference(ref["reference"])
        if target_type not in allowed:
            raise PipelineError(
                RejectReason.REFERENCE_BAD_FORMAT,
                f"{field_name} 引用类型 {target_type!r} 非法，允许: {allowed}",
                context={"field": field_name, "reference": ref["reference"]},
            )
        if not _ID_RE.match(target_id):
            raise PipelineError(
                RejectReason.REFERENCE_BAD_FORMAT,
                f"引用 id 非法: {target_id!r}",
                context={"field": field_name, "reference": ref["reference"]},
            )
        # 归一化为规范形态
        n[field_name] = {"reference": f"{target_type}/{target_id}"}


# ---------- 工具 ----------

def _split_reference(ref: str) -> tuple[str, str]:
    parts = ref.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise PipelineError(
            RejectReason.REFERENCE_BAD_FORMAT,
            f"引用必须形如 'Type/id'，实际: {ref!r}",
            context={"reference": ref},
        )
    return parts[0], parts[1]


def _require_field(n: dict[str, Any], field: str, resource_type: str) -> Any:
    value = n.get(field)
    if value is None:
        raise PipelineError(
            RejectReason.MISSING_FIELD,
            f"{resource_type} 缺少必填字段 {field}",
            context={"field": field, "resourceType": resource_type, "id": n.get("id")},
        )
    return value


def _require_datetime(value: Any, field: str) -> None:
    if not isinstance(value, str):
        raise PipelineError(
            RejectReason.INVALID_VALUE, f"{field} 必须为 ISO 时间字符串",
            context={"field": field, "value": value},
        )
    from datetime import datetime

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PipelineError(
            RejectReason.INVALID_VALUE, f"{field} 不是合法 ISO 时间: {value!r}",
            context={"field": field, "value": value},
        ) from exc


def _sort_keys(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _sort_keys(obj[k]) for k in sorted(obj)}
    if isinstance(obj, list):
        return [_sort_keys(v) for v in obj]
    return obj


def _snippet(raw: Any, limit: int = 80) -> str:
    text = raw if isinstance(raw, str) else str(raw)
    return text[:limit]


__all__ = ["canonical_json", "compute_payload_hash", "parse_payload", "normalize"]
