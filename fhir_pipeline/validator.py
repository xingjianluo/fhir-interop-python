"""跨资源引用校验。

格式问题在 normalizer 阶段已被拒绝（REFERENCE_BAD_FORMAT）；
本模块负责：归一化后的引用必须指向存储中已存在的目标，
否则给出 REFERENCE_NOT_FOUND，并附上是哪个字段、指向哪里。
"""

from __future__ import annotations

from typing import Any, Protocol

from .errors import PipelineError, RejectReason
from .normalizer import _REFERENCE_FIELDS, _split_reference  # 复用同一套引用字段表


class VersionedStoreView(Protocol):
    """校验所需的只读存储视图。"""

    def exists(self, resource_type: str, resource_id: str) -> bool: ...


def validate_references(body: dict[str, Any], store: VersionedStoreView) -> None:
    """校验 body 内所有引用；目标不存在即拒绝，不做静默跳过。"""
    resource_type = body.get("resourceType")
    if not isinstance(resource_type, str):
        raise PipelineError(
            RejectReason.MISSING_FIELD, "validate_references 需要 resourceType"
        )
    fields = _REFERENCE_FIELDS.get(resource_type, {})
    for field_name, (_, allowed) in fields.items():
        ref = body.get(field_name)
        if ref is None:
            continue  # 必填性已在 normalizer 校验
        ref_str = ref.get("reference") if isinstance(ref, dict) else None
        if not isinstance(ref_str, str):
            raise PipelineError(
                RejectReason.REFERENCE_BAD_FORMAT,
                f"{resource_type}.{field_name} 引用格式非法",
                context={"field": field_name, "value": ref},
            )
        target_type, target_id = _split_reference(ref_str)
        if target_type not in allowed or not store.exists(target_type, target_id):
            raise PipelineError(
                RejectReason.REFERENCE_NOT_FOUND,
                f"字段 {field_name} 引用的目标 {target_type}/{target_id} 不存在",
                context={
                    "field": field_name,
                    "targetType": target_type,
                    "targetId": target_id,
                },
            )


__all__ = ["VersionedStoreView", "validate_references"]
