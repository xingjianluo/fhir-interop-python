"""可区分的拒绝原因。

所有非法输入都必须抛出带有明确 RejectReason 的 PipelineError，
禁止静默丢弃。
"""

from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    """数据被拒绝的原因分类。"""

    MALFORMED_JSON = "malformed_json"
    NOT_AN_OBJECT = "not_an_object"
    UNSUPPORTED_RESOURCE_TYPE = "unsupported_resource_type"
    MISSING_FIELD = "missing_field"
    INVALID_VALUE = "invalid_value"
    REFERENCE_NOT_FOUND = "reference_not_found"
    REFERENCE_BAD_FORMAT = "reference_bad_format"
    RESOURCE_NOT_FOUND = "resource_not_found"
    VERSION_CONFLICT = "version_conflict"
    DUPLICATE_SUBMISSION = "duplicate_submission"
    # 患者合并相关
    MERGE_VERSION_CONFLICT = "merge_version_conflict"  # 合并基于的版本已落后
    PATIENT_ALREADY_MERGED = "patient_already_merged"  # 目标患者已被合并（墓碑）


class PipelineError(Exception):
    """流程中可解释的错误。

    reason: 机器可判定的拒绝原因；message: 人类可读说明；
    context: 附加上下文（如字段名、资源标识）。
    """

    def __init__(
        self,
        reason: RejectReason,
        message: str,
        *,
        context: dict | None = None,
    ) -> None:
        self.reason = reason
        self.message = message
        self.context = context or {}
        super().__init__(f"[{reason.value}] {message}")


__all__ = ["RejectReason", "PipelineError"]
