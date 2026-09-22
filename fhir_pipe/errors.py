"""可区分的拒绝原因。

所有因数据或版本问题拒绝处理的场景都抛出 ``RejectionError``，
其 ``reason`` 为 ``RejectReason`` 枚举值，调用方可据此区分失败原因，
而不会出现静默丢弃。
"""

from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    """拒绝原因枚举（字符串值直接面向日志与 API 响应）。"""

    MALFORMED_JSON = "malformed_json"
    """载荷不是合法 JSON 对象。"""

    MISSING_FIELD = "missing_field"
    """必填字段缺失或为空。"""

    BAD_VALUE = "bad_value"
    """字段取值不符合约束（类型、枚举、范围、格式）。"""

    UNKNOWN_RESOURCE_TYPE = "unknown_resource_type"
    """资源类型不在支持范围内。"""

    BROKEN_REFERENCE = "broken_reference"
    """引用的目标资源不存在。"""

    VERSION_CONFLICT = "version_conflict"
    """乐观锁版本冲突（If-Match 版本过期）。"""

    DUPLICATE_ID = "duplicate_id"
    """以创建语义提交的资源 ID 已存在。"""

    NOT_FOUND = "not_found"
    """更新/读取的目标资源不存在。"""

    ALREADY_DEIDENTIFIED = "already_deidentified"
    """资源已去标识化，重复去标识化应保持幂等而非报错时使用。"""


class RejectionError(Exception):
    """带可区分原因的业务拒绝异常。"""

    def __init__(self, reason: RejectReason, message: str, *, field: str | None = None):
        self.reason = reason
        self.field = field
        super().__init__(f"[{reason.value}] {message}")

    def to_dict(self) -> dict:
        return {
            "reason": self.reason.value,
            "message": str(self.args[0]),
            "field": self.field,
        }
