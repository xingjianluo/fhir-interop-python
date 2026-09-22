"""FHIR 风格资源的类型定义与判定结果。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# 本流程支持的资源类型：患者、就诊、检验结果
SUPPORTED_TYPES = ("Patient", "Encounter", "Observation")


class IngestStatus(str, Enum):
    """一次合入请求的判定结论。"""

    CREATED = "created"
    UPDATED = "updated"
    DUPLICATE = "duplicate"  # 重复提交：内容一致，幂等返回
    REJECTED = "rejected"


@dataclass
class Resource:
    """归一化后的资源记录。

    body 为归一化后的规范字典；version 为单调递增的整数版本号；
    version_id 形如 "1"，payload_hash 为规范内容的哈希（幂等判定）。
    """

    resource_type: str
    resource_id: str
    version: int
    body: dict[str, Any]
    payload_hash: str
    version_id: str = ""

    def __post_init__(self) -> None:
        if not self.version_id:
            self.version_id = str(self.version)


@dataclass
class IngestResult:
    """合入入口返回的统一结论。"""

    status: IngestStatus
    resource_type: str
    resource_id: str
    version: int
    version_id: str
    payload_hash: str
    reason: str = ""          # REJECTED 时为 RejectReason 的值
    detail: str = ""          # 判定依据说明
    existing_version: int | None = None  # 冲突/重复时的库内版本

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "resourceType": self.resource_type,
            "id": self.resource_id,
            "version": self.version,
            "versionId": self.version_id,
            "payloadHash": self.payload_hash,
            "reason": self.reason,
            "detail": self.detail,
            "existingVersion": self.existing_version,
        }


@dataclass
class HistoryEntry:
    """一个资源的某个历史版本。"""

    version: int
    version_id: str
    payload_hash: str
    body: dict[str, Any]


@dataclass
class ResourceHistory:
    """资源的完整版本链。"""

    resource_type: str
    resource_id: str
    current_version: int
    entries: list[HistoryEntry] = field(default_factory=list)


__all__ = [
    "SUPPORTED_TYPES",
    "IngestStatus",
    "Resource",
    "IngestResult",
    "HistoryEntry",
    "ResourceHistory",
]
