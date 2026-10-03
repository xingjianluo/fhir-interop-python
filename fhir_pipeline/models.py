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


class MergeStatus(str, Enum):
    """一次患者合并请求的判定结论。"""

    MERGED = "merged"
    DUPLICATE = "duplicate"  # 同一合并请求重复提交：返回首次结论，不二次改写


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
class MergeResult:
    """患者合并入口返回的统一结论。"""

    status: MergeStatus
    survivor_id: str            # 保留记录 id
    merged_id: str              # 被合并记录 id
    survivor_version: int       # 合并时保留记录的版本
    merged_version: int         # 合并后被合并记录的新版本（带合并标记）
    repointed: list[dict[str, Any]] = field(default_factory=list)
    # 每条: {"resourceType", "id", "fromVersion", "toVersion"}
    merge_key: str = ""         # 合并幂等键
    reason: str = ""
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "survivorId": self.survivor_id,
            "mergedId": self.merged_id,
            "survivorVersion": self.survivor_version,
            "mergedVersion": self.merged_version,
            "repointed": self.repointed,
            "mergeKey": self.merge_key,
            "reason": self.reason,
            "detail": self.detail,
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
    "HistoryEntry",
    "IngestResult",
    "IngestStatus",
    "MergeResult",
    "MergeStatus",
    "Resource",
    "ResourceHistory",
]
