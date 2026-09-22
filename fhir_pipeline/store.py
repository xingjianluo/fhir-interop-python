"""线程安全的版本化内存存储。

并发安全：所有读改写均在同一把 RLock 内完成（原子 check-then-act）。
不丢更新：更新必须携带期望版本号（乐观锁），与库内不一致即
VERSION_CONFLICT 拒绝。
不重复：同一幂等键（默认取内容哈希）的重复提交返回 DUPLICATE，
不产生新版本/新记录。
可追溯：每次成功写入都会在历史链上追加不可变的 HistoryEntry。
"""

from __future__ import annotations

import threading
from typing import Any

from .errors import PipelineError, RejectReason
from .models import (
    HistoryEntry,
    IngestResult,
    IngestStatus,
    Resource,
    ResourceHistory,
)


class ResourceStore:
    """以 (resourceType, id) 为键的版本化存储。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._current: dict[tuple[str, str], Resource] = {}
        self._history: dict[tuple[str, str], list[HistoryEntry]] = {}
        # 幂等键 -> (资源键, 版本号, 内容哈希)
        self._idempotency: dict[str, tuple[tuple[str, str], int, str]] = {}

    # ---------- 读 ----------

    def exists(self, resource_type: str, resource_id: str) -> bool:
        with self._lock:
            return (resource_type, resource_id) in self._current

    def get(self, resource_type: str, resource_id: str) -> Resource | None:
        with self._lock:
            return self._current.get((resource_type, resource_id))

    def all_resources(self) -> list[Resource]:
        with self._lock:
            return list(self._current.values())

    def history(self, resource_type: str, resource_id: str) -> ResourceHistory:
        key = (resource_type, resource_id)
        with self._lock:
            entries = self._history.get(key)
            if not entries:
                raise PipelineError(
                    RejectReason.RESOURCE_NOT_FOUND,
                    f"资源不存在，无历史: {resource_type}/{resource_id}",
                    context={"resourceType": resource_type, "id": resource_id},
                )
            return ResourceHistory(
                resource_type=resource_type,
                resource_id=resource_id,
                current_version=self._current[key].version,
                entries=list(entries),
            )

    # ---------- 写 ----------

    def create(
        self,
        resource_type: str,
        resource_id: str,
        body: dict[str, Any],
        payload_hash: str,
        *,
        idempotency_key: str | None = None,
    ) -> IngestResult:
        """新建资源（版本 1）。

        - 幂等键首次出现 -> CREATED；
        - 同键再次提交且指向同一资源 -> DUPLICATE；
        - 同键被用于不同内容/不同资源 -> VERSION_CONFLICT（请求方误用）；
        - 资源 id 已存在但走 create -> VERSION_CONFLICT（应改用 update）。
        """
        key = (resource_type, resource_id)
        idem = idempotency_key or payload_hash
        with self._lock:
            prior = self._idempotency.get(idem)
            if prior is not None:
                prior_key, prior_version, prior_hash = prior
                if prior_key == key:
                    existing = self._current[key]
                    return IngestResult(
                        status=IngestStatus.DUPLICATE,
                        resource_type=resource_type,
                        resource_id=resource_id,
                        version=existing.version,
                        version_id=existing.version_id,
                        payload_hash=existing.payload_hash,
                        reason=RejectReason.DUPLICATE_SUBMISSION.value,
                        detail=(
                            f"重复提交：幂等键 {_short(idem)} 已记录为版本 "
                            f"{existing.version}，内容哈希一致，未产生新记录"
                        ),
                        existing_version=existing.version,
                    )
                raise PipelineError(
                    RejectReason.VERSION_CONFLICT,
                    f"幂等键 {_short(idem)} 已绑定到另一个资源 "
                    f"{prior_key[0]}/{prior_key[1]}#v{prior_version}",
                    context={
                        "idempotencyKey": idem,
                        "boundTo": f"{prior_key[0]}/{prior_key[1]}",
                        "boundVersion": prior_version,
                    },
                )

            if key in self._current:
                existing = self._current[key]
                raise PipelineError(
                    RejectReason.VERSION_CONFLICT,
                    f"资源 {resource_type}/{resource_id} 已存在（版本 "
                    f"{existing.version}），新建被拒绝；请携带 If-Match 走更新",
                    context={
                        "resourceType": resource_type,
                        "id": resource_id,
                        "existingVersion": existing.version,
                        "existingHash": existing.payload_hash,
                    },
                )

            resource = Resource(
                resource_type=resource_type,
                resource_id=resource_id,
                version=1,
                body=body,
                payload_hash=payload_hash,
            )
            self._current[key] = resource
            self._history[key] = [self._entry(resource)]
            self._idempotency[idem] = (key, 1, payload_hash)
            return IngestResult(
                status=IngestStatus.CREATED,
                resource_type=resource_type,
                resource_id=resource_id,
                version=1,
                version_id="1",
                payload_hash=payload_hash,
                detail=f"新建成功，分配版本 1（幂等键 {_short(idem)}）",
            )

    def update(
        self,
        resource_type: str,
        resource_id: str,
        body: dict[str, Any],
        payload_hash: str,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> IngestResult:
        """更新已有资源。

        - expected_version 与库内当前版本不一致 -> VERSION_CONFLICT；
        - 内容哈希与当前版本一致（重复提交）-> DUPLICATE，不新增版本；
        - 否则 -> UPDATED，版本号 +1 并追加历史。
        """
        key = (resource_type, resource_id)
        idem = idempotency_key or payload_hash
        with self._lock:
            existing = self._current.get(key)
            if existing is None:
                raise PipelineError(
                    RejectReason.RESOURCE_NOT_FOUND,
                    f"更新目标不存在: {resource_type}/{resource_id}，请先新建",
                    context={"resourceType": resource_type, "id": resource_id},
                )

            if expected_version is not None and expected_version != existing.version:
                raise PipelineError(
                    RejectReason.VERSION_CONFLICT,
                    f"版本冲突：客户端基于 v{expected_version} 更新，库内当前为 "
                    f"v{existing.version}，拒绝以防丢失更新",
                    context={
                        "resourceType": resource_type,
                        "id": resource_id,
                        "expectedVersion": expected_version,
                        "currentVersion": existing.version,
                    },
                )

            # 内容未变：无论走哪个入口，都按重复提交幂等处理
            if existing.payload_hash == payload_hash:
                return IngestResult(
                    status=IngestStatus.DUPLICATE,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    version=existing.version,
                    version_id=existing.version_id,
                    payload_hash=existing.payload_hash,
                    reason=RejectReason.DUPLICATE_SUBMISSION.value,
                    detail=(
                        f"重复提交：内容哈希 {_short(payload_hash)} 与当前版本 "
                        f"v{existing.version} 一致，未产生新版本"
                    ),
                    existing_version=existing.version,
                )

            # 显式幂等键若已绑定到别的版本/内容，属于冲突重试误用
            prior = self._idempotency.get(idem)
            if prior is not None and prior[2] != payload_hash:
                raise PipelineError(
                    RejectReason.VERSION_CONFLICT,
                    f"幂等键 {_short(idem)} 已绑定到不同内容",
                    context={"idempotencyKey": idem},
                )

            new_version = existing.version + 1
            updated = Resource(
                resource_type=resource_type,
                resource_id=resource_id,
                version=new_version,
                body=body,
                payload_hash=payload_hash,
            )
            self._current[key] = updated
            self._history[key].append(self._entry(updated))
            self._idempotency[idem] = (key, new_version, payload_hash)
            return IngestResult(
                status=IngestStatus.UPDATED,
                resource_type=resource_type,
                resource_id=resource_id,
                version=new_version,
                version_id=str(new_version),
                payload_hash=payload_hash,
                detail=(
                    f"更新成功：v{existing.version} -> v{new_version}"
                    + (
                        f"（客户端基于 v{expected_version}）"
                        if expected_version is not None
                        else "（无条件更新）"
                    )
                ),
                existing_version=existing.version,
            )

    # ---------- 内部 ----------

    @staticmethod
    def _entry(resource: Resource) -> HistoryEntry:
        return HistoryEntry(
            version=resource.version,
            version_id=resource.version_id,
            payload_hash=resource.payload_hash,
            body=dict(resource.body),
        )


def _short(value: str, length: int = 12) -> str:
    return value if len(value) <= length else value[:length] + "..."


__all__ = ["ResourceStore"]
