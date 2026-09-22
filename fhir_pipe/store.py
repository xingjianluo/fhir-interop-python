"""版本化存储（内存实现，线程安全）。

语义（对齐 FHIR/REST 乐观锁）：
- ``create``：新建资源。若 ID 已存在 -> DUPLICATE_ID。
- ``update(resource, expected_version)``：
    * 资源不存在 -> NOT_FOUND；
    * expected_version 与当前版本不一致 -> VERSION_CONFLICT
      （并发更新/过期版本会走到这里，旧版本不会覆盖新版本）；
    * 版本一致且业务内容与当前版本相同 -> 幂等重放，不新增版本；
    * 版本一致且内容变化 -> 版本号 +1，旧版本进入历史，永不丢失。
- 全部历史版本可通过 ``history`` 追溯。
"""

from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass

from .errors import RejectReason, RejectionError
from .models import Resource


@dataclass
class PutResult:
    resource: Resource
    version_id: int
    replayed: bool  # True 表示命中幂等重放


def _business_key(r: Resource) -> tuple:
    """提取与版本无关的业务字段，用于判断“同版本重复提交”。"""
    d = r.to_dict()
    d.pop("meta", None)
    # dict 不可哈希，直接返回规整后的嵌套元组
    def _norm(x):
        if isinstance(x, dict):
            return tuple(sorted((k, _norm(v)) for k, v in x.items()))
        if isinstance(x, list):
            return tuple(_norm(v) for v in x)
        return x
    return (r.resource_type, r.id, _norm(d))


class VersionedStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        # key = (resourceType, id) -> 各版本（v1, v2, ...）
        self._versions: dict[tuple[str, str], list[Resource]] = {}

    def create(self, resource: Resource) -> PutResult:
        key = (resource.resource_type, resource.id)
        with self._lock:
            if key in self._versions:
                current = self._versions[key][-1]
                raise RejectionError(
                    RejectReason.DUPLICATE_ID,
                    f"资源 {key[0]}/{key[1]} 已存在（当前版本 "
                    f"{current.meta.version_id}）；更新请带 If-Match",
                    field="id",
                )
            stored = copy.deepcopy(resource)
            stored.meta.version_id = 1
            stored.meta.last_updated = time.time()
            self._versions[key] = [stored]
            return PutResult(resource=copy.deepcopy(stored), version_id=1, replayed=False)

    def update(
        self, resource: Resource, expected_version: int
    ) -> PutResult:
        key = (resource.resource_type, resource.id)
        with self._lock:
            versions = self._versions.get(key)
            if not versions:
                raise RejectionError(
                    RejectReason.NOT_FOUND,
                    f"资源 {key[0]}/{key[1]} 不存在，无法更新（请先创建）",
                )
            current = versions[-1]
            if expected_version != current.meta.version_id:
                raise RejectionError(
                    RejectReason.VERSION_CONFLICT,
                    f"版本冲突：提交基于 v{expected_version}，"
                    f"当前已是 v{current.meta.version_id}；"
                    f"请读取最新版本、合并后带新版本号重试",
                )

            # 版本号一致：内容相同 = 重复提交（幂等），内容变化 = 新版本
            if _business_key(resource) == _business_key(current):
                return PutResult(
                    resource=copy.deepcopy(current),
                    version_id=current.meta.version_id,
                    replayed=True,
                )

            new_version = copy.deepcopy(resource)
            new_version.meta.version_id = current.meta.version_id + 1
            new_version.meta.last_updated = time.time()
            versions.append(new_version)
            return PutResult(
                resource=copy.deepcopy(new_version),
                version_id=new_version.meta.version_id,
                replayed=False,
            )

    def get(self, resource_type: str, resource_id: str) -> Resource | None:
        with self._lock:
            versions = self._versions.get((resource_type, resource_id))
            return copy.deepcopy(versions[-1]) if versions else None

    def require(self, resource_type: str, resource_id: str) -> Resource:
        found = self.get(resource_type, resource_id)
        if found is None:
            raise RejectionError(
                RejectReason.NOT_FOUND,
                f"资源 {resource_type}/{resource_id} 不存在",
            )
        return found

    def history(self, resource_type: str, resource_id: str) -> list[Resource]:
        with self._lock:
            versions = self._versions.get((resource_type, resource_id))
            if not versions:
                raise RejectionError(
                    RejectReason.NOT_FOUND,
                    f"资源 {resource_type}/{resource_id} 不存在，无历史",
                )
            return [copy.deepcopy(r) for r in versions]

    def list(self, resource_type: str | None = None) -> list[Resource]:
        with self._lock:
            out: list[Resource] = []
            for (rtype, _rid), versions in sorted(
                self._versions.items(), key=lambda kv: kv[0]
            ):
                if resource_type is None or rtype == resource_type:
                    out.append(copy.deepcopy(versions[-1]))
            return out
