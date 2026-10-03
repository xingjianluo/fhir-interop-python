"""线程安全的版本化内存存储。

并发安全：所有读改写均在同一把 RLock 内完成（原子 check-then-act）。
不丢更新：更新必须携带期望版本号（乐观锁），与库内不一致即
VERSION_CONFLICT 拒绝。
不重复：同一幂等键（默认取内容哈希）的重复提交返回 DUPLICATE，
不产生新版本/新记录。
可追溯：每次成功写入都会在历史链上追加不可变的 HistoryEntry。
"""

from __future__ import annotations

import copy
import threading
from typing import Any

from .errors import PipelineError, RejectReason
from .models import (
    HistoryEntry,
    IngestResult,
    IngestStatus,
    MergeResult,
    MergeStatus,
    Resource,
    ResourceHistory,
)
from .normalizer import _REFERENCE_FIELDS, compute_payload_hash

# 被合并患者在 body 中的标记：link[type=replaced-by].other.reference
MERGE_LINK_TYPE = "replaced-by"


class ResourceStore:
    """以 (resourceType, id) 为键的版本化存储。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._current: dict[tuple[str, str], Resource] = {}
        self._history: dict[tuple[str, str], list[HistoryEntry]] = {}
        # 幂等键 -> (资源键, 版本号, 内容哈希)
        self._idempotency: dict[str, tuple[tuple[str, str], int, str]] = {}
        # 合并幂等键 -> 首次合并结论（重复提交原样返回，不二次改写）
        self._merges: dict[str, MergeResult] = {}

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

    def merged_into(self, resource_type: str, resource_id: str) -> str | None:
        """若该资源是被合并的患者，返回其保留记录 id，否则 None。"""
        with self._lock:
            resource = self._current.get((resource_type, resource_id))
            if resource is None:
                return None
            return merged_target_of(resource.body)

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

            merged_target = merged_target_of(existing.body)
            if merged_target is not None:
                raise PipelineError(
                    RejectReason.MERGED_RESOURCE,
                    f"{resource_type}/{resource_id} 已并入 Patient/{merged_target}，"
                    "禁止再直接更新；请改写保留记录",
                    context={
                        "resourceType": resource_type,
                        "id": resource_id,
                        "mergedInto": merged_target,
                    },
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

    # ---------- 患者合并 ----------

    def merge_patients(
        self,
        survivor_id: str,
        merged_id: str,
        *,
        expected_survivor_version: int,
        expected_merged_version: int,
        idempotency_key: str | None = None,
    ) -> MergeResult:
        """把被合并患者并入保留患者（原子操作，全程持锁）。

        - 两条记录各自的期望版本必须与库内一致，否则 VERSION_CONFLICT，
          context.staleResource 区分 "survivor"/"merged" 并给出当前版本；
        - 同一合并键重复提交 -> DUPLICATE，返回首次结论，不二次改写；
        - 所有指向被合并患者的引用改指保留患者（版本 +1、追加历史）；
        - 被合并患者不删除：版本 +1，body 标记 active=false 与
          link[type=replaced-by] -> 保留记录，历史可追溯。
        """
        if survivor_id == merged_id:
            raise PipelineError(
                RejectReason.INVALID_VALUE,
                "保留记录与被合并记录不能是同一条",
                context={"survivorId": survivor_id, "mergedId": merged_id},
            )
        merge_key = idempotency_key or f"merge:{merged_id}->{survivor_id}"
        with self._lock:
            prior = self._merges.get(merge_key)
            if prior is not None:
                return MergeResult(
                    status=MergeStatus.DUPLICATE,
                    survivor_id=prior.survivor_id,
                    merged_id=prior.merged_id,
                    survivor_version=prior.survivor_version,
                    merged_version=prior.merged_version,
                    repointed=list(prior.repointed),
                    merge_key=merge_key,
                    reason=RejectReason.DUPLICATE_SUBMISSION.value,
                    detail=(
                        f"重复提交：合并键 {_short(merge_key)} 已执行，"
                        "返回首次结论，未二次改写"
                    ),
                )

            skey, mkey = ("Patient", survivor_id), ("Patient", merged_id)
            survivor = self._current.get(skey)
            merged = self._current.get(mkey)
            if survivor is None:
                raise PipelineError(
                    RejectReason.RESOURCE_NOT_FOUND,
                    f"保留记录不存在: Patient/{survivor_id}",
                    context={"resourceType": "Patient", "id": survivor_id},
                )
            if merged is None:
                raise PipelineError(
                    RejectReason.RESOURCE_NOT_FOUND,
                    f"被合并记录不存在: Patient/{merged_id}",
                    context={"resourceType": "Patient", "id": merged_id},
                )

            already = merged_target_of(merged.body)
            if already is not None:
                if already == survivor_id:
                    # 同目标合并已完成（不同幂等键的重复请求）：幂等返回
                    result = MergeResult(
                        status=MergeStatus.DUPLICATE,
                        survivor_id=survivor_id,
                        merged_id=merged_id,
                        survivor_version=survivor.version,
                        merged_version=merged.version,
                        repointed=[],
                        merge_key=merge_key,
                        reason=RejectReason.DUPLICATE_SUBMISSION.value,
                        detail=(
                            f"Patient/{merged_id} 已并入 Patient/{survivor_id}，"
                            "本次为重复请求，未二次改写"
                        ),
                    )
                    self._merges[merge_key] = result
                    return result
                raise PipelineError(
                    RejectReason.VERSION_CONFLICT,
                    f"Patient/{merged_id} 已并入 Patient/{already}，"
                    "不能再次并入其他记录",
                    context={
                        "mergedId": merged_id,
                        "mergedInto": already,
                        "requestedSurvivor": survivor_id,
                    },
                )

            if survivor.version != expected_survivor_version:
                raise PipelineError(
                    RejectReason.VERSION_CONFLICT,
                    f"保留记录版本落后：发起人基于 v{expected_survivor_version}，"
                    f"库内当前 v{survivor.version}，合并被拒绝",
                    context={
                        "staleResource": "survivor",
                        "survivorId": survivor_id,
                        "expectedSurvivorVersion": expected_survivor_version,
                        "currentSurvivorVersion": survivor.version,
                        "currentMergedVersion": merged.version,
                    },
                )
            if merged.version != expected_merged_version:
                raise PipelineError(
                    RejectReason.VERSION_CONFLICT,
                    f"被合并记录版本落后：发起人基于 v{expected_merged_version}，"
                    f"库内当前 v{merged.version}，合并被拒绝",
                    context={
                        "staleResource": "merged",
                        "mergedId": merged_id,
                        "expectedMergedVersion": expected_merged_version,
                        "currentMergedVersion": merged.version,
                        "currentSurvivorVersion": survivor.version,
                    },
                )

            # 1) 所有引用被合并患者的资源改指保留患者（版本 +1 + 历史）
            repointed: list[dict[str, Any]] = []
            for key, resource in list(self._current.items()):
                new_body, changed = _repoint_references(
                    resource.body, merged_id, survivor_id
                )
                if not changed:
                    continue
                new_version = resource.version + 1
                updated = Resource(
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    version=new_version,
                    body=new_body,
                    payload_hash=compute_payload_hash(new_body),
                )
                self._current[key] = updated
                self._history[key].append(self._entry(updated))
                repointed.append({
                    "resourceType": resource.resource_type,
                    "id": resource.resource_id,
                    "fromVersion": resource.version,
                    "toVersion": new_version,
                })

            # 2) 被合并患者标记为已合并（不删除，版本 +1 + 历史）
            merged_body = copy.deepcopy(merged.body)
            merged_body["active"] = False
            links = merged_body.get("link")
            links = list(links) if isinstance(links, list) else []
            links.append({
                "other": {"reference": f"Patient/{survivor_id}"},
                "type": MERGE_LINK_TYPE,
            })
            merged_body["link"] = links
            merged_new_version = merged.version + 1
            merged_updated = Resource(
                resource_type="Patient",
                resource_id=merged_id,
                version=merged_new_version,
                body=merged_body,
                payload_hash=compute_payload_hash(merged_body),
            )
            self._current[mkey] = merged_updated
            self._history[mkey].append(self._entry(merged_updated))

            result = MergeResult(
                status=MergeStatus.MERGED,
                survivor_id=survivor_id,
                merged_id=merged_id,
                survivor_version=survivor.version,
                merged_version=merged_new_version,
                repointed=repointed,
                merge_key=merge_key,
                detail=(
                    f"合并完成：Patient/{merged_id}#v{merged.version} -> "
                    f"Patient/{survivor_id}#v{survivor.version}，"
                    f"改指资源 {len(repointed)} 条，被合并记录标记为 "
                    f"v{merged_new_version}（replaced-by）"
                ),
            )
            self._merges[merge_key] = result
            return result

    # ---------- 内部 ----------

    @staticmethod
    def _entry(resource: Resource) -> HistoryEntry:
        return HistoryEntry(
            version=resource.version,
            version_id=resource.version_id,
            payload_hash=resource.payload_hash,
            body=dict(resource.body),
        )


def merged_target_of(body: dict[str, Any]) -> str | None:
    """从资源 body 中读取合并标记，返回保留记录 id（无则 None）。"""
    links = body.get("link")
    if not isinstance(links, list):
        return None
    for link in links:
        if not isinstance(link, dict) or link.get("type") != MERGE_LINK_TYPE:
            continue
        other = link.get("other")
        if isinstance(other, dict) and isinstance(other.get("reference"), str):
            ref = other["reference"]
            if ref.startswith("Patient/"):
                return ref.split("/", 1)[1]
    return None


def _repoint_references(
    body: dict[str, Any], merged_id: str, survivor_id: str
) -> tuple[dict[str, Any], bool]:
    """把 body 中所有指向 Patient/{merged_id} 的引用改指保留记录。"""
    resource_type = body.get("resourceType")
    fields = _REFERENCE_FIELDS.get(resource_type, {})
    if not fields:
        return body, False
    changed = False
    new_body = copy.deepcopy(body)
    for field_name in fields:
        ref = new_body.get(field_name)
        if isinstance(ref, dict) and ref.get("reference") == f"Patient/{merged_id}":
            ref["reference"] = f"Patient/{survivor_id}"
            changed = True
    return new_body, changed


def _short(value: str, length: int = 12) -> str:
    return value if len(value) <= length else value[:length] + "..."


__all__ = ["ResourceStore"]
