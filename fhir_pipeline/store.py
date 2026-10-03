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
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from .errors import PipelineError, RejectReason
from .models import (
    HistoryEntry,
    IngestResult,
    IngestStatus,
    MergeResult,
    MergeStatus,
    RepointedResource,
    Resource,
    ResourceHistory,
)


@dataclass
class _MergeRecord:
    """一次已完成的患者合并（墓碑台账的内部记录，不可变）。"""

    surviving_id: str
    merged_id: str
    surviving_version: int          # 合并完成后保留患者的版本
    merged_tombstone_version: int   # 被合并患者的墓碑版本
    tombstone_hash: str
    repointed: list[RepointedResource] = field(default_factory=list)
    idempotency_key: str | None = None


class ResourceStore:
    """以 (resourceType, id) 为键的版本化存储。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._current: dict[tuple[str, str], Resource] = {}
        self._history: dict[tuple[str, str], list[HistoryEntry]] = {}
        # 幂等键 -> (资源键, 版本号, 内容哈希)
        self._idempotency: dict[str, tuple[tuple[str, str], int, str]] = {}
        # 已被合并患者 id -> 合并台账（墓碑指针）
        self._merge_ledger: dict[str, _MergeRecord] = {}
        # 合并幂等键 -> (被合并患者 id, 保留患者 id)
        self._merge_idempotency: dict[str, tuple[str, str]] = {}

    # ---------- 并发原语 ----------

    @contextmanager
    def atomic(self) -> Iterator[None]:
        """把“校验 + 写入”包成同一临界区，杜绝合并与合入互相穿插。

        读改写必须与 store 内部写操作持同一把可重入锁，因此本区间内
        可以安全地调用 get/exists/merge_patients 等方法。
        """
        with self._lock:
            yield

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

    # ---------- 合并状态查询 ----------

    def patient_replacement(self, patient_id: str) -> str | None:
        """若该患者已被合并，返回保留患者 id；否则返回 None。"""
        with self._lock:
            record = self._merge_ledger.get(patient_id)
            return record.surviving_id if record else None

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
                # 以被合并患者 id 新建 -> 给出可处置的合并结论而非笼统冲突
                if resource_type == "Patient":
                    record = self._merge_ledger.get(resource_id)
                    if record is not None:
                        raise PipelineError(
                            RejectReason.PATIENT_ALREADY_MERGED,
                            f"Patient/{resource_id} 已被合并到 "
                            f"Patient/{record.surviving_id}，不能以该墓碑 id "
                            f"新建；请使用保留记录或换一个新 id",
                            context={"id": resource_id,
                                     "replacementId": record.surviving_id},
                        )
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

            # 已被合并的患者是不可变墓碑：墓碑判定先于版本判定，保证
            # 任何更新（无论携带哪个版本）都得到可处置的合并结论，而非
            # 普通的版本冲突
            if resource_type == "Patient":
                record = self._merge_ledger.get(resource_id)
                if record is not None:
                    raise PipelineError(
                        RejectReason.PATIENT_ALREADY_MERGED,
                        f"Patient/{resource_id} 已被合并到 "
                        f"Patient/{record.surviving_id}，墓碑记录不可更新；"
                        f"请改对保留记录操作",
                        context={"id": resource_id,
                                 "replacementId": record.surviving_id,
                                 "tombstoneVersion": record.merged_tombstone_version},
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
        *,
        surviving_id: str,
        merged_id: str,
        expected_surviving_version: int,
        expected_merged_version: int,
        transform_merged: Callable[[dict[str, Any]], tuple[dict[str, Any], str]],
        transform_survivor: Callable[[dict[str, Any]], tuple[dict[str, Any], str]],
        repoint_resource: Callable[[dict[str, Any]], tuple[dict[str, Any], str] | None],
        idempotency_key: str | None = None,
    ) -> MergeResult:
        """原子地把 merged_id 并入 surviving_id。

        全部判定与写入都在同一把锁内完成（可由 service 包在 atomic()
        区间里调用，RLock 可重入）：
        1. 同一对 (surviving, merged) 重复提交 -> DUPLICATE，不二次改写；
        2. 任一方已是墓碑 / 已并到他人 -> PATIENT_ALREADY_MERGED；
        3. 期望版本落后 -> MERGE_VERSION_CONFLICT，context 分别标明
           保留方/被合并方的期望版本、当前版本与是否落后；
        4. 被合并方写墓碑（active=false + link replaced），保留方追加
           replaces 链接，所有指向被合并方的资源改指保留方并各自 +1
           版本、追加历史；最后落墓碑台账。
        """
        if surviving_id == merged_id:
            raise PipelineError(
                RejectReason.INVALID_VALUE,
                "保留患者与被合并患者不能是同一条记录",
                context={"survivingId": surviving_id, "mergedId": merged_id},
            )

        # 1) 幂等：同一对患者的合并只认第一次结论
        prior = self._merge_ledger.get(merged_id)
        if prior is not None:
            if prior.surviving_id == surviving_id:
                return MergeResult(
                    status=MergeStatus.DUPLICATE,
                    surviving_patient_id=surviving_id,
                    merged_patient_id=merged_id,
                    surviving_version=prior.surviving_version,
                    merged_version=prior.merged_tombstone_version,
                    repointed=list(prior.repointed),
                    payload_hash=prior.tombstone_hash,
                    reason=RejectReason.DUPLICATE_SUBMISSION.value,
                    detail=(
                        f"重复提交：Patient/{merged_id} 已并入 "
                        f"Patient/{surviving_id}（墓碑 v"
                        f"{prior.merged_tombstone_version}），未二次改写、"
                        f"未产生新版本"
                    ),
                )
            raise PipelineError(
                RejectReason.PATIENT_ALREADY_MERGED,
                f"Patient/{merged_id} 已被合并到 Patient/{prior.surviving_id}，"
                f"不能再次合并",
                context={
                    "mergedId": merged_id,
                    "replacementId": prior.surviving_id,
                    "tombstoneVersion": prior.merged_tombstone_version,
                },
            )

        # 显式幂等键若已绑定到另一对患者，属于请求方误用
        if idempotency_key is not None:
            bound = self._merge_idempotency.get(idempotency_key)
            if bound is not None and bound != (merged_id, surviving_id):
                raise PipelineError(
                    RejectReason.VERSION_CONFLICT,
                    f"合并幂等键 {_short(idempotency_key)} 已绑定到另一对患者 "
                    f"{bound[1]} <- {bound[0]}",
                    context={"idempotencyKey": idempotency_key, "boundTo": bound},
                )

        survivor = self._current.get(("Patient", surviving_id))
        victim = self._current.get(("Patient", merged_id))
        if survivor is None or victim is None:
            missing = surviving_id if survivor is None else merged_id
            raise PipelineError(
                RejectReason.RESOURCE_NOT_FOUND,
                f"合并目标患者不存在: Patient/{missing}",
                context={"missingId": missing,
                         "survivingId": surviving_id, "mergedId": merged_id},
            )

        # 保留方自身已是墓碑（被别人并过）-> 不能再作为保留记录
        if merged_id not in self._merge_ledger and surviving_id in self._merge_ledger:
            replacement = self._merge_ledger[surviving_id].surviving_id
            raise PipelineError(
                RejectReason.PATIENT_ALREADY_MERGED,
                f"保留患者 Patient/{surviving_id} 自身已被合并到 "
                f"Patient/{replacement}，不能作为保留记录",
                context={"survivingId": surviving_id, "replacementId": replacement},
            )

        # 3) 乐观版本核对：必须携带双方发起人当时看到的版本
        survivor_stale = expected_surviving_version != survivor.version
        victim_stale = expected_merged_version != victim.version
        if survivor_stale or victim_stale:
            sides = []
            if survivor_stale:
                sides.append("保留记录")
            if victim_stale:
                sides.append("被合并记录")
            raise PipelineError(
                RejectReason.MERGE_VERSION_CONFLICT,
                f"合并版本冲突：{'、'.join(sides)} 在合并发起后已被修改，"
                f"拒绝合并（保留方期望 v{expected_surviving_version}/"
                f"当前 v{survivor.version}，被合并方期望 "
                f"v{expected_merged_version}/当前 v{victim.version}）",
                context={
                    "surviving": {"id": surviving_id,
                                  "expectedVersion": expected_surviving_version,
                                  "currentVersion": survivor.version,
                                  "stale": survivor_stale},
                    "merged": {"id": merged_id,
                               "expectedVersion": expected_merged_version,
                               "currentVersion": victim.version,
                               "stale": victim_stale},
                },
            )

        # 4) 实际改写（全程持锁，外部无法在中间插入更新/挂载）
        tombstone_body, tombstone_hash = transform_merged(victim.body)
        new_victim_version = victim.version + 1
        tombstone = Resource(
            resource_type="Patient", resource_id=merged_id,
            version=new_victim_version, body=tombstone_body,
            payload_hash=tombstone_hash,
        )
        self._current[("Patient", merged_id)] = tombstone
        self._history[("Patient", merged_id)].append(self._entry(tombstone))

        survivor_body, survivor_hash = transform_survivor(survivor.body)
        new_survivor_version = survivor.version + 1
        new_survivor = Resource(
            resource_type="Patient", resource_id=surviving_id,
            version=new_survivor_version, body=survivor_body,
            payload_hash=survivor_hash,
        )
        self._current[("Patient", surviving_id)] = new_survivor
        self._history[("Patient", surviving_id)].append(self._entry(new_survivor))

        repointed: list[RepointedResource] = []
        # 只改指当前版本里仍挂在被合并患者名下的资源；逐条版本化可追溯
        for key, resource in list(self._current.items()):
            if resource.resource_type == "Patient":
                continue
            changed = repoint_resource(resource.body)
            if changed is None:
                continue
            new_body, new_hash = changed
            new_version = resource.version + 1
            moved = Resource(
                resource_type=resource.resource_type,
                resource_id=resource.resource_id,
                version=new_version, body=new_body, payload_hash=new_hash,
            )
            self._current[key] = moved
            self._history[key].append(self._entry(moved))
            repointed.append(RepointedResource(
                resource_type=moved.resource_type, resource_id=moved.resource_id,
                version=new_version,
            ))

        record = _MergeRecord(
            surviving_id=surviving_id, merged_id=merged_id,
            surviving_version=new_survivor_version,
            merged_tombstone_version=new_victim_version,
            tombstone_hash=tombstone_hash, repointed=repointed,
            idempotency_key=idempotency_key,
        )
        self._merge_ledger[merged_id] = record
        if idempotency_key is not None:
            self._merge_idempotency[idempotency_key] = (merged_id, surviving_id)

        return MergeResult(
            status=MergeStatus.MERGED,
            surviving_patient_id=surviving_id,
            merged_patient_id=merged_id,
            surviving_version=new_survivor_version,
            merged_version=new_victim_version,
            repointed=repointed,
            payload_hash=tombstone_hash,
            detail=(
                f"合并成功：Patient/{merged_id} 标记为墓碑并指向 "
                f"Patient/{surviving_id}；改指资源 {len(repointed)} 条；"
                f"保留方 v{survivor.version} -> v{new_survivor_version}，"
                f"被合并方 v{victim.version} -> v{new_victim_version}"
            ),
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
