"""统一接入服务：合入、更新、去标识化的一致性入口。

无论调用方走 Python API 还是 HTTP，都经过本服务，因此各入口下
结论一致且可解释：拒绝原因来自统一的 RejectionError，返回结构
统一为 :class:`IngestResult`，关键判定写入 ``fhir_pipe`` logger。

合入顺序约束：为保证引用可解析，引用目标必须先于引用方存在
（Patient 先于 Encounter/Observation；Encounter 先于引用它的
Observation）。目标缺失 -> BROKEN_REFERENCE。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .deidentify import PseudonymTable, deidentify_resource
from .errors import RejectReason, RejectionError
from .models import Encounter, Observation, Resource
from .parser import parse_resource
from .store import PutResult, VersionedStore

logger = logging.getLogger("fhir_pipe")


@dataclass
class IngestResult:
    resource: Resource
    version_id: int
    created: bool
    replayed: bool
    deidentified: bool = False

    def to_dict(self) -> dict:
        d = self.resource.to_dict()
        d["meta"] = {
            **d["meta"],
            "ingest": {
                "versionId": self.version_id,
                "created": self.created,
                "replayed": self.replayed,
                "deidentified": self.deidentified,
            },
        }
        return d


class IngestionService:
    """组合 parser + versioned store + pseudonym table 的门面。"""

    def __init__(self, *, salt: str = "local-static-salt") -> None:
        self.store = VersionedStore()
        self.table = PseudonymTable(salt=salt)

    # ---- 引用完整性 --------------------------------------------------

    def _check_references(self, resource: Resource) -> None:
        refs: list[tuple[str, str, str]] = []  # (字段, 类型, id)
        if isinstance(resource, Encounter):
            refs.append(("subject", resource.subject.resource_type,
                         resource.subject.resource_id))
        elif isinstance(resource, Observation):
            refs.append(("subject", resource.subject.resource_type,
                         resource.subject.resource_id))
            if resource.encounter is not None:
                refs.append(("encounter", resource.encounter.resource_type,
                             resource.encounter.resource_id))

        for field_name, rtype, rid in refs:
            if self.store.get(rtype, rid) is None:
                # 自引用例外：引用目标恰好是正在写入的资源自身
                if rtype == resource.resource_type and rid == resource.id:
                    continue
                raise RejectionError(
                    RejectReason.BROKEN_REFERENCE,
                    f"资源 {resource.resource_type}/{resource.id} 的引用 "
                    f"{field_name} -> {rtype}/{rid} 目标不存在；"
                    f"请先合入目标资源",
                    field=field_name,
                )

    # ---- 合入主流程 --------------------------------------------------

    def ingest(
        self,
        payload: Any,
        *,
        expected_version: int | None = None,
        deidentify: bool = False,
    ) -> IngestResult:
        """解析并合入一个资源。

        - expected_version is None：创建语义（ID 已存在 -> DUPLICATE_ID）。
        - expected_version 为整数：更新语义（版本过期 -> VERSION_CONFLICT，
          资源不存在 -> NOT_FOUND；内容与版本都相同视为幂等重放）。
        - deidentify=True：合入后返回去标识化视图；映射稳定，重复执行
          结果一致（存储中的原始资源不变）。
        """
        logger.info("ingest 输入: expected_version=%s deidentify=%s payload=%r",
                    expected_version, deidentify, payload)

        resource = parse_resource(payload)  # 解析失败在此抛可区分异常
        self._check_references(resource)

        if expected_version is None:
            put: PutResult = self.store.create(resource)
            created, decision = True, "create"
        else:
            put = self.store.update(resource, expected_version=expected_version)
            created = False
            decision = "idempotent-replay" if put.replayed else "update"

        logger.info(
            "ingest 判定: %s %s/%s -> version=%d replayed=%s",
            decision, put.resource.resource_type, put.resource.id,
            put.version_id, put.replayed,
        )

        view = put.resource
        did_deid = False
        if deidentify:
            view = deidentify_resource(put.resource, self.table)
            did_deid = True
            logger.info(
                "ingest 判定: 去标识化视图 id=%s（映射表共 %d 条）",
                view.id, len(self.table.mappings()),
            )

        return IngestResult(
            resource=view,
            version_id=put.version_id,
            created=created,
            replayed=put.replayed,
            deidentified=did_deid,
        )

    # ---- 读取 --------------------------------------------------------

    def get(self, resource_type: str, resource_id: str) -> Resource:
        return self.store.require(resource_type, resource_id)

    def history(self, resource_type: str, resource_id: str) -> list[Resource]:
        return self.store.history(resource_type, resource_id)

    def deidentify_all(self) -> list[Resource]:
        """对存储中全部资源执行去标识化（拓扑顺序：Patient→Encounter→Observation）。

        重复执行结果完全一致：映射表稳定、引用关系不变。
        """
        order = ["Patient", "Encounter", "Observation"]
        raw = [
            r for t in order for r in self.store.list(t)
        ]
        logger.info("deidentify_all 输入: %d 个资源", len(raw))
        out = [deidentify_resource(r, self.table) for r in raw]
        logger.info(
            "deidentify_all 判定: 完成，映射=%r", self.table.mappings()
        )
        return out
