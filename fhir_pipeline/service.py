"""各入口（Python API / HTTP / CLI）共用的合入编排服务。

流程：parse_payload -> normalize -> validate_references（更新时
引用允许指向自身）-> 按 id 是否存在分流 create/update。
所有判定都通过 logging 打印输入摘要与判定依据，便于在各入口下
得到一致且可解释的结论。
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from . import normalizer
from .deidentify import PseudonymMapper, deidentify_resource
from .errors import PipelineError, RejectReason
from .models import IngestResult, IngestStatus
from .store import ResourceStore
from .validator import validate_references

logger = logging.getLogger("fhir_pipeline.service")


class _SelfRefAwareStore:
    """让“更新自身”时的自引用通过校验（如 Observation 不涉及，
    但资源更新后引用自身类型时需要），其余引用仍必须已存在。"""

    def __init__(self, store: ResourceStore, self_key: tuple[str, str] | None):
        self._store = store
        self._self_key = self_key

    def exists(self, resource_type: str, resource_id: str) -> bool:
        if self._self_key == (resource_type, resource_id):
            return True
        return self._store.exists(resource_type, resource_id)


class IngestionService:
    """解析 -> 归一化 -> 引用校验 -> 版本化合入。"""

    def __init__(
        self,
        store: ResourceStore | None = None,
        *,
        pseudonym_salt: str = "local-dev-salt",
    ) -> None:
        self.store = store or ResourceStore()
        self._pseudonym_salt = pseudonym_salt
        self._mapper: PseudonymMapper | None = None

    # ---------- 合入 ----------

    def ingest(
        self,
        raw: str | bytes | dict[str, Any],
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> IngestResult:
        """统一合入入口：自动区分 CREATED / UPDATED / DUPLICATE；
        非法数据与版本冲突以 PipelineError 明确拒绝。
        """
        rid = request_id or uuid.uuid4().hex[:8]
        snippet = _raw_snippet(raw)
        logger.info("[%s] 收到合入请求 expected_version=%s idem=%s 输入=%s",
                    rid, expected_version, idempotency_key, snippet)
        try:
            parsed = normalizer.parse_payload(raw)
            body = normalizer.normalize(parsed)
            resource_type = body["resourceType"]
            resource_id = body["id"]
            payload_hash = normalizer.compute_payload_hash(body)
            logger.info(
                "[%s] 归一化通过 type=%s id=%s hash=%s",
                rid, resource_type, resource_id, payload_hash[:12],
            )

            existing = self.store.get(resource_type, resource_id)
            if existing is None:
                if expected_version is not None:
                    raise PipelineError(
                        RejectReason.RESOURCE_NOT_FOUND,
                        f"携带期望版本 v{expected_version} 更新，但资源 "
                        f"{resource_type}/{resource_id} 不存在",
                        context={"resourceType": resource_type, "id": resource_id},
                    )
                # 新建前校验引用（所有目标必须已入库）
                validate_references(body, self.store)
                result = self.store.create(
                    resource_type, resource_id, body, payload_hash,
                    idempotency_key=idempotency_key,
                )
            else:
                validate_references(
                    body, _SelfRefAwareStore(self.store, (resource_type, resource_id))
                )
                result = self.store.update(
                    resource_type, resource_id, body, payload_hash,
                    expected_version=expected_version,
                    idempotency_key=idempotency_key,
                )
        except PipelineError as exc:
            logger.warning(
                "[%s] 拒绝合入 reason=%s 依据=%s 输入=%s",
                rid, exc.reason.value, exc.message, snippet,
            )
            raise
        logger.info("[%s] 判定结论 status=%s %s", rid, result.status.value,
                    result.detail)
        return result

    # ---------- 查询 ----------

    def get_history(self, resource_type: str, resource_id: str):
        return self.store.history(resource_type, resource_id)

    def get(self, resource_type: str, resource_id: str):
        return self.store.get(resource_type, resource_id)

    # ---------- 去标识化 ----------

    @property
    def mapper(self) -> PseudonymMapper:
        if self._mapper is None:
            self._mapper = PseudonymMapper(self._pseudonym_salt)
        return self._mapper

    def deidentify_all(self) -> dict[str, Any]:
        """对库内当前版本资源执行幂等去标识化。

        返回 mapping（真实标识 -> 伪标识）与 resources（去标识化后的
        当前版本快照）。重复执行：映射不增、引用不乱。
        """
        resources = self.store.all_resources()
        logger.info("开始去标识化，资源数=%d", len(resources))
        deidentified = [
            deidentify_resource(r.body, self.mapper) for r in resources
        ]
        mapping = self.mapper.snapshot()
        logger.info("去标识化完成，映射条目=%d", len(mapping))
        return {"mapping": mapping, "resources": deidentified}


def _raw_snippet(raw: Any, limit: int = 160) -> str:
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return f"<{len(raw)} bytes 非 UTF-8>"
    if isinstance(raw, str):
        compact = " ".join(raw.split())
        return compact if len(compact) <= limit else compact[:limit] + "..."
    return repr(raw)[:limit]


__all__ = ["IngestionService"]
