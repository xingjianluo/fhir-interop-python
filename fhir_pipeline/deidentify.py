"""跨资源去标识化。

稳定性：伪标识由 HMAC-SHA256(盐, "Type/真实id") 截取得出，
同一真实标识永远得到同一伪标识，与调用次数、资源种类、入口无关。
一致性：引用字段（subject/encounter）中的 Type/id 用同一映射一并
改写，因此去标识化后的资源之间引用仍可解析。
幂等性：对已经是伪标识的资源再次执行不会产生新映射，也不会破坏
已有引用（伪标识输入回映到自身）。
"""

from __future__ import annotations

import hashlib
import hmac
import threading
from typing import Any

from .errors import PipelineError, RejectReason
from .normalizer import _REFERENCE_FIELDS, _split_reference

_PREFIX = "PSEUDO-"


class PseudonymMapper:
    """真实标识 -> 伪标识的稳定映射。"""

    def __init__(self, salt: str = "local-dev-salt") -> None:
        if not isinstance(salt, str) or not salt:
            raise PipelineError(
                RejectReason.INVALID_VALUE, "去标识化盐必须是非空字符串",
                context={"field": "salt"},
            )
        self._salt = salt
        self._lock = threading.Lock()
        # "Type/realId" -> "PSEUDO-xxxx"
        self._mapping: dict[str, str] = {}

    def pseudonym_for(self, resource_type: str, real_id: str) -> str:
        """返回某个具体资源的稳定伪标识。"""
        return self.pseudonym(f"{resource_type}/{real_id}")

    def pseudonym(self, key: str) -> str:
        """键 "Type/id" -> 稳定伪标识；已是伪标识的键原样返回（幂等）。"""
        resource_id = key.split("/", 1)[1] if "/" in key else key
        if resource_id.startswith(_PREFIX):
            # 已去标识化：直接返回，重复执行不产生任何新映射
            return resource_id
        with self._lock:
            existing = self._mapping.get(key)
            if existing is not None:
                return existing
            digest = hmac.new(
                self._salt.encode("utf-8"), key.encode("utf-8"), hashlib.sha256
            ).hexdigest()
            pseudo = f"{_PREFIX}{digest[:16]}"
            self._mapping[key] = pseudo
            return pseudo

    def snapshot(self) -> dict[str, str]:
        """返回当前映射快照（真实标识 -> 伪标识），用于审计/核对。"""
        with self._lock:
            return dict(self._mapping)


def deidentify_resource(
    body: dict[str, Any], mapper: PseudonymMapper
) -> dict[str, Any]:
    """返回去标识化后的新字典（不修改入参）。

    处理：资源 id、所有跨资源引用、Patient.identifier；
    并在 meta 标记 _deidentified=true。直接标识符（姓名、出生日期）
    按规则移除/泛化。
    """
    import copy

    resource_type = body.get("resourceType")
    resource_id = body.get("id")
    if not isinstance(resource_type, str) or not isinstance(resource_id, str):
        raise PipelineError(
            RejectReason.MISSING_FIELD,
            "去标识化需要 resourceType 与 id",
        )

    out = copy.deepcopy(body)
    out["id"] = mapper.pseudonym_for(resource_type, resource_id)

    # 1) 引用随映射改写——使用同一张映射表，保证跨资源一致且可解析
    for field_name in _REFERENCE_FIELDS.get(resource_type, {}):
        ref = out.get(field_name)
        if isinstance(ref, dict) and isinstance(ref.get("reference"), str):
            t, i = _split_reference(ref["reference"])
            ref["reference"] = f"{t}/{mapper.pseudonym_for(t, i)}"

    # 1b) 合并标记 link[type=replaced-by] 中的引用同样改写，
    #     否则去标识化后会留下指向真实 id 的悬空引用
    links = out.get("link")
    if isinstance(links, list):
        for link in links:
            if not isinstance(link, dict):
                continue
            other = link.get("other")
            if isinstance(other, dict) and isinstance(other.get("reference"), str):
                t, i = _split_reference(other["reference"])
                other["reference"] = f"{t}/{mapper.pseudonym_for(t, i)}"

    # 2) 类型专属的直接标识符处理
    if resource_type == "Patient":
        out.pop("name", None)  # 姓名属于直接标识符，移除
        if isinstance(out.get("birthDate"), str) and len(out["birthDate"]) >= 4:
            out["birthDate"] = out["birthDate"][:4]  # 出生日期泛化到出生年
        if isinstance(out.get("identifier"), list):
            new_ids = []
            for ident in out["identifier"]:
                if not isinstance(ident, dict):
                    continue
                value = ident.get("value")
                if isinstance(value, str) and value:
                    ident = dict(ident)
                    # 真实证件号 -> 同一稳定伪标识（跨资源可对齐）
                    ident["value"] = mapper.pseudonym(
                        f"identifier/{value}"
                    )
                new_ids.append(ident)
            out["identifier"] = new_ids or [
                {"system": "urn:pseudo", "value": out["id"]}
            ]

    meta = dict(out.get("meta") or {})
    meta["_deidentified"] = True
    out["meta"] = meta
    return out


__all__ = ["PseudonymMapper", "deidentify_resource"]
