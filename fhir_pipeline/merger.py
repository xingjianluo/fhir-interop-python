"""患者合并时的资源改写规则（纯函数，无副作用）。

- 被合并患者 -> 墓碑：active=false，追加 link[type=replaced] 指向保留患者，
  不删除记录（历史链与当前版本都能看出它被并到了哪里）；
- 保留患者 -> 追加 link[type=replaces] 指向被合并患者，并升一个版本；
- 挂在被合并患者名下的 Encounter/Observation -> subject 改指保留患者，
  逐条升版本、追加历史，其余内容原样保留（不覆盖别人刚写入的字段）。

所有输出都经键排序，且用与合入一致的确定性哈希计算 payloadHash。
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

from .normalizer import _sort_keys, compute_payload_hash

# subject 指向被合并患者的资源类型（本流程仅这两类挂患者）
_REPOINTABLE_TYPES = ("Encounter", "Observation")


def _with_link(body: dict[str, Any], *, other_id: str, link_type: str) -> dict[str, Any]:
    """返回追加（或复用）指定 link 的排序后新字典。"""
    out = copy.deepcopy(body)
    link_ref = {"other": {"reference": f"Patient/{other_id}"}, "type": link_type}
    links = list(out.get("link") or [])
    for existing in links:
        if (
            isinstance(existing, dict)
            and existing.get("type") == link_type
            and isinstance(existing.get("other"), dict)
            and existing["other"].get("reference") == f"Patient/{other_id}"
        ):
            break
    else:
        links.append(link_ref)
    out["link"] = links
    return _sort_keys(out)


def build_tombstone(
    merged_body: dict[str, Any], surviving_id: str
) -> tuple[dict[str, Any], str]:
    """被合并患者 -> active=false + link replaced 指向保留患者。"""
    out = _with_link(merged_body, other_id=surviving_id, link_type="replaced")
    out["active"] = False
    out = _sort_keys(out)
    return out, compute_payload_hash(out)


def build_survivor(
    survivor_body: dict[str, Any], merged_id: str
) -> tuple[dict[str, Any], str]:
    """保留患者 -> active=true + link replaces 指向被合并患者。"""
    out = _with_link(survivor_body, other_id=merged_id, link_type="replaces")
    out["active"] = True
    out = _sort_keys(out)
    return out, compute_payload_hash(out)


def make_repointer(
    merged_id: str, surviving_id: str
) -> Callable[[dict[str, Any]], tuple[dict[str, Any], str] | None]:
    """生成改指函数：仅当资源 subject 仍指向被合并患者时改写。

    返回 (新 body, 新哈希)；不指向被合并患者则返回 None，原资源不动。
    """
    old_ref = f"Patient/{merged_id}"
    new_ref = f"Patient/{surviving_id}"

    def repoint(body: dict[str, Any]) -> tuple[dict[str, Any], str] | None:
        if body.get("resourceType") not in _REPOINTABLE_TYPES:
            return None
        subject = body.get("subject")
        if not isinstance(subject, dict) or subject.get("reference") != old_ref:
            return None
        out = copy.deepcopy(body)
        out["subject"] = {"reference": new_ref}
        out = _sort_keys(out)
        return out, compute_payload_hash(out)

    return repoint


__all__ = ["build_survivor", "build_tombstone", "make_repointer"]
