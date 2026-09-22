"""跨资源去标识化。

规则：
- 真实标识 -> 稳定伪标识是“全域”映射：同一个真实 ID（如 Patient/P1）
  在 Patient.id、Encounter.subject、Observation.subject 中始终映射为
  同一个伪 ID，不同资源类型带各自类型前缀（Patient/pid-<12 位>）。
- 映射保存在 :class:`PseudonymTable`；同一真实标识重复执行返回既有
  伪 ID，绝不产生第二个映射（幂等）。
- 重写资源 ID 与所有引用，使去标识化后的资源之间引用仍可解析。
- 直接身份字段处理：Patient.name 置为 None（输出中省略）；
  birthDate 泛化到年份（YYYY-01-01），避免精确生日重识别。
- 已经是去标识化视图的资源重复执行是幂等 no-op，不产生新映射、
  不破坏既有引用关系。

为何已去标识化资源能做到 no-op：解析归一化时伪资源携带的
``meta.pseudoId`` 会被保留；去标识化函数据此直接返回深拷贝。
"""

from __future__ import annotations

import copy
import hashlib
import threading

from .models import (
    Encounter,
    Observation,
    Patient,
    Reference,
    Resource,
    ResourceType,
)

# 各资源类型的伪 ID 前缀，便于人眼区分
_PREFIX = {
    ResourceType.PATIENT.value: "pid",
    ResourceType.ENCOUNTER.value: "enc",
    ResourceType.OBSERVATION.value: "obs",
}


class PseudonymTable:
    """真实 ID -> 伪 ID 的稳定双向映射表（线程安全）。"""

    def __init__(self, *, salt: str = "local-static-salt") -> None:
        self._salt = salt
        self._lock = threading.RLock()
        # (type, real_id) -> pseudo_id
        self._forward: dict[tuple[str, str], str] = {}
        # pseudo_id -> (type, real_id)
        self._reverse: dict[str, tuple[str, str]] = {}

    @staticmethod
    def _digest(salt: str, resource_type: str, real_id: str) -> str:
        h = hashlib.sha256(f"{salt}|{resource_type}/{real_id}".encode("utf-8"))
        return h.hexdigest()[:12]

    def pseudonym(self, resource_type: str, real_id: str) -> str:
        """返回稳定伪 ID；首次见到的真实标识建立映射，之后复用。"""
        key = (resource_type, real_id)
        with self._lock:
            existing = self._forward.get(key)
            if existing is not None:
                return existing
            prefix = _PREFIX.get(resource_type, "res")
            # 极端情况下处理哈希碰撞：变换后缀直到不冲突
            token = self._digest(self._salt, resource_type, real_id)
            pseudo = f"{prefix}-{token}"
            while pseudo in self._reverse:
                token = hashlib.sha256(token.encode()).hexdigest()[:12]
                pseudo = f"{prefix}-{token}"
            self._forward[key] = pseudo
            self._reverse[pseudo] = key
            return pseudo

    def is_mapped(self, resource_type: str, real_id: str) -> bool:
        with self._lock:
            return (resource_type, real_id) in self._forward

    def mappings(self) -> dict[str, str]:
        """``{真实逻辑ID: 伪逻辑ID}`` 的快照，供检查/导出。"""
        with self._lock:
            return {
                f"{rtype}/{rid}": pseudo
                for (rtype, rid), pseudo in self._forward.items()
            }


def deidentify_resource(
    resource: Resource, table: PseudonymTable
) -> Resource:
    """返回资源的去标识化深拷贝（含引用重写）。幂等。"""
    out = copy.deepcopy(resource)

    # 已是去标识化视图：幂等 no-op
    if out.meta.deidentified and out.meta.pseudo_id is not None:
        return out

    # 引用中的 ID 可能是真实 ID，也可能（跨次调用时）已经是伪 ID。
    # 若能反查到真实 ID 则用真实 ID 再映射（保证稳定）；否则直接映射。
    def resolve_real_id(rtype: str, rid: str) -> str:
        rev = table._reverse.get(rid)  # noqa: SLF001 - 内部协作
        if rev is not None and rev[0] == rtype:
            return rev[1]
        return rid

    def pseudo(rtype: str, rid: str) -> str:
        return table.pseudonym(rtype, resolve_real_id(rtype, rid))

    rtype = out.resource_type
    new_pid = table.pseudonym(rtype, out.id)
    out.id = new_pid

    if isinstance(out, Patient):
        out.name = ""  # 直接标识符移除（to_dict 时省略空名）
        out.birth_date = f"{out.birth_date[:4]}-01-01"  # 泛化到年份
    elif isinstance(out, Encounter):
        out.subject = Reference(
            f"{out.subject.resource_type}/"
            f"{pseudo(out.subject.resource_type, out.subject.resource_id)}"
        )
    elif isinstance(out, Observation):
        out.subject = Reference(
            f"{out.subject.resource_type}/"
            f"{pseudo(out.subject.resource_type, out.subject.resource_id)}"
        )
        if out.encounter is not None:
            out.encounter = Reference(
                f"{out.encounter.resource_type}/"
                f"{pseudo(out.encounter.resource_type, out.encounter.resource_id)}"
            )

    out.meta.deidentified = True
    out.meta.pseudo_id = new_pid
    return out
