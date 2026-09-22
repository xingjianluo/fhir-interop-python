"""归一化后的 FHIR 风格资源模型。

只保留本流程需要的最小字段集合，但保留 FHIR 的核心语义：
- resourceType / id / meta.versionId / meta.lastUpdated
- 资源间引用使用 "ResourceType/id" 形式的逻辑 ID（非完整 URL）

这些 dataclass 同时承担归一化结果与存储记录两个角色。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class ResourceType(str, Enum):
    PATIENT = "Patient"
    ENCOUNTER = "Encounter"
    OBSERVATION = "Observation"


@dataclass(frozen=True)
class Reference:
    """FHIR 风格的资源间引用，如 ``Patient/P1``。"""

    ref: str

    @property
    def resource_type(self) -> str:
        return self.ref.partition("/")[0]

    @property
    def resource_id(self) -> str:
        return self.ref.partition("/")[2]

    def __str__(self) -> str:  # pragma: no cover - 简单委托
        return self.ref


@dataclass
class Meta:
    version_id: int = 1
    last_updated: float = field(default_factory=lambda: time.time())
    deidentified: bool = False
    # 去标识化后该资源对应的稳定伪 ID（原始资源为 None）
    pseudo_id: str | None = None

    def to_dict(self) -> dict:
        d = {
            "versionId": str(self.version_id),
            "lastUpdated": self.last_updated,
            "deidentified": self.deidentified,
        }
        if self.pseudo_id is not None:
            d["pseudoId"] = self.pseudo_id
        return d


@dataclass
class Patient:
    id: str
    name: str
    gender: str  # male | female | other | unknown
    birth_date: str  # YYYY-MM-DD
    meta: Meta = field(default_factory=Meta)
    resource_type: str = ResourceType.PATIENT.value

    def to_dict(self) -> dict:
        d = {
            "resourceType": "Patient",
            "id": self.id,
            "gender": self.gender,
            "birthDate": self.birth_date,
            "meta": self.meta.to_dict(),
        }
        if self.name:
            d["name"] = self.name
        return d


@dataclass
class Encounter:
    id: str
    subject: Reference  # -> Patient
    status: str  # planned|arrived|in-progress|finished|cancelled
    period_start: str  # YYYY-MM-DD
    period_end: str | None
    meta: Meta = field(default_factory=Meta)
    resource_type: str = ResourceType.ENCOUNTER.value

    def to_dict(self) -> dict:
        return {
            "resourceType": "Encounter",
            "id": self.id,
            "subject": {"reference": self.subject.ref},
            "status": self.status,
            "period": {"start": self.period_start, "end": self.period_end},
            "meta": self.meta.to_dict(),
        }


@dataclass
class Observation:
    id: str
    status: str  # registered|preliminary|final|amended|cancelled
    code: str  # LOINC 风格检验代码，如 4548-4 (HbA1c)
    subject: Reference  # -> Patient
    encounter: Reference | None  # -> Encounter
    value: float
    unit: str
    effective: str  # YYYY-MM-DD
    meta: Meta = field(default_factory=Meta)
    resource_type: str = ResourceType.OBSERVATION.value

    def to_dict(self) -> dict:
        return {
            "resourceType": "Observation",
            "id": self.id,
            "status": self.status,
            "code": {"coding": [{"code": self.code}]},
            "subject": {"reference": self.subject.ref},
            "encounter": (
                {"reference": self.encounter.ref} if self.encounter else None
            ),
            "valueQuantity": {"value": self.value, "unit": self.unit},
            "effectiveDateTime": self.effective,
            "meta": self.meta.to_dict(),
        }


# 归一化资源联合类型
Resource = Patient | Encounter | Observation
