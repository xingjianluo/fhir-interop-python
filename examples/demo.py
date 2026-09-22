"""端到端演示：合入 -> 重复提交 -> 版本冲突 -> 历史 -> 去标识化。

运行：uv run python examples/demo.py
"""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fhir_pipeline.errors import PipelineError
from fhir_pipeline.service import IngestionService

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s %(name)s: %(message)s")

svc = IngestionService()

patient = {"resourceType": "Patient", "id": "P1", "gender": "female",
           "birthDate": "1990-03-04",
           "identifier": [{"system": "id-card", "value": "ID-001"}]}
encounter = {"resourceType": "Encounter", "id": "E1", "status": "finished",
             "subject": {"reference": "Patient/P1"}}
observation = {"resourceType": "Observation", "id": "O1", "status": "final",
               "code": {"coding": [{"code": "2345-7", "text": "Glucose"}]},
               "subject": {"reference": "Patient/P1"},
               "encounter": {"reference": "Encounter/E1"},
               "valueQuantity": {"value": 5.4, "unit": "mmol/L"}}

print("== 正常合入（按依赖顺序：患者 -> 就诊 -> 检验） ==")
for payload in (patient, encounter, observation):
    print(svc.ingest(payload).to_dict()["status"])

print("== 先提交检验而患者不存在 ==")
try:
    svc.ingest({**observation, "id": "O2",
                "subject": {"reference": "Patient/NOPE"}})
except PipelineError as exc:
    print("被拒绝:", exc.reason.value, "-", exc.message)

print("== 重复提交检验结果 ==")
print(svc.ingest(observation).to_dict())

print("== 基于 v1 更新到 v2，再用过期 v1 提交（冲突） ==")
v2 = {**observation, "valueQuantity": {"value": 6.0, "unit": "mmol/L"}}
print(svc.ingest(v2, expected_version=1).to_dict()["status"])
try:
    v3 = {**observation, "valueQuantity": {"value": 7.0, "unit": "mmol/L"}}
    svc.ingest(v3, expected_version=1)
except PipelineError as exc:
    print("被拒绝:", exc.reason.value, "-", exc.message)

print("== 历史版本 ==")
history = svc.get_history("Observation", "O1")
print([(e.version, e.body["valueQuantity"]["value"]) for e in history.entries])

print("== 去标识化（执行两次验证幂等） ==")
out1 = svc.deidentify_all()
out2 = svc.deidentify_all()
print("两次结果一致:", out1 == out2)
print(json.dumps(out1["resources"], ensure_ascii=False, indent=2))
print("映射:", json.dumps(out1["mapping"], ensure_ascii=False, indent=2))
