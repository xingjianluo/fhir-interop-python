# 接入协议（FHIR 风格本地医疗数据）

本服务在本地模拟一条 FHIR 风格的数据接入链路，支持三类资源：

| 资源 | resourceType | 必填字段 | 跨资源引用 |
| --- | --- | --- | --- |
| 患者 | `Patient` | `id`, `gender`(male/female/other/unknown), `birthDate`(YYYY-MM-DD) | — |
| 就诊 | `Encounter` | `id`, `status`, `subject` | `subject -> Patient/{id}`（必填） |
| 检验结果 | `Observation` | `id`, `status`, `code`(coding[].code 或 text), `valueQuantity`/`valueString`, `subject` | `subject -> Patient/{id}`（必填），`encounter -> Encounter/{id}`（可选） |

> 任何非法数据都不会被静默丢弃，而是以一个**可区分的拒绝原因**明确拒绝。

## 1. 合入流程

```
原始载荷(JSON 文本/字节/字典)
  -> parse_payload   解析，坏 JSON / 非对象直接拒绝
  -> normalize       结构、必填、取值、引用格式校验，输出规范化字典
  -> validate_references   引用目标必须已在库中存在（按依赖顺序接入）
  -> store.create / store.update
```

- **幂等键**：默认取规范化内容的 SHA-256；也可用 `Idempotency-Key` 请求头显式提供。
- **内容哈希**：对“键排序 + 紧凑分隔符”的确定性 JSON 求 SHA-256，键顺序不同的相同内容视为同一载荷。

## 2. 新建 / 更新 / 重复 / 冲突

| 场景 | 结论 | HTTP | 说明 |
| --- | --- | --- | --- |
| id 不存在，首次提交 | `created`（v1） | 200 | 追加历史版本 |
| 同 id、内容完全相同再次提交 | `duplicate` | 200 | 不产生新记录/新版本，返回库内版本 |
| 已存在的 id 走新建 | `version_conflict` | 409 | 必须改走更新 |
| 更新携带的 `If-Match` 与当前版本一致、内容变化 | `updated`（v+1） | 200 | 追加不可变历史 |
| 更新携带的 `If-Match` 落后于当前版本 | `version_conflict` | 409 | 明确拒绝，防止丢失更新 |
| 内容有变化但不携带期望版本 | `version_conflict` | 409 | 任何入口都不做无版本覆盖 |
| 更新/引用已并入他记录的患者 | `merged_resource` | 409 | `context.mergedInto` 指向保留记录，见[患者合并](MERGE.md) |
| 拿已作废（被并掉）的患者当合并保留记录 | `survivor_merged` | 409 | `context.survivorMergedInto` 指向当前有效记录，无副作用 |
| 同一幂等键绑定到别的资源/内容 | `version_conflict` | 409 | 重试误用，拒绝 |

所有读改写在同一把锁内原子完成；并发更新中只有一个成功，其余全部得到
`version_conflict`（见 `tests/test_store.py::test_concurrent_updates_exactly_one_wins`）。

## 3. 拒绝原因（reason，机器可判定）

| reason | 含义 | HTTP |
| --- | --- | --- |
| `malformed_json` | 载荷不是合法 JSON | 400 |
| `not_an_object` | 顶层不是 JSON 对象 | 400 |
| `unsupported_resource_type` | resourceType 不在支持列表 | 422 |
| `missing_field` | 必填字段缺失（或 Observation 无结果值） | 422 |
| `invalid_value` | 取值不满足约束（枚举/日期/数值/id 字符集） | 422 |
| `reference_bad_format` | 引用不是 `Type/id` 或引用类型不允许 | 422 |
| `reference_not_found` | 引用目标在库中不存在 | 422 |
| `resource_not_found` | 读取/更新的资源不存在 | 404 |
| `version_conflict` | 版本冲突 / 幂等键冲突 | 409 |
| `merged_resource` | 目标患者已并入其他记录，禁止写入/引用 | 409 |
| `survivor_merged` | 指定的合并保留记录本身已作废（被并掉） | 409 |
| `duplicate_submission` | 重复提交（随 `duplicate` 结论返回，非错误） | 200 |

## 4. HTTP 接口

```
POST /fhir/{type}                       新建（Idempotency-Key 可选）
POST /fhir/{type}/{id}                  指定 id 新建
PUT  /fhir/{type}/{id}                  更新（If-Match: W/<version> 或 <version>）
GET  /fhir/{type}/{id}                  当前版本
GET  /fhir/{type}/{id}/_history         历史版本链
POST /fhir/Patient/{id}/$merge          患者合并（双版本乐观锁，见 MERGE.md）
POST /deidentify                        对库内当前资源执行幂等去标识化
```

成功响应（IngestResult）：

```json
{
  "status": "updated",
  "resourceType": "Observation",
  "id": "O1",
  "version": 2,
  "versionId": "2",
  "payloadHash": "2ddba835cdb3...",
  "reason": "",
  "detail": "更新成功：v1 -> v2（客户端基于 v1）",
  "existingVersion": 1
}
```

冲突响应：

```json
{
  "status": "rejected",
  "reason": "version_conflict",
  "detail": "版本冲突：客户端基于 v1 更新，库内当前为 v2，拒绝以防丢失更新",
  "context": {"expectedVersion": 1, "currentVersion": 2, "...": "..."}
}
```

## 5. 命令行

```bash
uv run python -m fhir_pipeline.cli ingest -f examples/patient.json
uv run python -m fhir_pipeline.cli ingest -f examples/observation.json --if-match 2 --key REQ-42
uv run python -m fhir_pipeline.cli history Observation O1
uv run python -m fhir_pipeline.cli deidentify
```

拒绝时进程退出码为 `1`，stderr 输出含 `reason/detail/context` 的 JSON。

## 6. Python API

```python
from fhir_pipeline.service import IngestionService
from fhir_pipeline.errors import PipelineError, RejectReason

svc = IngestionService()
svc.ingest(patient_dict)
try:
    svc.ingest(new_observation, expected_version=1)
except PipelineError as exc:
    assert exc.reason is RejectReason.VERSION_CONFLICT
```

三个入口（HTTP / CLI / Python API）调用同一个 `IngestionService`，结论与拒绝原因完全一致。
