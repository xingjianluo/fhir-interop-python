# 本地 FHIR 风格医疗数据接入流程

一个纯本地运行的最小实现，模拟患者（Patient）、就诊（Encounter）、
检验结果（Observation）三类 FHIR 风格资源的**合入、去标识化与版本
冲突处理**。无论从 Python API 还是 HTTP 入口进入，都经过同一个
`IngestionService`，因此结论一致、拒绝原因可区分、过程可解释。

## 1. 架构

```
payload(JSON)
   │
   ▼
parser.parse_resource      解析 + 字段/约束/引用格式校验（不静默丢弃）
   │
   ▼
IngestionService           引用存在性校验（BROKEN_REFERENCE）
   │
   ▼
VersionedStore             乐观锁 create/update + 历史版本（线程安全）
   │
   ▼（可选）
deidentify                 跨资源稳定伪标识 + 引用重写（幂等视图）
```

| 模块 | 职责 |
| --- | --- |
| `fhir_pipe/errors.py` | 拒绝原因枚举 `RejectReason` 与 `RejectionError` |
| `fhir_pipe/models.py` | 归一化资源 dataclass（Patient/Encounter/Observation） |
| `fhir_pipe/parser.py` | JSON 解析、必填与约束校验、归一化 |
| `fhir_pipe/store.py` | 内存版本化存储：乐观锁、幂等重放、历史追溯 |
| `fhir_pipe/deidentify.py` | 稳定伪标识表、跨资源引用重写、直接标识符处理 |
| `fhir_pipe/service.py` | 统一门面，所有入口的唯一业务路径 + 判定日志 |
| `fhir_pipe/api.py` | FastAPI HTTP 入口与统一错误响应 |

## 2. 接入协议

### 2.1 资源模型（最小字段集）

- **Patient**：`id`、`name`、`gender`（`male/female/other/unknown`）、
  `birthDate`（`YYYY-MM-DD`）。
- **Encounter**：`id`、`subject → Patient/<id>`、
  `status`（`planned/arrived/in-progress/finished/cancelled`）、
  `period.start`、可选 `period.end`（不得早于 start）。
- **Observation**：`id`、`status`
  （`registered/preliminary/final/amended/cancelled`）、
  `code.coding[0].code`、`subject → Patient/<id>`、
  可选 `encounter → Encounter/<id>`、
  `valueQuantity.value`（数值，布尔值被拒绝）、`valueQuantity.unit`、
  `effectiveDateTime`。

`id` 形如 `^[A-Za-z0-9][A-Za-z0-9.\-]{0,63}$`。

### 2.2 HTTP 入口

| 方法与路径 | 语义 |
| --- | --- |
| `POST /fhir/{type}` | 创建（体内含 `id`）。ID 已存在 → `409 duplicate_id` |
| `PUT /fhir/{type}/{id}` | 更新，必须带头 `If-Match: W/"<version>"` |
| `GET /fhir/{type}/{id}` | 读取当前版本 |
| `GET /fhir/{type}/{id}/_history` | 历史版本列表（Bundle） |
| `POST /fhir/$deidentify` | 返回全部资源的去标识化 Bundle |

**乐观锁规则**

- 更新基于的版本与当前版本不一致 → `409 version_conflict`，
  更新被明确拒绝，旧版本不会覆盖新版本（无丢失更新）。
- 版本一致但**内容相同**：判为重复提交（幂等重放），返回当前版本、
  `meta.ingest.replayed=true`，**不产生新版本、不产生重复记录**。
- 缺少/非法 `If-Match` 按版本 0 处理，必然冲突或未找到，杜绝裸 PUT 覆盖。
- 更新不存在的资源 → `404 not_found`，与版本冲突可区分。

**合入顺序**：引用目标必须先存在。请按
`Patient → Encounter → Observation` 的顺序合入；引用目标缺失时
返回 `422 broken_reference`，数据不会落库，也不会被静默丢弃。

### 2.3 拒绝原因（全部可区分）

| reason | HTTP | 触发场景 |
| --- | --- | --- |
| `malformed_json` | 400 | 载荷不是 JSON 对象 / 请求体非法 JSON |
| `missing_field` | 422 | 必填字段缺失或为空 |
| `bad_value` | 422 | 类型/枚举/日期/范围/ID/引用格式不合法、路径与体内不一致 |
| `unknown_resource_type` | 422 | `resourceType` 不在支持集合内 |
| `broken_reference` | 422 | 引用目标不存在或未先合入 |
| `duplicate_id` | 409 | 创建语义下 ID 已存在 |
| `version_conflict` | 409 | If-Match 版本过期（并发更新） |
| `not_found` | 404 | 读取/更新的资源不存在 |

错误响应统一为：

```json
{"error": {"reason": "version_conflict",
           "message": "[version_conflict] 提交基于 v1，当前已是 v2 …",
           "field": null}}
```

### 2.4 Python 入口（与 HTTP 完全等价）

```python
from fhir_pipe.service import IngestionService
from fhir_pipe.errors import RejectReason, RejectionError

svc = IngestionService(salt="local-static-salt")
svc.ingest(patient)                       # 创建
svc.ingest(patient_v2, expected_version=1)  # 更新/重放/冲突
svc.history("Patient", "P1")              # 历史版本
svc.deidentify_all()                      # 去标识化视图
```

## 3. 去标识化规则

- **全域稳定映射**：真实逻辑 ID `Type/<real>` 经
  `SHA-256(salt | Type/real)` 截断 12 位生成伪 ID，并带类型前缀
  （`pid-` / `enc-` / `obs-`）。同一真实 Patient 在
  Patient.id、Encounter.subject、Observation.subject 中**始终是同一
  个伪 ID**；映射保存在 `PseudonymTable` 中。
- **引用重写**：资源自身 ID 与所有 `subject`/`encounter` 引用一并
  重写，去标识化后的资源之间引用仍可解析。
- **直接标识符处理**：`Patient.name` 移除；`birthDate` 泛化到年份
  （`1980-05-12 → 1980-01-01`），降低精确生日重识别风险。
- **幂等**：重复执行去标识化返回完全相同的伪 ID，不新增映射；
  对已是去标识化视图的资源再次执行是 no-op，不破坏既有引用关系。
- **视图语义**：去标识化结果是派生视图，存储中的原始资源不变，
  仍可用真实 ID 读取。
- salt 决定伪 ID 域：同 salt 可复现，不同 salt 互不可关联。

## 4. 验证方法

```bash
uv sync --group dev        # 安装依赖（含 pytest）
uv run pytest              # 全部 57 个用例，日志实时打印输入与判定依据
uv run pytest -k conflict  # 只看版本冲突相关
uv run uvicorn main:app    # 启动 HTTP 服务（http://127.0.0.1:8000/docs）
```

测试覆盖：

- `tests/test_parser.py`：合法归一化 + 20+ 种坏数据的可区分拒绝；
- `tests/test_store.py`：重复提交幂等、过期版本冲突、历史追溯、
  并发更新恰好一方成功（无丢失更新）；
- `tests/test_deidentify.py`：跨资源一致映射、引用可解析、重复执行
  不产生新映射、直接标识符移除/泛化；
- `tests/test_service.py`：合入顺序、断引用、重复提交、版本冲突与
  not_found 可区分、历史、去标识化一致性；
- `tests/test_api.py`：HTTP 与 Python 入口结论一致、状态码/reason
  可区分、幂等重放、去标识化 Bundle。

pytest 已开启 `log_cli`，每个用例都会打印**输入载荷**与**判定依据
（create/update/idempotent-replay/具体拒绝原因）**。

### 手动 curl 示例

```bash
curl -s -X POST localhost:8000/fhir/Patient -H 'content-type: application/json' \
  -d '{"resourceType":"Patient","id":"P1","name":"张三","gender":"male","birthDate":"1980-05-12"}'
curl -s -X PUT  localhost:8000/fhir/Patient/P1 -H 'If-Match: W/"1"' \
  -H 'content-type: application/json' \
  -d '{"resourceType":"Patient","id":"P1","name":"张三丰","gender":"male","birthDate":"1980-05-12"}'
curl -s localhost:8000/fhir/Patient/P1/_history
curl -s -X POST localhost:8000/fhir/\$deidentify
```

## 5. 范围说明

存储为进程内内存实现（线程安全），重启不保留；聚焦本地接入流程的
一致性与可解释性。生产化时可将 `VersionedStore`/`PseudonymTable`
替换为数据库支持的实现，协议与拒绝原因保持不变。
