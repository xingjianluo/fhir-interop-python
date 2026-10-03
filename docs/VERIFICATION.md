# 验证方法

## 1. 环境准备

```bash
uv sync
```

## 2. 运行全部单测

```bash
uv run pytest -q
```

预期：`58 passed`。测试文件：

| 文件 | 覆盖点 |
| --- | --- |
| `tests/test_normalizer.py` | 坏 JSON、非对象、字段缺失、非法取值、引用格式错误——10 类拒绝原因参数化覆盖 |
| `tests/test_store.py` | 重复提交不产生新记录、版本冲突拒绝且数据不回退、10 线程并发仅一个更新成功、历史版本链可追溯、幂等键误用冲突 |
| `tests/test_deidentify.py` | 跨资源同一真实标识一致、去标识化后引用闭合可解析、重复执行映射不增引用不乱、直接标识符移除/泛化 |
| `tests/test_service.py` | 三资源端到端合入、Duplicate 与 Conflict 的可区分结论与上下文、并发无丢失更新、去标识化二次执行稳定 |
| `tests/test_api.py` | HTTP 状态码映射（400/409/422/404）、`If-Match` 乐观锁、`Idempotency-Key` 头、历史接口、去标识化接口 |
| `tests/test_merge.py` | 患者合并：引用改指、墓碑与双侧历史、重复提交幂等、双方版本落后可区分、并发合并仅一成、合并与更新/新建并发不丢数据不悬空、墓碑引用拒绝、合并后去标识化闭合且稳定 |
| `tests/test_entrypoints_merge.py` | 合并在 Python/HTTP/CLI 三入口成功/幂等/冲突/墓碑引用四类结论一致；CLI 退出码与 stderr |

## 3. 查看测试中的输入与判定依据日志

服务在 INFO 级别记录每次请求的**输入摘要**和**判定结论/拒绝理由**：

```bash
uv run pytest tests/test_service.py -o log_cli=true --log-cli-level=INFO
```

典型输出：

```
[reqid] 收到合入请求 expected_version=1 idem=None 输入={"resourceType":"Observation", ...}
[reqid] 归一化通过 type=Observation id=O1 hash=2ddba835cdb3
[reqid] 判定结论 status=updated 更新成功：v1 -> v2（客户端基于 v1）
[reqid] 拒绝合入 reason=version_conflict 依据=版本冲突：客户端基于 v1 更新，库内当前为 v2 ...
```

合并请求同理，日志含双方 id 与版本、改指数量与判定依据：

```
[reqid] 收到合并请求 survivor=Patient/P1@v1 merged=Patient/P2@v1 idem=None
[reqid] 判定结论 status=merged 改指资源数=2 合并成功：Patient/P2 标记为墓碑并指向 Patient/P1 ...
[reqid] 判定结论 status=duplicate 改指资源数=2 重复提交：Patient/P2 已并入 Patient/P1（墓碑 v2）...
[reqid] 拒绝合并 reason=merge_version_conflict 依据=合并版本冲突：保留记录 在合并发起后已被修改 ...
```

测试内部也通过 `caplog` 断言日志确实携带输入与“重复提交/版本冲突”等判定依据
（见 `tests/test_service.py` 中的 `assert any(... for r in caplog.records)`）。

## 4. 手工验证：HTTP

```bash
uv run fastapi dev fhir_pipeline/api.py
# 新建患者
curl -s localhost:8000/fhir/Patient -H 'Content-Type: application/json' \
  -d @examples/patient.json
# 重复提交（status=duplicate，不新增版本）
curl -s localhost:8000/fhir/Patient -H 'Content-Type: application/json' \
  -d @examples/patient.json
# 基于 v1 更新
curl -s -X PUT localhost:8000/fhir/Patient/P1 -H 'If-Match: W/1' \
  -H 'Content-Type: application/json' \
  -d '{"gender":"male","birthDate":"1990-03-04"}'
# 再次基于 v1 -> 409 version_conflict
curl -i -X PUT localhost:8000/fhir/Patient/P1 -H 'If-Match: 1' \
  -H 'Content-Type: application/json' \
  -d '{"gender":"other","birthDate":"1990-03-04"}'
# 历史版本
curl -s localhost:8000/fhir/Patient/P1/_history
# 去标识化
curl -s -X POST localhost:8000/deidentify
```

患者合并（先准备两条患者，再调用 `$merge`）：

```bash
curl -s localhost:8000/fhir/Patient -H 'Content-Type: application/json' \
  -d '{"id":"P1","gender":"female","birthDate":"1990-03-04"}'
curl -s localhost:8000/fhir/Patient -H 'Content-Type: application/json' \
  -d '{"id":"P2","gender":"male","birthDate":"1985-07-08"}'
# 合并 P2 -> P1（携带双方当前版本）
curl -s -X POST localhost:8000/fhir/Patient/$merge \
  -H 'Content-Type: application/json' \
  -d '{"survivingPatientId":"P1","mergedPatientId":"P2",
       "expectedSurvivingVersion":1,"expectedMergedVersion":1}'
# 同体重放 -> status=duplicate，不二次改写
# 合并后再引用 P2 -> 409 patient_already_merged，context.replacementId=P1
curl -i -X POST localhost:8000/fhir/Encounter \
  -H 'Content-Type: application/json' \
  -d '{"id":"E9","status":"arrived","subject":{"reference":"Patient/P2"}}'
# 墓碑历史：v1 合并前原状，v2 active=false + link replaced
curl -s localhost:8000/fhir/Patient/P2/_history
```

## 5. 手工验证：CLI

```bash
uv run python -m fhir_pipeline.cli ingest -f examples/patient.json -v
echo '{"id":"X"}' | uv run python -m fhir_pipeline.cli ingest -f -   # exit=1, reason=missing_field
```

## 6. 关键性质速查

- **不静默丢弃**：任何拒绝都有 `RejectReason` + `detail` + `context`；
- **不丢更新**：更新必须带期望版本；并发下只有一个 `updated`，其余 `version_conflict`；
- **不重复**：相同内容/幂等键的重发得到 `duplicate`，版本数与记录数不变；
- **可追溯**：`GET .../_history` 或 `service.get_history(...)` 返回每个不可变版本及哈希；
- **去标识化一致**：同一真实标识跨资源伪标识相同、引用闭合、二次执行无变化。
- **合并不悬空/不丢数据**：合并与合入共用同一临界区，引用原子改指；版本落后
  可区分地拒绝（`merge_version_conflict`，标注保留方/被合并方及当前版本）；
  合并后对墓碑的新引用/更新得到 `patient_already_merged` 并告知保留记录；
  同一合并重放恒为 `duplicate`。
