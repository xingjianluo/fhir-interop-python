# 验证方法

## 1. 环境准备

```bash
uv sync
```

## 2. 运行全部单测

```bash
uv run pytest -q
```

预期：`64 passed`。测试文件：

| 文件 | 覆盖点 |
| --- | --- |
| `tests/test_normalizer.py` | 坏 JSON、非对象、字段缺失、非法取值、引用格式错误——10 类拒绝原因参数化覆盖 |
| `tests/test_store.py` | 重复提交不产生新记录、版本冲突拒绝且数据不回退、10 线程并发仅一个更新成功、历史版本链可追溯、幂等键误用冲突 |
| `tests/test_deidentify.py` | 跨资源同一真实标识一致、去标识化后引用闭合可解析、重复执行映射不增引用不乱、直接标识符移除/泛化 |
| `tests/test_service.py` | 三资源端到端合入、Duplicate 与 Conflict 的可区分结论与上下文、并发无丢失更新、去标识化二次执行稳定 |
| `tests/test_api.py` | HTTP 状态码映射（400/409/422/404）、`If-Match` 乐观锁、`Idempotency-Key` 头、历史接口、去标识化接口 |
| `tests/test_merge.py` | 患者合并：引用改指与标记、重复提交幂等、保留记录资格（`survivor_merged` 三入口一致）、双方版本落后可区分且报双版本、并发合并只成功一个、合并与更新/新建的**确定性交错**竞争（无悬空引用、晚到写入被明确拒绝）、合并后去标识化闭合且幂等 |

每条用例的业务含义直接写在其 docstring 里；加 `-v` 运行时，每条用例后会
追加打印该业务说明与通过情况，例如：

```bash
uv run pytest tests/test_merge.py -v
# tests/test_merge.py::test_already_merged_patient_cannot_be_survivor PASSED
#   业务：P2 已并入 P1 后，再拿 P2 当保留记录并 P3：必须以专门原因拒绝 ...
```

并发交错用例不靠重试碰运气：`_IngestGate` 在校验点用事件固定线程顺序，
单次运行即可稳定复现（详见 [患者合并](MERGE.md) 第 7 节）。

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
# 患者合并（P2 并入 P1，双版本乐观锁）
curl -s -X POST localhost:8000/fhir/Patient/P1/\$merge \
  -H 'Content-Type: application/json' \
  -d '{"mergedId":"P2","expectedSurvivorVersion":1,"expectedMergedVersion":1}'
# 去标识化
curl -s -X POST localhost:8000/deidentify
```

## 5. 手工验证：CLI

```bash
uv run python -m fhir_pipeline.cli ingest -f examples/patient.json -v
echo '{"id":"X"}' | uv run python -m fhir_pipeline.cli ingest -f -   # exit=1, reason=missing_field
uv run python -m fhir_pipeline.cli merge P1 P2 --survivor-version 1 --merged-version 1
```

## 6. 关键性质速查

- **不静默丢弃**：任何拒绝都有 `RejectReason` + `detail` + `context`；
- **不丢更新**：更新必须带期望版本；并发下只有一个 `updated`，其余 `version_conflict`；
- **不重复**：相同内容/幂等键的重发得到 `duplicate`，版本数与记录数不变；
- **可追溯**：`GET .../_history` 或 `service.get_history(...)` 返回每个不可变版本及哈希；
- **去标识化一致**：同一真实标识跨资源伪标识相同、引用闭合、二次执行无变化。
