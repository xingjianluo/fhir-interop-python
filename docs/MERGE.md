# 患者合并规则

同一个人被不同来源录成两条患者记录时，可以把**被合并记录**并入**保留记录**，
同时保证引用、版本、历史与去标识化自洽。合并由 `ResourceStore.merge_patients`
在同一把锁内原子完成，三个入口（Python / HTTP / CLI）共用
`IngestionService.merge_patients`，结论一致。

## 1. 合并语义

指定 `survivorId`（保留）与 `mergedId`（被合并）后，一次合并原子地完成：

1. **引用改指**：库内所有指向 `Patient/{mergedId}` 的引用
   （Encounter.subject、Observation.subject 等）改指 `Patient/{survivorId}`；
   每条被改指的资源版本 +1 并追加历史，绝不留下指向被合并 id 的悬空引用。
2. **被合并记录不删除**：版本 +1，body 置 `active=false` 并追加
   `link=[{"other": {"reference": "Patient/{survivorId}"}, "type": "replaced-by"}]`，
   状态与去向在资源本体和历史中都可见。
3. **保留记录本身不改写**（其版本不变，仅作为引用目标）。

## 2. 谁能当保留记录

保留记录（`survivorId`）必须是一条**当前仍有效（未被作废）的 Patient**：

- 已被合并过的患者（`active=false` 且带 `link[type=replaced-by]`）**不得**
  再作为保留记录。即使请求里带的版本号恰好是它作废后的当前版本也一样拒绝，
  防止把新的引用改指到一条死链上。
- 拒绝结论：409 `merged_resource`，`context.role="survivor"`，
  `context.mergedInto` 指明它已并入哪条有效记录、`context.survivorId`
  指明被误用的 id，并在 `detail` 里提示应以去向记录为保留记录重新发起。
- 该拒绝发生在任何改写之前：被合并方与库内其他资源都**不会被改一半**。
- 链式合并是允许的：`P2 并入 P1` 之后，只要 `P1` 仍有效，可以继续
  `P3 并入 P1`；不能做的是把别人再并到已作废的 `P2`。

## 3. 版本核对（并发安全）

合并请求必须携带发起人当时看到的**两条记录各自的版本**：

| 场景 | 结论 |
| --- | --- |
| 两版本都与库内一致 | 执行合并 |
| 保留记录已作废（被合并过） | 409 `merged_resource`，`context.role="survivor"`，`mergedInto` 给出去向 |
| 保留记录版本落后 | 409 `version_conflict`，`context.staleResource="survivor"`，含双方当前版本 |
| 被合并记录版本落后 | 409 `version_conflict`，`context.staleResource="merged"`，含双方当前版本 |
| 被合并记录已并入他处 | 409 `version_conflict`，`context.mergedInto` 给出去向 |
| 缺少任一版本 | 422 `missing_field`（HTTP）/ 参数必填（CLI），禁止无版本合并 |

并发保证（check-then-act 全部在同一存储临界区内，可重入）：

- 合并全程持锁，读-判-写原子；两个并发合并**只有一个成功**，另一个得到
  `version_conflict`（或按重复提交幂等返回）。
- 合并与任一方记录上的并发更新：先完成者生效，后来者被明确拒绝
  （更新晚于合并 -> `merged_resource`；更新先于合并 -> 合并方看到版本落后，
  `staleResource` 指明是哪一方落后），双方数据互不覆盖、不留半截改写。
- **合并进行中向被合并患者挂新就诊/检验结果**：引用校验与写入与合并处于
  同一把锁保护下，不存在“校验通过后目标恰好被合并”的时间窗。
  - 新建先进入临界区：合并扫描到它，一并改指到保留记录；
  - 合并先进入临界区：新建在锁外排队，合并提交后它看到目标已作废，
    得到 409 `merged_resource`，**不会悄悄写进去，也不会覆盖别人刚写的内容**。
- 合并成功结束后，库内**不再有任何资源指向被合并 id**（最终不变量）。
- 一般更新同样收紧：内容有变化的更新必须携带期望版本（`If-Match`），
  任何入口都不做无版本覆盖；内容一致的重复提交仍幂等返回 `duplicate`。

## 4. 幂等重复提交

- 合并键缺省为 `merge:{mergedId}->{survivorId}`，也可用幂等键显式指定；
- 同一合并键重复提交：返回首次结论（`status=duplicate`，含首次的改指清单），
  **不二次改写、不产生新版本/新数据**；
- 即使换了幂等键，只要被合并记录已并入同一保留记录，同样按重复请求幂等返回。

## 5. 合并完成后再引用被合并患者

- 新建/更新资源时把 `subject` 等指向被合并患者 -> 409 `merged_resource`，
  `context.mergedInto` 指向保留记录，提示改指；资源不会落库。
- 直接更新被合并患者本身 -> 409 `merged_resource`，请改写保留记录。
- 被合并记录**仍可读取**（`GET /fhir/Patient/{mergedId}`）：
  返回体带 `active=false` 与 `link[type=replaced-by]`，历史 `_history` 可查，
  供审计追溯，但它不再是可写入/可被引用的有效目标。

## 6. 与历史版本、去标识化的兼容

- **历史**：合并产生的新版本（被改指资源、被合并患者的标记版本）都追加在
  各自历史链上，合并前后的每个版本都可通过 `_history` 追溯。
- **去标识化**：合并标记 `link` 中的引用与 `subject/encounter` 一样用同一张
  映射表改写，去标识化后引用仍然闭合、可解析，不留指向真实 id 的悬空引用；
  重复执行去标识化映射不增、结果不变。被合并记录以伪标识保留在结果集中，
  其 `link` 指向保留记录的伪标识。

## 7. 入口

```bash
# HTTP
curl -X POST localhost:8000/fhir/Patient/P1/$merge \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: MERGE-1' \
  -d '{"mergedId": "P2", "expectedSurvivorVersion": 1, "expectedMergedVersion": 1}'

# CLI
uv run python -m fhir_pipeline.cli merge P1 P2 \
  --survivor-version 1 --merged-version 1 --key MERGE-1
```

```python
# Python
svc.merge_patients("P1", "P2",
                   expected_survivor_version=1,
                   expected_merged_version=1,
                   idempotency_key="MERGE-1")
```

成功响应含 `status=merged`、`survivorVersion`、`mergedVersion`（被合并记录的
新版本）与 `repointed`（每条被改指资源的类型/id/起止版本）。

## 8. 本地验证

```bash
uv run pytest tests/test_merge.py -q                 # 23 个合并用例
uv run pytest tests/test_merge.py -o log_cli=true \
  --log-cli-level=INFO                               # 日志可见输入与判定依据
uv run pytest -q                                     # 全量回归（60 passed）
```

覆盖：重复提交幂等、双方版本落后可区分、**作废记录不得当保留记录
（`role=survivor`，三入口一致）**、合法链式合并、并发合并确定性地只成功一个、
合并与更新/新建竞争的两种交错均无悬空引用（通过合并临界区阶段插桩
确定性复现，不靠多跑碰运气）、三入口结论一致、合并后去标识化闭合且幂等。
