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

### 哪些记录能当保留记录

保留记录必须是**当前仍然有效**的患者：

- 记录存在（不存在 -> 404 `resource_not_found`）；
- 记录本身**没有**被并掉：一旦某患者带 `active=false` 与
  `link[type=replaced-by]` 标记，它就已经作废，不能再作为任何合并的保留记录。

拿已作废的患者当保留记录时，合并在任何写入之前即被拒绝，返回专门的、可与
版本冲突区分的原因 `survivor_merged`（HTTP 409），`context` 给出去向与双方
当前版本：

```json
{
  "status": "rejected",
  "reason": "survivor_merged",
  "detail": "保留记录 Patient/P2 已并入 Patient/P1（已作废），不能再作为合并的保留记录；请以当前有效记录 Patient/P1 作为保留记录重试",
  "context": {
    "survivorId": "P2",
    "survivorMergedInto": "P1",
    "survivorCurrentVersion": 2,
    "mergedId": "P3",
    "mergedCurrentVersion": 1
  }
}
```

此拒绝不产生任何副作用：被合并方及其名下就诊/检验结果不会被改指到作废记录，
版本与历史均保持原样。链式并人时，应以链头的当前有效记录（上例中的
`Patient/P1`）作为保留记录。

## 2. 版本核对（并发安全）

合并请求必须携带发起人当时看到的**两条记录各自的版本**：

| 场景 | 结论 |
| --- | --- |
| 两版本都与库内一致 | 执行合并 |
| 保留记录已作废（被并掉） | 409 `survivor_merged`，`context.survivorMergedInto` 给当前有效记录，附双方版本 |
| 保留记录版本落后 | 409 `version_conflict`，`context.staleResource="survivor"`，含双方当前版本 |
| 被合并记录版本落后 | 409 `version_conflict`，`context.staleResource="merged"`，含双方当前版本 |
| 被合并记录已并入他处 | 409 `version_conflict`，`context.mergedInto` 给出去向 |
| 缺少任一版本 | 422 `missing_field`（HTTP）/ 参数必填（CLI），禁止无版本合并 |

并发保证（合并全程持有同一把存储锁，读-判-写原子）：

- 两个并发合并**只有一个成功**，另一个得到 `version_conflict`
  （或按重复提交幂等返回）；拒绝时库里的数据不会被改一半。
- 合并与患者更新同时发生：
  - 更新先落在被合并患者上 -> 合并方看到版本落后（`staleResource="merged"`），
    合并被拒，更新内容保留；
  - 更新先落在保留患者上 -> 合并方看到版本落后（`staleResource="survivor"`），
    合并被拒，更新内容保留；
  - 合并先完成 -> 再更新被合并患者得到 `merged_resource`；拿着旧版本更新
    保留患者得到 `version_conflict`，按当前版本重试即可。
  两种抢跑都不会互相覆盖，拒绝时双方当前版本都会随结论报出。
- **合并期间有人往被合并患者下挂新就诊/检验结果**（重点收口）：
  合入入口在存储锁内、真正落库前会再复核一次引用（`write_guard`），
  “引用校验 + 写入”与合并不可能交错。因此只有两种结果：
  1. 新写入先提交 -> 合并的引用改指会把它**一并扫到并改指**（版本 +1），
     合并成功后库里不存在任何指向被合并 id 的资源；
  2. 合并先提交 -> 新写入在锁内复核处被 **`merged_resource` 明确拒绝**，
     不会悄悄写进去，也不会覆盖别人刚写的内容，响应里附
     `context.mergedInto` 指明应改指的保留记录。
- 一般更新同样收紧：内容有变化的更新必须携带期望版本（`If-Match`），
  任何入口都不做无版本覆盖；内容一致的重复提交仍幂等返回 `duplicate`。

## 3. 幂等重复提交

- 合并键缺省为 `merge:{mergedId}->{survivorId}`，也可用幂等键显式指定；
- 同一合并键重复提交：返回首次结论（`status=duplicate`，含首次的改指清单），
  **不二次改写、不产生新版本/新数据**；
- 即使换了幂等键，只要被合并记录已并入同一保留记录，同样按重复请求幂等返回。

## 4. 合并后的写入处置

合并完成后再想引用被合并患者，结论是明确且跨入口一致的：

- 更新被合并患者 -> 409 `merged_resource`，`context.mergedInto` 指向保留记录；
- 新建/更新资源时把 `subject` 等引用指向被合并患者 -> 409
  `merged_resource`，不会默默写进去，既有数据也不会被改一半；
- 拿被合并患者当**保留记录**再发起合并 -> 409 `survivor_merged`，
  响应指明当前有效记录，调用方应改以它为保留记录重试；
- 被合并记录本身仍可读取（`GET /fhir/Patient/{mergedId}`）且历史可查，
  其 `active=false` 与 `link[type=replaced-by]` 即“已作废、去向何处”的凭据。

调用方收到上述拒绝后，应把引用改写为 `context.mergedInto` /
`context.survivorMergedInto` 指向的当前有效记录后重试。

## 5. 与历史版本、去标识化的兼容

- **历史**：合并产生的新版本（被改指资源、被合并患者的标记版本）都追加在
  各自历史链上，合并前后的每个版本都可通过 `_history` 追溯。
- **去标识化**：合并标记 `link` 中的引用与 `subject/encounter` 一样用同一张
  映射表改写，去标识化后引用仍然闭合、可解析，不留指向真实 id 的悬空引用；
  重复执行去标识化映射不增、结果不变。被合并记录以伪标识保留在结果集中，
  其 `link` 指向保留记录的伪标识。

## 6. 入口

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

## 7. 本地验证

```bash
# 全部用例（合并 + 更新 + 去标识化等，确认无回归）
uv run pytest -q

# 仅合并用例（25 个）
uv run pytest tests/test_merge.py -q

# 带日志运行：可见每个请求输入了什么、判定依据是什么
uv run pytest tests/test_merge.py -o log_cli=true --log-cli-level=INFO
```

并发交错用例的可复现方式：测试通过“屏障 + 事件”在 `IngestionService`
的引用校验点插桩，把“校验已通过但尚未落库”的请求挂起，等合并完整提交后
再放行（见 `tests/test_merge.py` 中的 `_IngestGate` /
`run_after_gate`）。交错顺序由测试固定，单次运行即稳定复现，
不依赖多跑几轮碰运气。覆盖：

- 已作废患者当保留记录：`survivor_merged` 拒绝、无副作用、三入口一致；
- 保留记录/被合并记录版本落后可区分，拒绝时报出双方当前版本；
- 并发合并只成功一个；合并与双方患者的更新抢跑均有确定结论、互不覆盖；
- 合并与“新挂就诊”竞争：晚到的新建被 `merged_resource` 拒绝、未落库；
  早到的新建被合并一并改指；两种交错后均无悬空引用；
- 合并后去标识化引用闭合且幂等。
