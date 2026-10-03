# 患者合并（Patient Merge）

同一位真实患者被不同来源录成两条 `Patient` 时，可用合并能力把**被合并记录**
并入**保留记录**：引用整体改指、被合并记录保留为可追溯的墓碑，全过程带乐观
版本核对、原子执行、幂等，且与历史版本和去标识化兼容。

## 1. 合并规则

给定保留患者 `surviving` 与被合并患者 `merged`（二者必须都存在且不同）：

1. **引用改指**：所有当前版本中 `subject = Patient/merged` 的
   `Encounter` / `Observation`，其 `subject` 改为 `Patient/surviving`，
   逐条 **版本号 +1 并追加历史**，其余字段原样保留。合并完成后不存在任何
   指向被合并 id 的悬空引用。
2. **墓碑保留，不删除**：被合并患者追加一个新版本，置 `active=false`，并加
   `link: [{other: {reference: "Patient/surviving"}, type: "replaced"}]`。
   记录仍可 `GET`、仍有完整历史，一眼能看出它被并到了哪里。
3. **保留方回链**：保留患者追加一个新版本，加
   `link: [{other: {reference: "Patient/merged"}, type: "replaces"}]`，
   保持 `active=true`。
4. **幂等**：同一对 `(surviving, merged)` 的合并请求重复提交（即使携带的是
   最初看到的旧版本号）一律返回 `duplicate`，不二次改写、不新增版本、不重复
   改指；返回体里的 `repointed` 清单与首次一致。
5. **整段原子**：版本核对、墓碑写入、引用改指、台账落地在同一把锁内完成，
   与普通合入共用该临界区，因此合并与「更新被合并患者 / 往其名下挂新就诊、
   检验结果」不可能互相穿插（见并发语义）。

## 2. 乐观版本核对与并发语义

发起合并**必须**同时携带发起人当时看到的两条记录版本：
`expectedSurvivingVersion` 与 `expectedMergedVersion`。任何入口都不允许
不核对版本就合并。

- 任一方在合并发起后已被改动 -> 拒绝，reason 为
  **`merge_version_conflict`**（HTTP 409）。`context` 分别给出两侧的
  `expectedVersion / currentVersion / stale`，可区分是**保留记录落后**还是
  **被合并记录落后**、当前版本各是多少。
- 两个**不同方向**的并发合并（`P1<-P2` 与 `P2<-P1`）只有一个成功，另一个
  得到 `merge_version_conflict` 或 `patient_already_merged`。
- 同一对患者的**并发重复**合并：恰好一次真实合并，其余请求在锁内看到台账后
  返回 `duplicate`。
- 合并与「更新被合并患者」并发：恰好一方成功。更新赢则数据为更新者写入、
  引用不变；合并赢则迟到更新被 **`patient_already_merged`** 拒绝，更新内容
  不会被悄悄带进墓碑（墓碑判定先于版本判定，结论可区分、可处置）。
- 合并与「新建挂在被合并患者名下的资源」并发：若新建先提交，则被合并一并
  改指；若合并先完成，则新建以 `patient_already_merged` 被拒。不存在合并后
  仍指向被合并 id 的资源。

## 3. 合并完成后的引用处置

- 新建/更新资源若 `subject` 指向已被合并的患者 -> 拒绝，reason 为
  **`patient_already_merged`**（HTTP 409），`context.replacementId` 告知应
  改指的保留记录；数据不会被默默写进去。改指保留记录后即可正常写入。
- 直接更新墓碑患者本身（即使携带墓碑当前版本号）同样以
  `patient_already_merged` 拒绝；应改为操作保留记录。
- 已被合并的患者不能再次作为被合并目标；已是墓碑的患者也不能作为保留目标。

## 4. 与历史版本、去标识化的兼容范围

- **历史可追溯**：两位患者合并前后的版本链都保留。墓碑的上一版本仍是合并前
  原状；被改指的就诊/检验结果，其历史链上能看到引用从旧 id 变为保留 id。
  `GET /fhir/{type}/{id}/_history` 对两条患者与所有被改指资源均可用。
- **去标识化闭合**：去标识化对普通引用（`subject`/`encounter`）与患者
  `link`（`replaced`/`replaces`）使用同一张稳定伪标识映射一并改写，因此
  去标识化后墓碑链接、保留方回链与资源引用全部仍可解析、无悬空指针。
- **去标识化幂等**：合并后重复执行去标识化，映射条目不增加、资源结果逐字段
  相等（与既有去标识化幂等承诺一致）。
- 去标识化针对**当前版本快照**；历史版本仍以原始形态可查，不在去标识化改写
  范围内（与合并前行为一致）。

## 5. 三个入口

### Python

```python
svc.merge_patients(
    surviving_patient_id="P1",
    merged_patient_id="P2",
    expected_surviving_version=1,   # 必填：发起人看到的保留患者版本
    expected_merged_version=1,      # 必填：发起人看到的被合并患者版本
    idempotency_key="MERGE-REQ-1",  # 可选
)
# -> MergeResult(status="merged", survivingVersion=2, mergedVersion=2,
#                repointed=[RepointedResource("Encounter","E2",2), ...])
```

### HTTP

```
POST /fhir/Patient/$merge
Content-Type: application/json
Idempotency-Key: MERGE-REQ-1        # 可选

{
  "survivingPatientId": "P1",
  "mergedPatientId": "P2",
  "expectedSurvivingVersion": 1,
  "expectedMergedVersion": 1
}
```

> `$merge` 是具体路由，注册在 `/fhir/{type}/{id}` 之前，不会被当成资源 id。

成功 `200`：

```json
{
  "status": "merged",
  "survivingPatientId": "P1",
  "mergedPatientId": "P2",
  "survivingVersion": 2,
  "mergedVersion": 2,
  "repointed": [{"resourceType": "Encounter", "id": "E2", "version": 2}],
  "payloadHash": "e7e5b147...",
  "reason": "",
  "detail": "合并成功：Patient/P2 标记为墓碑并指向 Patient/P1；改指资源 1 条；..."
}
```

重复提交 `200` 且 `status="duplicate"`；版本冲突 / 已合并 `409`，体同
通用拒绝结构（`status/reason/detail/context`）。

### CLI

```bash
uv run python -m fhir_pipeline.cli merge \
  --survivor P1 --merged P2 \
  --survivor-version 1 --merged-version 2 \
  [--key MERGE-REQ-1]
```

成功/幂等退出码 `0`；任何拒绝退出码 `1`，stderr 输出含
`reason/detail/context` 的 JSON。

## 6. 新增拒绝原因

| reason | 含义 | HTTP |
| --- | --- | --- |
| `merge_version_conflict` | 合并基于的某一方版本已落后，`context` 标明哪一侧、期望/当前版本 | 409 |
| `patient_already_merged` | 引用/更新指向了已合并患者，或对墓碑再合并；`context.replacementId` 给出保留记录 | 409 |

## 7. 本地验证

```bash
uv run pytest -q                                   # 全量（含合并用例）
uv run pytest tests/test_merge.py -q              # 合并：改指/墓碑/幂等/并发/去标识化
uv run pytest tests/test_entrypoints_merge.py -q  # 三入口结论一致
uv run pytest -q -k merge -o log_cli=true          # 查看输入内容与判定依据日志
```

关键用例：

- `test_merge_repoints_resources_and_marks_tombstone`
- `test_repeated_merge_is_idempotent_no_double_rewrite`
- `test_stale_survivor_version_rejected_with_distinguishable_sides`
- `test_stale_merged_patient_version_rejected_with_current_version`
- `test_concurrent_identical_merge_requests_only_one_merge`
- `test_two_concurrent_distinct_merges_only_one_wins`
- `test_merge_concurrent_with_update_to_merged_patient_no_lost_data`
- `test_merge_concurrent_with_new_encounter_no_dangling_reference`
- `test_new_reference_to_merged_patient_is_rejected`
- `test_deid_after_merge_references_closed_and_stable`
- `test_successful_merge_consistent_across_entrypoints`

日志在 INFO 级别打印每次合并的输入（双方 id 与版本）与判定依据
（`merged` / `duplicate` / `merge_version_conflict` /
`patient_already_merged` 及期望与当前版本）。
