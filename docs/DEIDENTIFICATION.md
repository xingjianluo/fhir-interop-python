# 去标识化规则

去标识化由 `fhir_pipeline/deidentify.py` 实现，目标：**同一真实标识在不同
资源中映射为稳定一致的伪标识，去标识化后引用仍可解析，重复执行幂等**。

## 1. 伪标识生成（稳定且一致）

```
pseudoid = "PSEUDO-" + HMAC_SHA256(盐, "资源类型/真实id")[0:16]
```

- 盐由 `IngestionService(pseudonym_salt=...)` / `PseudonymMapper(salt=...)` 提供，默认 `local-dev-salt`；
- 输入包含资源类型，因此不同类型的相同 id 不会碰撞；
- 同盐、同真实标识，无论在哪个入口、调用多少次、映射在哪种资源里出现，结果恒等；
- 盐不同则伪标识不同（测试 `test_different_salts_produce_different_pseudonyms`）。
- 映射表 `mapper.snapshot()` 保存 `Patient/P1 -> PSEUDO-xxxx`，可供审计与跨批对齐。

## 2. 字段改写规则

| 位置 | 规则 |
| --- | --- |
| 资源自身 `id` | 替换为该类型+id 的伪标识 |
| `subject.reference`、`encounter.reference` | 拆分出 `Type/id`，用**同一张映射表**改写，保持 `Type/伪id` 形态 |
| `Patient.name` | 直接标识符，移除 |
| `Patient.birthDate` | 精确日期泛化为出生年份（`1990-03-04 -> 1990`） |
| `Patient.identifier[].value`（证件号等） | 以 `identifier/<真实值>` 为键映射为伪标识；同一证件号跨次/跨资源稳定 |
| `meta._deidentified` | 置为 `true` 作为标记 |

## 3. 引用仍可解析的原因

就诊、检验结果中的 `Patient/P1` 与患者自身的 `id=P1` 使用的是同一函数
`mapper.pseudonym_for("Patient", "P1")`，因此两边同时变成同一个
`PSEUDO-xxxx`；`Encounter/E1` 与其被 Observation 引用的方式同理。
去标识化后的资源集合内部引用闭合：

```
Patient/PSEUDO-a  <-  Encounter.subject = Patient/PSEUDO-a
Encounter/PSEUDO-b <- Observation.encounter = Encounter/PSEUDO-b
```

## 4. 幂等性（重复执行安全）

- 已是 `PSEUDO-` 前缀的 id 再次入映时直接原样返回，**不新增映射条目**；
- 对去标识化结果再次执行 `deidentify_all()`：资源集合逐字段相等、映射表键集合不变
  （测试 `test_repeated_run_is_idempotent_and_keeps_references`）；
- 不会出现“二次去标识化产生新 id、把引用指向悬空目标”的情况。

## 5. 使用方式

```python
svc.ingest(patient); svc.ingest(encounter); svc.ingest(observation)
out = svc.deidentify_all()
out["mapping"]    # {"Patient/P1": "PSEUDO-...", ...}
out["resources"]  # 去标识化后的当前版本资源
```

HTTP：`POST /deidentify`；CLI：`python -m fhir_pipeline.cli deidentify`。
