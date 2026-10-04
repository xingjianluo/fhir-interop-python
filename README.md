# FHIR 风格本地医疗数据接入流程（bootstrap-b）

在本地用 FHIR 风格资源（`Patient` / `Encounter` / `Observation`）模拟患者、
就诊与检验结果的接入，提供**解析归一化、版本化合入、乐观锁冲突处理、
患者合并、跨资源稳定去标识化**，并保证 Python API、HTTP、CLI 三个入口
结论一致、可解释。

## 快速开始

```bash
uv sync
uv run pytest -q                      # 64 个测试
uv run fastapi dev fhir_pipeline/api.py
```

## 模块结构

```
fhir_pipeline/
├── errors.py        # 可区分拒绝原因 RejectReason 与 PipelineError
├── models.py        # 资源/合入结论/历史版本类型
├── normalizer.py    # JSON 解析与归一化（字段/取值/引用格式校验）
├── validator.py     # 跨资源引用存在性校验
├── store.py         # 线程安全版本化存储：幂等去重 + 乐观锁 + 历史 + 患者合并
├── deidentify.py    # HMAC 稳定伪标识与引用改写（幂等）
├── service.py       # 三入口共用的编排服务（含输入/判定日志）
├── api.py           # FastAPI 入口
└── cli.py           # 命令行入口
examples/            # 患者/就诊/检验结果样例载荷
docs/                # 接入协议 / 患者合并 / 去标识化规则 / 验证方法
```

## 文档

- [接入协议](docs/INGEST_PROTOCOL.md)
- [患者合并](docs/MERGE.md)
- [去标识化规则](docs/DEIDENTIFICATION.md)
- [验证方法](docs/VERIFICATION.md)
