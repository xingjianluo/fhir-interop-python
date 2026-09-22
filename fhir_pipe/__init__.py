"""FHIR 风格医疗数据本地接入流程。

模块划分：
- errors: 可区分的拒绝原因与异常
- models: 归一化资源与公共数据结构
- parser: FHIR JSON 解析与校验
- store: 版本化存储（乐观锁、历史追溯）
- deidentify: 跨资源去标识化（稳定伪标识）
- service: 合入/更新/去标识化 统一入口
- api: HTTP 入口
"""

__all__ = [
    "errors",
    "models",
    "parser",
    "store",
    "deidentify",
    "service",
    "api",
]
