"""FastAPI HTTP 入口。

路由与 Python 入口共用同一个 IngestionService，结论与拒绝原因一致：
所有 RejectionError 经统一异常处理器映射为带 ``reason`` 的错误体。

- POST /fhir/{resource_type}            创建（体内含 id）
- PUT  /fhir/{resource_type}/{id}      更新（If-Match: W/"<v>" 乐观锁）
- GET  /fhir/{resource_type}/{id}      读取当前版本
- GET  /fhir/{resource_type}/{id}/_history  历史版本
- POST /fhir/$deidentify                对全部资源去标识化，返回伪资源 Bundle
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .errors import RejectReason, RejectionError
from .service import IngestionService

logger = logging.getLogger("fhir_pipe")

# 拒绝原因 -> HTTP 状态码
_STATUS = {
    RejectReason.MALFORMED_JSON: 400,
    RejectReason.MISSING_FIELD: 422,
    RejectReason.BAD_VALUE: 422,
    RejectReason.UNKNOWN_RESOURCE_TYPE: 422,
    RejectReason.BROKEN_REFERENCE: 422,
    RejectReason.DUPLICATE_ID: 409,
    RejectReason.VERSION_CONFLICT: 409,
    RejectReason.NOT_FOUND: 404,
}

app = FastAPI(title="local-fhir-interop", version="0.1.0")
service = IngestionService()


@app.exception_handler(RejectionError)
async def _on_rejection(_: Request, exc: RejectionError) -> JSONResponse:
    logger.info("HTTP 拒绝: %s", exc.to_dict())
    return JSONResponse(
        status_code=_STATUS.get(exc.reason, 400),
        content={"error": exc.to_dict()},
    )


async def _read_json(request: Request) -> Any:
    try:
        return await request.json()
    except Exception:  # noqa: BLE001 - 任何 JSON 解析失败归一化
        raise RejectionError(
            RejectReason.MALFORMED_JSON, "请求体不是合法 JSON"
        ) from None


def _check_path(payload: Any, resource_type: str, resource_id: str | None) -> None:
    if not isinstance(payload, dict):
        return  # 交由 parser 报 MALFORMED_JSON
    body_type = payload.get("resourceType")
    if body_type is not None and body_type != resource_type:
        raise RejectionError(
            RejectReason.BAD_VALUE,
            f"路径资源类型 {resource_type} 与体内 resourceType={body_type} 不一致",
            field="resourceType",
        )
    if resource_id is not None:
        body_id = payload.get("id")
        if body_id is not None and body_id != resource_id:
            raise RejectionError(
                RejectReason.BAD_VALUE,
                f"路径 id={resource_id} 与体内 id={body_id} 不一致",
                field="id",
            )


@app.post("/fhir/$deidentify")
async def deidentify_all() -> dict[str, Any]:
    resources = service.deidentify_all()
    return {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [r.to_dict() for r in resources],
    }


@app.post("/fhir/{resource_type}")
async def create_resource(resource_type: str, request: Request) -> dict[str, Any]:
    payload = await _read_json(request)
    _check_path(payload, resource_type, None)
    result = service.ingest(payload, expected_version=None)
    return result.to_dict()


@app.put("/fhir/{resource_type}/{resource_id}")
async def update_resource(
    resource_type: str, resource_id: str, request: Request
) -> dict[str, Any]:
    payload = await _read_json(request)
    _check_path(payload, resource_type, resource_id)
    version = _parse_if_match(request.headers.get("if-match", ""))
    result = service.ingest(payload, expected_version=version)
    return result.to_dict()


@app.get("/fhir/{resource_type}/{resource_id}")
async def read_resource(resource_type: str, resource_id: str) -> dict[str, Any]:
    return service.get(resource_type, resource_id).to_dict()


@app.get("/fhir/{resource_type}/{resource_id}/_history")
async def read_history(resource_type: str, resource_id: str) -> dict[str, Any]:
    versions = service.history(resource_type, resource_id)
    return {
        "resourceType": "Bundle",
        "type": "history",
        "entry": [v.to_dict() for v in versions],
    }


def _parse_if_match(header: str) -> int:
    # 接受 W/"3"、"3" 或裸 3；缺失/非法按 0 处理（必然版本冲突或未找到）
    raw = header.strip()
    if raw.startswith("W/"):
        raw = raw[2:].strip()
    raw = raw.strip('"')
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0
