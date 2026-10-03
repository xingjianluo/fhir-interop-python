"""FastAPI HTTP 入口，与 Python API/CLI 共用同一 IngestionService。

路由：
- POST /fhir/{resource_type}          新建（可选 Idempotency-Key）
- PUT  /fhir/{resource_type}/{rid}    更新（If-Match: W/<version> 乐观锁）
- POST /fhir/{resource_type}/{rid}    新建指定 id（与 POST 集合语义一致）
- GET  /fhir/{resource_type}/{rid}    读取当前版本
- GET  /fhir/{resource_type}/{rid}/_history  历史版本链
- POST /fhir/Patient/$merge         患者合并（双方版本必填，乐观并发）
- POST /deidentify                    对库内当前资源执行幂等去标识化
"""

from __future__ import annotations

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from .errors import PipelineError, RejectReason
from .service import IngestionService

app = FastAPI(title="FHIR 本地接入流程", version="0.1.0")
service = IngestionService()


def reset_service() -> None:
    """重置全局服务（测试隔离使用）。"""
    global service
    service = IngestionService()

# 拒绝原因 -> HTTP 状态码，保证不同失败原因在协议层也可区分
_STATUS_MAP = {
    RejectReason.MALFORMED_JSON: 400,
    RejectReason.NOT_AN_OBJECT: 400,
    RejectReason.MISSING_FIELD: 422,
    RejectReason.INVALID_VALUE: 422,
    RejectReason.UNSUPPORTED_RESOURCE_TYPE: 422,
    RejectReason.REFERENCE_BAD_FORMAT: 422,
    RejectReason.REFERENCE_NOT_FOUND: 422,
    RejectReason.RESOURCE_NOT_FOUND: 404,
    RejectReason.VERSION_CONFLICT: 409,
    RejectReason.MERGE_VERSION_CONFLICT: 409,
    RejectReason.PATIENT_ALREADY_MERGED: 409,
    RejectReason.DUPLICATE_SUBMISSION: 200,
}


@app.exception_handler(PipelineError)
async def pipeline_error_handler(request: Request, exc: PipelineError) -> JSONResponse:
    status_code = _STATUS_MAP.get(exc.reason, 400)
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "rejected",
            "reason": exc.reason.value,
            "detail": exc.message,
            "context": exc.context,
        },
    )


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "fhir-local-ingest", "status": "ok"}


# 注意：具体路径必须在 /fhir/{resource_type}/{resource_id} 这类参数化
# 路由之前注册，否则 $merge 会被当成 resource_id 匹配进合入流程。
@app.post("/fhir/Patient/$merge")
async def merge_patients(
    request: Request,
    idempotency_key: str | None = Header(default=None),
) -> dict:
    """把两条重复患者合并为一条。

    体：{"survivingPatientId", "mergedPatientId",
         "expectedSurvivingVersion", "expectedMergedVersion"}。
    版本字段必填（发起人当时看到的版本），用于并发冲突判定。
    """
    raw = (await request.body()).decode("utf-8")
    return _handle_merge_request(raw, idempotency_key=idempotency_key)


@app.post("/fhir/{resource_type}")
async def create_in_collection(
    resource_type: str,
    request: Request,
    idempotency_key: str | None = Header(default=None),
) -> dict:
    raw = (await request.body()).decode("utf-8")
    return _ingest_with_type_check(
        raw, resource_type, idempotency_key=idempotency_key
    )


@app.post("/fhir/{resource_type}/{resource_id}")
async def create_named(
    resource_type: str,
    resource_id: str,
    request: Request,
    idempotency_key: str | None = Header(default=None),
) -> dict:
    raw = (await request.body()).decode("utf-8")
    return _ingest_with_type_check(
        raw, resource_type, resource_id=resource_id,
        idempotency_key=idempotency_key,
    )


@app.put("/fhir/{resource_type}/{resource_id}")
async def update_named(
    resource_type: str,
    resource_id: str,
    request: Request,
    if_match: str | None = Header(default=None),
    idempotency_key: str | None = Header(default=None),
) -> dict:
    raw = (await request.body()).decode("utf-8")
    expected = _parse_if_match(if_match)
    return _ingest_with_type_check(
        raw, resource_type, resource_id=resource_id,
        expected_version=expected, idempotency_key=idempotency_key,
    )


@app.get("/fhir/{resource_type}/{resource_id}")
def read_resource(resource_type: str, resource_id: str) -> dict:
    resource = service.get(resource_type, resource_id)
    if resource is None:
        raise PipelineError(
            RejectReason.RESOURCE_NOT_FOUND,
            f"资源不存在: {resource_type}/{resource_id}",
            context={"resourceType": resource_type, "id": resource_id},
        )
    return {
        "resourceType": resource.resource_type,
        "id": resource.resource_id,
        "versionId": resource.version_id,
        "payloadHash": resource.payload_hash,
        "resource": resource.body,
    }


@app.get("/fhir/{resource_type}/{resource_id}/_history")
def read_history(resource_type: str, resource_id: str) -> dict:
    history = service.get_history(resource_type, resource_id)
    return {
        "resourceType": history.resource_type,
        "id": history.resource_id,
        "currentVersion": history.current_version,
        "versions": [
            {
                "version": e.version,
                "versionId": e.version_id,
                "payloadHash": e.payload_hash,
                "resource": e.body,
            }
            for e in history.entries
        ],
    }


@app.post("/deidentify")
def deidentify_all() -> dict:
    return service.deidentify_all()


# ---------- 辅助 ----------

def _handle_merge_request(raw: str, *, idempotency_key: str | None) -> dict:
    """解析并校验合并请求体，调用同一 IngestionService 完成合并。"""
    import json

    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise PipelineError(
            RejectReason.MALFORMED_JSON, "合并请求体不是合法 JSON",
            context={"snippet": raw[:80]},
        )
    if not isinstance(payload, dict):
        raise PipelineError(
            RejectReason.NOT_AN_OBJECT, "合并请求体必须是 JSON 对象",
        )

    surviving_id = payload.get("survivingPatientId")
    merged_id = payload.get("mergedPatientId")
    sv = payload.get("expectedSurvivingVersion")
    mv = payload.get("expectedMergedVersion")
    for field_name, value in (
        ("survivingPatientId", surviving_id),
        ("mergedPatientId", merged_id),
        ("expectedSurvivingVersion", sv),
        ("expectedMergedVersion", mv),
    ):
        if value is None:
            raise PipelineError(
                RejectReason.MISSING_FIELD,
                f"合并请求缺少必填字段 {field_name}",
                context={"field": field_name},
            )
    for field_name, value in (
        ("survivingPatientId", surviving_id),
        ("mergedPatientId", merged_id),
    ):
        if not isinstance(value, str) or not value:
            raise PipelineError(
                RejectReason.INVALID_VALUE,
                f"{field_name} 必须是非空字符串",
                context={"field": field_name, "value": value},
            )
    for field_name, value in (
        ("expectedSurvivingVersion", sv),
        ("expectedMergedVersion", mv),
    ):
        if isinstance(value, bool) or not isinstance(value, int):
            raise PipelineError(
                RejectReason.INVALID_VALUE,
                f"{field_name} 必须是整数版本号",
                context={"field": field_name, "value": value},
            )

    result = service.merge_patients(
        surviving_patient_id=surviving_id,
        merged_patient_id=merged_id,
        expected_surviving_version=sv,
        expected_merged_version=mv,
        idempotency_key=idempotency_key,
    )
    return result.to_dict()


def _ingest_with_type_check(
    raw: str,
    path_type: str,
    *,
    resource_id: str | None = None,
    expected_version: int | None = None,
    idempotency_key: str | None = None,
) -> dict:
    import json

    from .errors import RejectReason as R

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = raw  # 交给 normalizer 给出 malformed_json
    if isinstance(payload, dict):
        if "resourceType" in payload and payload["resourceType"] != path_type:
            raise PipelineError(
                R.INVALID_VALUE,
                f"路径资源类型 {path_type} 与载荷 resourceType "
                f"{payload['resourceType']} 不一致",
                context={"path": path_type, "body": payload["resourceType"]},
            )
        payload["resourceType"] = path_type
        if resource_id is not None:
            payload["id"] = resource_id
    result = service.ingest(
        payload if isinstance(payload, dict) else raw,
        expected_version=expected_version,
        idempotency_key=idempotency_key,
    )
    return result.to_dict()


def _parse_if_match(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if text.startswith("W/"):
        text = text[2:].strip()
    text = text.strip('"')
    try:
        return int(text)
    except ValueError as exc:
        raise PipelineError(
            RejectReason.INVALID_VALUE,
            f"If-Match 必须是版本号或 W/<version>，实际: {value!r}",
            context={"header": "If-Match", "value": value},
        ) from exc
