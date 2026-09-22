"""兼容入口：uvicorn/fastapi 可直接指向 main:app。"""

from fhir_pipeline.api import app

__all__ = ["app"]
