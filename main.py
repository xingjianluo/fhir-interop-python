"""本地启动入口：uvicorn main:app。"""

from fhir_pipe.api import app

__all__ = ["app"]
