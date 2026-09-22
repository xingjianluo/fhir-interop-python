"""让测试日志同时打印输入与判定依据（caplog 在 WARNING 级别即可见）。"""

import logging

import pytest


@pytest.fixture(autouse=True)
def _capture_decision_logs(caplog):
    caplog.set_level(logging.INFO, logger="fhir_pipeline.service")
    yield
