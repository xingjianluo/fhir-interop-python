"""测试日志与报告。

- caplog 固定到 INFO：服务打印的输入摘要/判定依据在测试中可被断言；
- -v 运行时，每条用例通过后追加其 docstring 首行（业务覆盖点），
  让测试日志能直接看出每条用例验证的业务与通过情况。
"""

import logging

import pytest


@pytest.fixture(autouse=True)
def _capture_decision_logs(caplog):
    caplog.set_level(logging.INFO, logger="fhir_pipeline.service")
    yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        doc_lines = (item.function.__doc__ or "").strip().splitlines()
        report._business_doc = doc_lines[0].strip() if doc_lines else ""


def pytest_runtest_logreport(report):
    if report.when != "call" or not report.passed:
        return
    if not getattr(report, "_business_doc", ""):
        return
    config = pytest._verbose_config
    if config is None or config.getoption("verbose") <= 0:
        return
    reporter = config.pluginmanager.getplugin("terminalreporter")
    reporter.write_line(f"        业务：{report._business_doc}")


def pytest_configure(config):
    pytest._verbose_config = config
