"""store 单测：创建/更新、乐观锁冲突、幂等重放、历史追溯、并发。"""

import logging
import threading

import pytest

from fhir_pipe.errors import RejectReason
from fhir_pipe.parser import parse_resource
from fhir_pipe.store import VersionedStore

from conftest import assert_rejected, patient

log = logging.getLogger("fhir_pipe.test")


@pytest.fixture
def store_with_patient() -> tuple[VersionedStore, dict]:
    s = VersionedStore()
    data = patient()
    s.create(parse_resource(data))
    return s, data


def test_create_then_get(store_with_patient) -> None:
    s, data = store_with_patient
    log.info("test_create 输入=%r", data)
    got = s.require("Patient", "P1")
    assert got.meta.version_id == 1 and got.name == "张三"
    log.info("test_create 判定=当前版本 v1")


def test_duplicate_create_rejected(
    caplog: pytest.LogCaptureFixture, store_with_patient
) -> None:
    s, data = store_with_patient
    assert_rejected(
        caplog, lambda: s.create(parse_resource(data)),
        RejectReason.DUPLICATE_ID, label="重复创建 P1", payload=data,
    )


def test_update_bumps_version_and_keeps_history(store_with_patient) -> None:
    s, _ = store_with_patient
    changed = patient(name="张三丰")
    log.info("test_update 输入=%r 基于 v1", changed)
    r = s.update(parse_resource(changed), expected_version=1)
    assert r.version_id == 2 and not r.replayed
    hist = s.history("Patient", "P1")
    assert [h.meta.version_id for h in hist] == [1, 2]
    assert hist[0].name == "张三" and hist[1].name == "张三丰"
    log.info("test_update 判定=升版 v2，历史保留 v1/v2，无丢失更新")


def test_duplicate_submit_is_idempotent(store_with_patient) -> None:
    """重复提交：同版本同内容不产生新版本、不报错。"""
    s, data = store_with_patient
    log.info("test_idempotent 输入=%r 再次基于 v1 提交", data)
    r1 = s.update(parse_resource(data), expected_version=1)
    r2 = s.update(parse_resource(data), expected_version=1)
    assert r1.replayed and r2.replayed
    assert [h.meta.version_id for h in s.history("Patient", "P1")] == [1]
    log.info("test_idempotent 判定=两次均 replayed，版本仍为 v1，无重复记录")


def test_stale_version_conflict(
    caplog: pytest.LogCaptureFixture, store_with_patient
) -> None:
    s, _ = store_with_patient
    s.update(parse_resource(patient(name="张三丰")), expected_version=1)
    stale = patient(name="过期更新")
    assert_rejected(
        caplog, lambda: s.update(parse_resource(stale), expected_version=1),
        RejectReason.VERSION_CONFLICT,
        label="基于 v1 更新已是 v2 的资源", payload=stale,
    )
    # 被拒绝后数据不变
    assert s.require("Patient", "P1").name == "张三丰"
    assert [h.meta.version_id for h in s.history("Patient", "P1")] == [1, 2]


def test_update_missing_is_not_found(
    caplog: pytest.LogCaptureFixture,
) -> None:
    s = VersionedStore()
    data = patient("NOPE")
    assert_rejected(
        caplog, lambda: s.update(parse_resource(data), expected_version=1),
        RejectReason.NOT_FOUND, label="更新不存在资源", payload=data,
    )


def test_concurrent_updates_exactly_one_wins(store_with_patient) -> None:
    """并发：两个线程都基于同一旧版本更新，恰一个成功，另一个冲突，无丢失更新。"""
    s, _ = store_with_patient
    a = patient(name="并发甲")
    b = patient(name="并发乙", gender="female")
    outcomes: dict[str, tuple] = {}
    barrier = threading.Barrier(2)

    def worker(tag: str, payload: dict) -> None:
        barrier.wait()
        try:
            r = s.update(parse_resource(payload), expected_version=1)
            outcomes[tag] = ("ok", r.version_id, payload["name"])
        except Exception as exc:  # noqa: BLE001
            outcomes[tag] = ("reject", type(exc).__name__, str(exc))

    t1 = threading.Thread(target=worker, args=("a", a))
    t2 = threading.Thread(target=worker, args=("b", b))
    log.info("test_concurrent 输入=两个线程均基于 v1: %r / %r", a, b)
    t1.start(); t2.start(); t1.join(); t2.join()

    oks = [v for v in outcomes.values() if v[0] == "ok"]
    rejects = [v for v in outcomes.values() if v[0] == "reject"]
    assert len(oks) == 1 and len(rejects) == 1
    assert s.require("Patient", "P1").meta.version_id == 2
    assert s.require("Patient", "P1").name == oks[0][2]
    assert [h.meta.version_id for h in s.history("Patient", "P1")] == [1, 2]
    log.info("test_concurrent 判定=%s 胜出、%s 冲突拒绝，最终 v2，无覆盖丢失",
             oks[0][2], [v[1] for v in rejects][0])


def test_get_missing_returns_none_and_history_404(
    caplog: pytest.LogCaptureFixture,
) -> None:
    s = VersionedStore()
    assert s.get("Patient", "X") is None
    assert_rejected(
        caplog, lambda: s.history("Patient", "X"),
        RejectReason.NOT_FOUND, label="读取不存在历史", payload="Patient/X",
    )
