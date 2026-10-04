"""患者合并：幂等重复提交、版本核对、并发竞争、三入口一致、去标识化兼容。

日志（INFO 级）会打印每次合并的输入与判定依据，便于核对结论来源。
"""

import json
import logging
import threading
import time

import pytest
from fastapi.testclient import TestClient

from fhir_pipeline import api, cli
from fhir_pipeline.errors import PipelineError, RejectReason
from fhir_pipeline.service import IngestionService

P1 = {"resourceType": "Patient", "id": "P1",
      "gender": "male", "birthDate": "1980-01-01"}
P2 = {"resourceType": "Patient", "id": "P2",
      "gender": "male", "birthDate": "1980-01-02"}
P3 = {"resourceType": "Patient", "id": "P3",
      "gender": "female", "birthDate": "1981-05-06"}
ENC_ON_P2 = {"resourceType": "Encounter", "id": "E1", "status": "finished",
             "subject": {"reference": "Patient/P2"}}
OBS_ON_P2 = {"resourceType": "Observation", "id": "O1", "status": "final",
             "code": {"text": "glucose"},
             "subject": {"reference": "Patient/P2"},
             "encounter": {"reference": "Encounter/E1"},
             "valueQuantity": {"value": 5.5}}


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    """轮询直到 predicate 为真（仅用于测试线程同步，不参与业务判定）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return False


class _LockWaitTracker:
    """包裹存储写锁，记录当前正阻塞等待该锁的线程 ident（测试确定性交错用）。"""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.waiting: set[int] = set()

    def acquire(self, *args, **kwargs):
        ident = threading.get_ident()
        self.waiting.add(ident)
        try:
            return self._inner.acquire(*args, **kwargs)
        finally:
            # 拿到锁即不再“等待”；拿不到时线程阻塞在此行之前，ident 保留
            self.waiting.discard(ident)

    def release(self) -> None:
        self._inner.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()


def _assert_no_references_to(svc: IngestionService, reference: str) -> None:
    """最终不变量：库内不存在任何 subject/encounter 指向 reference 的资源。"""
    offenders = []
    for resource in svc.store.all_resources():
        for field_name in ("subject", "encounter"):
            ref = resource.body.get(field_name)
            if isinstance(ref, dict) and ref.get("reference") == reference:
                offenders.append((resource.resource_type, resource.resource_id,
                                  field_name))
    assert not offenders, f"发现悬空引用指向 {reference}: {offenders}"



def make_service() -> IngestionService:
    """构造同一初始数据集：P1/P2 两名患者，E1/O1 挂在 P2 下。"""
    svc = IngestionService()
    svc.ingest(P1)
    svc.ingest(P2)
    svc.ingest(ENC_ON_P2)
    svc.ingest(OBS_ON_P2)
    return svc


def merge_kwargs() -> dict:
    return {"expected_survivor_version": 1, "expected_merged_version": 1}


# ---------- 基本语义 ----------

def test_merge_repoints_references_and_marks_merged_record(caplog):
    svc = make_service()
    result = svc.merge_patients("P1", "P2", **merge_kwargs())

    assert result.status.value == "merged"
    assert result.survivor_version == 1
    assert {tuple(sorted(r.items())) for r in result.repointed} == {
        tuple(sorted({"resourceType": "Encounter", "id": "E1",
                      "fromVersion": 1, "toVersion": 2}.items())),
        tuple(sorted({"resourceType": "Observation", "id": "O1",
                      "fromVersion": 1, "toVersion": 2}.items())),
    }

    # 所有引用都改指保留记录，不再有资源指向被合并 id
    for resource in svc.store.all_resources():
        for ref_holder in (resource.body.get("subject"),
                           resource.body.get("encounter")):
            if isinstance(ref_holder, dict):
                assert ref_holder["reference"] != "Patient/P2"
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P1"
    assert svc.get("Observation", "O1").body["subject"]["reference"] == "Patient/P1"

    # 被合并记录未删除：状态可见、指向保留记录、历史可追溯
    merged = svc.get("Patient", "P2")
    assert merged is not None and merged.version == 2
    assert merged.body["active"] is False
    assert merged.body["link"] == [
        {"other": {"reference": "Patient/P1"}, "type": "replaced-by"}
    ]
    history = svc.get_history("Patient", "P2")
    assert [e.version for e in history.entries] == [1, 2]
    assert "link" not in history.entries[0].body  # 合并前版本原样保留
    # 被改指资源的历史同样可追溯
    assert [e.version for e in svc.get_history("Encounter", "E1").entries] == [1, 2]

    # 日志可见输入与判定依据
    texts = [r.getMessage() for r in caplog.records]
    assert any("收到合并请求" in t and "P1" in t and "P2" in t for t in texts)
    assert any("合并结论" in t and "merged" in t for t in texts)


def test_merge_same_patient_rejected():
    svc = make_service()
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "P1", **merge_kwargs())
    assert exc.value.reason is RejectReason.INVALID_VALUE


def test_merge_missing_patient_rejected():
    svc = make_service()
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "NOPE", **merge_kwargs())
    assert exc.value.reason is RejectReason.RESOURCE_NOT_FOUND


# ---------- 幂等重复提交 ----------

def test_merge_duplicate_submission_returns_same_conclusion(caplog):
    svc = make_service()
    first = svc.merge_patients("P1", "P2", **merge_kwargs())
    snapshot = {r.resource_id: r.version for r in svc.store.all_resources()}

    again = svc.merge_patients("P1", "P2", **merge_kwargs())
    assert again.status.value == "duplicate"
    assert again.reason == RejectReason.DUPLICATE_SUBMISSION.value
    # 首次结论原样返回（改指清单一致），且未二次改写任何版本
    assert again.repointed == first.repointed
    assert again.merged_version == first.merged_version
    assert {r.resource_id: r.version for r in svc.store.all_resources()} == snapshot
    assert len(svc.store.all_resources()) == 4

    # 显式幂等键重复提交同样幂等
    svc2 = make_service()
    key = "MERGE-REQ-1"
    a = svc2.merge_patients("P1", "P2", idempotency_key=key, **merge_kwargs())
    b = svc2.merge_patients("P1", "P2", idempotency_key=key, **merge_kwargs())
    assert a.status.value == "merged" and b.status.value == "duplicate"
    assert b.repointed == a.repointed


# ---------- 版本核对 ----------

def test_merge_stale_survivor_version_distinguishable():
    svc = make_service()
    svc.ingest({**P1, "gender": "female"}, expected_version=1)  # P1 -> v2
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "P2", **merge_kwargs())
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    ctx = exc.value.context
    assert ctx["staleResource"] == "survivor"
    assert ctx["currentSurvivorVersion"] == 2
    assert ctx["currentMergedVersion"] == 1
    # 合并被拒绝后数据未被改动
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P2"


def test_merge_stale_merged_version_distinguishable():
    svc = make_service()
    svc.ingest({**P2, "gender": "other"}, expected_version=1)  # P2 -> v2
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P1", "P2", **merge_kwargs())
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    ctx = exc.value.context
    assert ctx["staleResource"] == "merged"
    assert ctx["currentMergedVersion"] == 2
    assert ctx["currentSurvivorVersion"] == 1


def test_merged_patient_cannot_be_merged_elsewhere():
    svc = make_service()
    svc.ingest(P3)
    svc.merge_patients("P1", "P2", **merge_kwargs())
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P3", "P2", expected_survivor_version=1,
                           expected_merged_version=2)
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    assert exc.value.context["mergedInto"] == "P1"


# ---------- 已作废记录不得当保留记录 ----------

def test_voided_patient_cannot_be_used_as_survivor():
    """拿已被并掉的患者当保留记录：明确拒绝（merged_resource, role=survivor），
    不执行任何改写，更不能把引用改指到作废记录上。"""
    svc = make_service()
    svc.ingest(P3)
    enc_on_p3 = {"resourceType": "Encounter", "id": "E3", "status": "finished",
                 "subject": {"reference": "Patient/P3"}}
    svc.ingest(enc_on_p3)
    # P2 先并入 P1：P2 自此作废（active=false, replaced-by -> P1）
    svc.merge_patients("P1", "P2", **merge_kwargs())

    # 后来想把 P3 也并过来，却误选了已作废的 P2 作为保留记录
    with pytest.raises(PipelineError) as exc:
        svc.merge_patients("P2", "P3", expected_survivor_version=2,
                           expected_merged_version=1)
    assert exc.value.reason is RejectReason.MERGED_RESOURCE
    ctx = exc.value.context
    assert ctx["role"] == "survivor"
    assert ctx["survivorId"] == "P2"
    assert ctx["mergedInto"] == "P1"
    assert "P1" in exc.value.message  # 说清原因与正确做法

    # P3 身上的就诊没有被改指到作废的 P2
    assert svc.get("Encounter", "E3").body["subject"]["reference"] == "Patient/P3"
    # P3 仍有效、P2 仍只指向 P1，数据未被改一半
    assert svc.get("Patient", "P3").body.get("active", True) is True
    assert svc.get("Patient", "P2").body["link"] == [
        {"other": {"reference": "Patient/P1"}, "type": "replaced-by"}
    ]
    _assert_no_references_to(svc, "Patient/P2")


def test_chained_merge_onto_active_survivor_succeeds():
    """合法的链式合并：P2 并入 P1 后，再把 P3 并入仍有效的 P1，正常执行。"""
    svc = make_service()
    svc.ingest(P3)
    enc_on_p3 = {"resourceType": "Encounter", "id": "E3", "status": "finished",
                 "subject": {"reference": "Patient/P3"}}
    svc.ingest(enc_on_p3)
    svc.merge_patients("P1", "P2", **merge_kwargs())

    result = svc.merge_patients("P1", "P3", expected_survivor_version=1,
                                expected_merged_version=1)
    assert result.status.value == "merged"
    assert svc.get("Encounter", "E3").body["subject"]["reference"] == "Patient/P1"
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P1"
    _assert_no_references_to(svc, "Patient/P2")
    _assert_no_references_to(svc, "Patient/P3")


def test_voided_survivor_rejection_agrees_across_entries(monkeypatch, capsys):
    """Python / CLI / HTTP 对“作废记录当保留记录”给出一致的拒绝结论。"""
    def _setup(svc: IngestionService) -> None:
        svc.ingest(P1)
        svc.ingest(P2)
        svc.ingest(P3)
        svc.merge_patients("P1", "P2", **merge_kwargs())  # P2 -> v2 作废

    # Python 入口
    svc = IngestionService()
    _setup(svc)
    with pytest.raises(PipelineError) as py_exc:
        svc.merge_patients("P2", "P3", expected_survivor_version=2,
                           expected_merged_version=1)
    assert py_exc.value.reason is RejectReason.MERGED_RESOURCE
    assert py_exc.value.context["role"] == "survivor"
    assert py_exc.value.context["mergedInto"] == "P1"

    # CLI 入口：非零退出码 + 同样的 reason/context
    svc2 = IngestionService()
    _setup(svc2)
    monkeypatch.setattr(cli, "IngestionService", lambda: svc2)
    rc = cli.main(["merge", "P2", "P3",
                   "--survivor-version", "2", "--merged-version", "1"])
    assert rc == cli.EXIT_REJECTED
    err = json.loads(capsys.readouterr().err)
    assert err["reason"] == "merged_resource"
    assert err["context"]["role"] == "survivor"
    assert err["context"]["mergedInto"] == "P1"

    # HTTP 入口：409 + 同样的 reason/context
    api.reset_service()
    client = TestClient(api.app)
    for body in (P1, P2, P3):
        client.post(f"/fhir/{body['resourceType']}", json=body)
    client.post("/fhir/Patient/P1/$merge",
                json={"mergedId": "P2", "expectedSurvivorVersion": 1,
                      "expectedMergedVersion": 1})
    r = client.post("/fhir/Patient/P2/$merge",
                    json={"mergedId": "P3", "expectedSurvivorVersion": 2,
                          "expectedMergedVersion": 1})
    assert r.status_code == 409
    payload = r.json()
    assert payload["reason"] == "merged_resource"
    assert payload["context"]["role"] == "survivor"
    assert payload["context"]["mergedInto"] == "P1"



# ---------- 合并后的写入处置 ----------

def test_update_merged_patient_rejected_with_distinguishable_reason():
    svc = make_service()
    svc.merge_patients("P1", "P2", **merge_kwargs())
    with pytest.raises(PipelineError) as exc:
        svc.ingest({**P2, "gender": "female"}, expected_version=2)
    assert exc.value.reason is RejectReason.MERGED_RESOURCE
    assert exc.value.context["mergedInto"] == "P1"


def test_new_resource_referencing_merged_patient_rejected():
    svc = make_service()
    svc.merge_patients("P1", "P2", **merge_kwargs())
    dangling = {"resourceType": "Encounter", "id": "E2", "status": "planned",
                "subject": {"reference": "Patient/P2"}}
    with pytest.raises(PipelineError) as exc:
        svc.ingest(dangling)
    assert exc.value.reason is RejectReason.MERGED_RESOURCE
    assert exc.value.context["mergedInto"] == "P1"
    assert svc.get("Encounter", "E2") is None  # 没有默默写进去


def test_unversioned_overwrite_rejected_on_all_entries():
    svc = make_service()
    with pytest.raises(PipelineError) as exc:
        svc.ingest({**P1, "gender": "female"})  # 内容变了却不带版本
    assert exc.value.reason is RejectReason.VERSION_CONFLICT
    assert exc.value.context["missingPrecondition"] == "expected_version"
    assert svc.get("Patient", "P1").body["gender"] == "male"  # 未被覆盖
    # 内容一致的重复提交仍然幂等返回 duplicate
    dup = svc.ingest(P1)
    assert dup.status.value == "duplicate"


# ---------- 并发 ----------

def test_concurrent_merges_only_one_succeeds():
    svc = make_service()
    svc.ingest(P3)
    outcomes, errors = [], []

    def merge(target):
        try:
            outcomes.append(svc.merge_patients(
                target, "P2", expected_survivor_version=1,
                expected_merged_version=1))
        except PipelineError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=merge, args=(t,)) for t in ("P1", "P3")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 两个并发合并只有一个真正执行；另一个被明确拒绝（版本或已合并）
    assert len(outcomes) == 1 and outcomes[0].status.value == "merged"
    assert len(errors) == 1
    assert errors[0].reason is RejectReason.VERSION_CONFLICT
    # 最终不存在指向被合并 id 的悬空引用
    survivor = outcomes[0].survivor_id
    for resource in svc.store.all_resources():
        subject = resource.body.get("subject")
        if isinstance(subject, dict):
            assert subject["reference"] != "Patient/P2"
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == (
        f"Patient/{survivor}")


def test_concurrent_merges_deterministic_only_one_wins():
    """并发合并的确定性版本：第二个合并在第一个持锁期间排队，
    必以可区分的版本冲突（或已合并）被拒绝，不产生第二条改指链。"""
    svc = make_service()
    svc.ingest(P3)
    tracker = _LockWaitTracker(svc.store._lock)
    svc.store._lock = tracker
    passed = threading.Event()
    release = threading.Event()
    svc.store.merge_barrier = lambda s, m: (passed.set(), release.wait(timeout=5))

    outcomes, errors = [], []

    def merge_p1_p2():
        try:
            outcomes.append(svc.merge_patients(
                "P1", "P2", expected_survivor_version=1,
                expected_merged_version=1))
        except PipelineError as exc:
            errors.append(exc)

    def merge_p3_p2():
        try:
            outcomes.append(svc.merge_patients(
                "P3", "P2", expected_survivor_version=1,
                expected_merged_version=1))
        except PipelineError as exc:
            errors.append(exc)

    first = threading.Thread(target=merge_p1_p2)
    second = threading.Thread(target=merge_p3_p2)
    first.start()
    assert passed.wait(timeout=5)      # P1<-P2 已核对通过并持锁
    second.start()
    assert _wait_until(lambda: second.ident in tracker.waiting, timeout=5)
    release.set()
    first.join()
    second.join()

    assert len(outcomes) == 1 and outcomes[0].survivor_id == "P1"
    assert len(errors) == 1
    assert errors[0].reason is RejectReason.VERSION_CONFLICT
    _assert_no_references_to(svc, "Patient/P2")
    assert svc.get("Encounter", "E1").body["subject"]["reference"] == "Patient/P1"


def test_merge_pinned_before_blocked_update():
    """合并与更新抢跑（合并先）的确定性版本：更新在合并临界区外排队，
    合并提交后更新必以 merged_resource 被拒绝，互不覆盖、不改一半。"""
    svc = make_service()
    tracker = _LockWaitTracker(svc.store._lock)
    svc.store._lock = tracker
    passed = threading.Event()
    release = threading.Event()
    svc.store.merge_barrier = lambda s, m: (passed.set(), release.wait(timeout=5))

    update_outcome: dict = {}

    def do_update():
        try:
            update_outcome["result"] = svc.ingest(
                {**P2, "gender": "female"}, expected_version=1)
        except PipelineError as exc:
            update_outcome["error"] = exc

    mt = threading.Thread(target=lambda: svc.merge_patients(
        "P1", "P2", **merge_kwargs()))
    ut = threading.Thread(target=do_update)
    mt.start()
    assert passed.wait(timeout=5)
    ut.start()
    assert _wait_until(lambda: ut.ident in tracker.waiting, timeout=5)
    release.set()
    mt.join()
    ut.join()

    assert "error" in update_outcome
    assert update_outcome["error"].reason is RejectReason.MERGED_RESOURCE
    assert update_outcome["error"].context["mergedInto"] == "P1"
    # 被拒更新没有留下半截内容：性别仍为 v1 的 male，合并标记完整
    p2 = svc.get("Patient", "P2")
    assert p2.version == 2
    assert p2.body["gender"] == "male"
    assert p2.body["active"] is False


def test_merge_races_with_update_on_merged_patient():
    """合并与被合并患者的并发更新：必有一方被明确拒绝，互不覆盖。"""
    svc = make_service()
    results, errors = [], []
    barrier = threading.Barrier(2)

    def do_merge():
        barrier.wait()
        try:
            results.append(("merge", svc.merge_patients("P1", "P2",
                                                        **merge_kwargs())))
        except PipelineError as exc:
            errors.append(("merge", exc))

    def do_update():
        barrier.wait()
        try:
            results.append(("update", svc.ingest({**P2, "gender": "female"},
                                                 expected_version=1)))
        except PipelineError as exc:
            errors.append(("update", exc))

    threads = [threading.Thread(target=do_merge), threading.Thread(target=do_update)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 恰好一个成功、一个被拒绝
    assert len(results) == 1 and len(errors) == 1
    winner, loser = results[0][0], errors[0][0]
    assert {winner, loser} == {"merge", "update"}
    if winner == "merge":
        # 合并先完成：更新被合并记录 -> merged_resource
        assert errors[0][1].reason is RejectReason.MERGED_RESOURCE
        assert svc.get("Patient", "P2").body["active"] is False
    else:
        # 更新先完成：合并看到的版本落后 -> version_conflict 且可区分
        assert errors[0][1].reason is RejectReason.VERSION_CONFLICT
        assert errors[0][1].context["staleResource"] == "merged"
        assert svc.get("Patient", "P2").body["gender"] == "female"  # 未被回滚


def test_merge_races_with_new_encounter_on_merged_patient():
    """合并进行中向被合并患者挂新就诊：两种交错均不留悬空引用。

    不依赖“多跑几轮碰运气”，而是通过 store 的合并阶段插桩
    （merge_barrier，在持锁状态、核对通过后/改写前触发）确定性地
    构造两种交错各一次。
    """
    new_encounter = {"resourceType": "Encounter", "id": "E9",
                     "status": "planned",
                     "subject": {"reference": "Patient/P2"}}

    # ---- 交错 A：合并先完成，新建被锁挡在临界区外 -> 明确拒绝 ----
    svc = make_service()
    tracker = _LockWaitTracker(svc.store._lock)
    svc.store._lock = tracker
    merge_passed_checks = threading.Event()
    release_merge = threading.Event()

    def barrier(survivor_id, merged_id, *,
                passed=merge_passed_checks, go=release_merge):
        passed.set()
        go.wait(timeout=5)

    svc.store.merge_barrier = barrier

    def do_merge():
        svc.merge_patients("P1", "P2", **merge_kwargs())

    mt = threading.Thread(target=do_merge)
    mt.start()
    assert merge_passed_checks.wait(timeout=5)  # 合并已核对通过并持锁

    create_outcome: dict = {}

    def do_create():
        try:
            create_outcome["result"] = svc.ingest(dict(new_encounter))
        except PipelineError as exc:
            create_outcome["error"] = exc

    ct = threading.Thread(target=do_create)
    ct.start()
    # 等到新建线程确实阻塞在写锁上，再放行合并 -> 合并必然先提交
    assert _wait_until(lambda: ct.ident in tracker.waiting, timeout=5)
    release_merge.set()
    mt.join()
    ct.join()

    assert "error" in create_outcome
    assert create_outcome["error"].reason is RejectReason.MERGED_RESOURCE
    assert create_outcome["error"].context["mergedInto"] == "P1"
    assert svc.get("Encounter", "E9") is None  # 没有悄悄写进去
    _assert_no_references_to(svc, "Patient/P2")

    # ---- 交错 B：新建先提交，随后合并扫描到它 -> 一并改指 ----
    svc = make_service()

    def barrier_then_create(survivor_id, merged_id, *, service=svc):
        # 仍持合并写锁：合并已核对通过但尚未改指。
        # 事务可重入，此刻写入等价于“资源恰好赶在合并扫描前提交”。
        created = service.ingest(dict(new_encounter))
        assert created.status.value == "created"

    svc.store.merge_barrier = barrier_then_create
    result = svc.merge_patients("P1", "P2", **merge_kwargs())
    assert result.status.value == "merged"
    assert {"resourceType": "Encounter", "id": "E9"} in [
        {"resourceType": r["resourceType"], "id": r["id"]}
        for r in result.repointed
    ]
    assert svc.get("Encounter", "E9").body["subject"]["reference"] == "Patient/P1"
    _assert_no_references_to(svc, "Patient/P2")


# ---------- 三入口结论一致 ----------

def _merge_via_python(svc: IngestionService) -> dict:
    return svc.merge_patients("P1", "P2", **merge_kwargs()).to_dict()


def _merge_via_cli(monkeypatch, capsys) -> dict:
    svc = make_service()
    monkeypatch.setattr(cli, "IngestionService", lambda: svc)
    rc = cli.main(["merge", "P1", "P2",
                   "--survivor-version", "1", "--merged-version", "1"])
    assert rc == cli.EXIT_OK
    return json.loads(capsys.readouterr().out)


def _merge_via_http() -> dict:
    api.reset_service()
    client = TestClient(api.app)
    for body in (P1, P2, ENC_ON_P2, OBS_ON_P2):
        r = client.post(f"/fhir/{body['resourceType']}", json=body)
        assert r.status_code == 200
    r = client.post("/fhir/Patient/P1/$merge",
                    json={"mergedId": "P2", "expectedSurvivorVersion": 1,
                          "expectedMergedVersion": 1})
    assert r.status_code == 200
    return r.json()


def test_three_entries_agree_on_merge(monkeypatch, capsys):
    py = _merge_via_python(make_service())
    cl = _merge_via_cli(monkeypatch, capsys)
    http = _merge_via_http()
    for key in ("status", "survivorId", "mergedId",
                "survivorVersion", "mergedVersion", "repointed"):
        assert py[key] == cl[key] == http[key], f"入口结论不一致: {key}"


def test_three_entries_agree_on_stale_rejection(monkeypatch, capsys):
    # Python 入口
    svc = make_service()
    svc.ingest({**P2, "gender": "other"}, expected_version=1)  # P2 -> v2
    with pytest.raises(PipelineError) as py_exc:
        svc.merge_patients("P1", "P2", **merge_kwargs())
    assert py_exc.value.reason is RejectReason.VERSION_CONFLICT
    assert py_exc.value.context["staleResource"] == "merged"

    # CLI 入口：同样的拒绝，非零退出码
    svc2 = make_service()
    svc2.ingest({**P2, "gender": "other"}, expected_version=1)
    monkeypatch.setattr(cli, "IngestionService", lambda: svc2)
    rc = cli.main(["merge", "P1", "P2",
                   "--survivor-version", "1", "--merged-version", "1"])
    assert rc == cli.EXIT_REJECTED
    err = json.loads(capsys.readouterr().err)
    assert err["reason"] == "version_conflict"
    assert err["context"]["staleResource"] == "merged"

    # HTTP 入口：409 + 同样的 reason/context
    api.reset_service()
    client = TestClient(api.app)
    for body in (P1, P2, ENC_ON_P2, OBS_ON_P2):
        client.post(f"/fhir/{body['resourceType']}", json=body)
    client.put("/fhir/Patient/P2", json={**P2, "gender": "other"},
               headers={"If-Match": "1"})
    r = client.post("/fhir/Patient/P1/$merge",
                    json={"mergedId": "P2", "expectedSurvivorVersion": 1,
                          "expectedMergedVersion": 1})
    assert r.status_code == 409
    assert r.json()["reason"] == "version_conflict"
    assert r.json()["context"]["staleResource"] == "merged"


def test_http_merge_requires_versions():
    api.reset_service()
    client = TestClient(api.app)
    client.post("/fhir/Patient", json=P1)
    client.post("/fhir/Patient", json=P2)
    r = client.post("/fhir/Patient/P1/$merge", json={"mergedId": "P2"})
    assert r.status_code == 422
    assert r.json()["reason"] == "missing_field"


# ---------- 去标识化兼容 ----------

def _assert_reference_closure(resources: list[dict]) -> None:
    """去标识化结果中所有引用都能解析到结果集内的资源 id。"""
    ids = {(r["resourceType"], r["id"]) for r in resources}
    for resource in resources:
        refs = [resource.get("subject"), resource.get("encounter")]
        for link in resource.get("link") or []:
            refs.append(link.get("other") if isinstance(link, dict) else None)
        for ref in refs:
            if isinstance(ref, dict) and isinstance(ref.get("reference"), str):
                rtype, rid = ref["reference"].split("/")
                assert (rtype, rid) in ids, (
                    f"悬空引用: {resource['resourceType']}/{resource['id']} "
                    f"-> {ref['reference']}")


def test_deidentify_after_merge_is_closed_and_stable():
    svc = make_service()
    svc.merge_patients("P1", "P2", **merge_kwargs())

    first = svc.deidentify_all()
    # 引用闭合：含被合并患者的 link 在内，没有指向真实 id 的悬空引用
    _assert_reference_closure(first["resources"])
    serialized = json.dumps(first["resources"], ensure_ascii=False)
    assert "Patient/P2" not in serialized and '"P2"' not in serialized

    # 被合并记录仍在（可审计），其 link 指向保留记录的伪标识
    mapping = first["mapping"]
    pseudo_p1 = mapping["Patient/P1"]
    pseudo_p2 = mapping["Patient/P2"]
    merged_res = next(r for r in first["resources"]
                      if r["resourceType"] == "Patient" and r["id"] == pseudo_p2)
    assert merged_res["link"][0]["other"]["reference"] == f"Patient/{pseudo_p1}"
    assert merged_res["active"] is False

    # 重复执行：映射不增、结果不变
    second = svc.deidentify_all()
    assert second["mapping"] == first["mapping"]
    assert second["resources"] == first["resources"]

    # 合并前后历史都可追溯
    assert [e.version for e in svc.get_history("Patient", "P2").entries] == [1, 2]
    assert [e.version for e in svc.get_history("Patient", "P1").entries] == [1]


def test_merge_logs_inputs_and_decision_basis(caplog):
    caplog.set_level(logging.INFO, logger="fhir_pipeline.service")
    svc = make_service()
    svc.merge_patients("P1", "P2", idempotency_key="MERGE-LOG-1", **merge_kwargs())
    svc.merge_patients("P1", "P2", idempotency_key="MERGE-LOG-1", **merge_kwargs())
    texts = [r.getMessage() for r in caplog.records]
    # 输入内容：合并双方、版本、幂等键
    assert any("MERGE-LOG-1" in t and "P1" in t and "P2" in t for t in texts)
    # 判定依据：改指清单与重复提交的判定
    assert any("改指" in t for t in texts)
    assert any("duplicate" in t for t in texts)
