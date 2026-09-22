"""deidentify 单测：稳定一致映射、引用可解析、重复执行幂等。"""

import logging

from fhir_pipe.deidentify import PseudonymTable, deidentify_resource
from fhir_pipe.parser import parse_resource

from conftest import encounter, observation, patient

log = logging.getLogger("fhir_pipe.test")


def _sample_graph():
    return (
        parse_resource(patient()),
        parse_resource(encounter()),
        parse_resource(observation()),
    )


def test_same_real_id_maps_to_same_pseudo_everywhere() -> None:
    table = PseudonymTable(salt="salt-A")
    p, e, o = _sample_graph()
    log.info("test_consistency 输入=Patient/Encounter/Observation 同一主体 P1")
    dp = deidentify_resource(p, table)
    de = deidentify_resource(e, table)
    do = deidentify_resource(o, table)

    # 同一真实 Patient/P1 在三处必须是同一个伪 ID
    patient_pseudo = dp.id
    assert de.subject.resource_id == patient_pseudo
    assert do.subject.resource_id == patient_pseudo
    assert do.encounter.resource_id == de.id
    log.info(
        "test_consistency 判定=Patient/P1->%s，Encounter/E1->%s，引用一致",
        patient_pseudo, de.id,
    )


def test_pseudonym_is_deterministic_and_salt_scoped() -> None:
    t1 = PseudonymTable(salt="salt-A")
    t2 = PseudonymTable(salt="salt-A")
    t3 = PseudonymTable(salt="salt-B")
    a = t1.pseudonym("Patient", "P1")
    assert t2.pseudonym("Patient", "P1") == a  # 同 salt 可复现
    assert t3.pseudonym("Patient", "P1") != a  # 不同 salt 不同
    log.info("test_deterministic 判定=%s 在同 salt 稳定、异 salt 不同", a)


def test_direct_identifiers_removed_or_generalized() -> None:
    table = PseudonymTable()
    dp = deidentify_resource(parse_resource(patient()), table)
    assert dp.name == ""
    assert dp.birth_date == "1980-01-01"  # 精确生日泛化到年份
    assert dp.meta.deidentified and dp.meta.pseudo_id == dp.id
    log.info("test_direct_id 判定=name 移除、birthDate 泛化为 %s", dp.birth_date)


def test_references_still_resolve_after_deid() -> None:
    table = PseudonymTable()
    dp, de, do = (deidentify_resource(x, table) for x in _sample_graph())
    ids = {("Patient", dp.id), ("Encounter", de.id), ("Observation", do.id)}

    def resolves(rtype: str, rid: str) -> bool:
        return (rtype, rid) in ids

    assert resolves(de.subject.resource_type, de.subject.resource_id)
    assert resolves(do.subject.resource_type, do.subject.resource_id)
    assert do.encounter is not None
    assert resolves(do.encounter.resource_type, do.encounter.resource_id)
    log.info("test_resolve 判定=去标识化后 subject/encounter 引用全部可解析")


def test_repeated_deidentify_creates_no_new_mapping() -> None:
    """重复执行不得产生新映射，也不得破坏引用关系。"""
    table = PseudonymTable()
    p, e, o = _sample_graph()
    dp1, de1, do1 = (deidentify_resource(x, table) for x in (p, e, o))
    first_mappings = dict(table.mappings())

    # 再跑一轮（对原始资源重放）
    dp2, de2, do2 = (deidentify_resource(x, table) for x in (p, e, o))
    assert (dp2.id, de2.id, do2.id) == (dp1.id, de1.id, do1.id)
    assert table.mappings() == first_mappings

    # 直接对“已去标识化视图”再去标识化 = no-op
    dp3 = deidentify_resource(dp1, table)
    do3 = deidentify_resource(do1, table)
    assert dp3.id == dp1.id and do3.subject.ref == do1.subject.ref
    assert table.mappings() == first_mappings
    log.info(
        "test_repeat 判定=重复执行映射不变：%d 条，引用关系保持",
        len(first_mappings),
    )


def test_distinct_real_ids_get_distinct_pseudos() -> None:
    table = PseudonymTable()
    a = table.pseudonym("Patient", "P1")
    b = table.pseudonym("Patient", "P2")
    assert a != b
    log.info("test_distinct 判定=P1->%s, P2->%s 互不相同", a, b)
