"""跨资源去标识化：稳定伪标识、引用可解析、重复执行幂等。"""

from fhir_pipeline.deidentify import PseudonymMapper, deidentify_resource

PATIENT = {"resourceType": "Patient", "id": "P1", "gender": "female",
           "birthDate": "1990-03-04", "name": [{"text": "张三"}],
           "identifier": [{"system": "id-card", "value": "ID-001"}]}
ENCOUNTER = {"resourceType": "Encounter", "id": "E1", "status": "finished",
             "subject": {"reference": "Patient/P1"}}
OBSERVATION = {"resourceType": "Observation", "id": "O1", "status": "final",
               "code": {"text": "Glucose"}, "subject": {"reference": "Patient/P1"},
               "encounter": {"reference": "Encounter/E1"},
               "valueQuantity": {"value": 5.4, "unit": "mmol/L"}}


def _run_dataset(salt: str = "salt-A"):
    mapper = PseudonymMapper(salt=salt)
    p = deidentify_resource(PATIENT, mapper)
    e = deidentify_resource(ENCOUNTER, mapper)
    o = deidentify_resource(OBSERVATION, mapper)
    return mapper, p, e, o


def test_same_real_id_maps_to_same_pseudonym_everywhere():
    mapper1, p1, e1, o1 = _run_dataset()
    # 另一张表/另一批资源，同样真实标识 -> 同样伪标识（盐相同即一致）
    mapper2 = PseudonymMapper(salt="salt-A")
    assert mapper2.pseudonym_for("Patient", "P1") == p1["id"]
    assert e1["subject"]["reference"] == f"Patient/{p1['id']}"
    assert o1["subject"]["reference"] == f"Patient/{p1['id']}"
    # 跨资源映射稳定（多次调用不变）
    assert mapper1.pseudonym_for("Patient", "P1") == p1["id"]


def test_references_remain_resolvable_after_deid():
    _, p, e, o = _run_dataset()
    pseudo_patient = p["id"]
    pseudo_encounter = e["id"]
    ids = {("Patient", pseudo_patient), ("Encounter", pseudo_encounter)}
    for ref in (e["subject"]["reference"], o["subject"]["reference"],
                o["encounter"]["reference"]):
        t, i = ref.split("/")
        assert (t, i) in ids


def test_repeated_run_is_idempotent_and_keeps_references():
    mapper, p1, e1, o1 = _run_dataset()
    mapping_before = dict(mapper.snapshot())
    # 对“已经去标识化”的结果再次执行
    p2 = deidentify_resource(p1, mapper)
    e2 = deidentify_resource(e1, mapper)
    o2 = deidentify_resource(o1, mapper)
    assert p2["id"] == p1["id"]
    assert e2["subject"]["reference"] == e1["subject"]["reference"]
    assert o2["encounter"]["reference"] == o1["encounter"]["reference"]
    # 不产生新映射
    assert mapper.snapshot() == mapping_before


def test_direct_identifiers_removed_or_generalized():
    _, p, _, _ = _run_dataset()
    assert "name" not in p
    assert p["birthDate"] == "1990"          # 出生日期泛化到年
    assert p["identifier"][0]["value"] != "ID-001"
    assert p["meta"]["_deidentified"] is True
    # 同一证件号跨次稳定
    m = PseudonymMapper(salt="salt-A")
    assert m.pseudonym("identifier/ID-001") == p["identifier"][0]["value"]


def test_different_salts_produce_different_pseudonyms():
    a = PseudonymMapper(salt="a").pseudonym_for("Patient", "P1")
    b = PseudonymMapper(salt="b").pseudonym_for("Patient", "P1")
    assert a != b
