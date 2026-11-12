"""GROUP BY and PARTITION BY transformations (docs/groupby_partition_plan.md).

The classifier and statement builder (engine/etl_sql.py), then every workspace
end to end through the API: a window column at source grain (scenario A), a
declared grain (scenario B), every rejection the specification lists, and the
same feature on an unrelated, hostile dataset (CLAIMS99) to show nothing is
tied to the policy data.

Offline (LLM tiers off) and with the server's upload area redirected to a temp
folder, like tests/test_row_filter.py.
"""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from engine.etl_sql import (ORD, build_target_sql, check_same_within_group,
                            classify_expression, conservation_function,
                            spec_expression_errors)

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "groupby"
HOSTILE = ROOT / "tests" / "fixtures"

WINDOW_SQL = {
    "policy_active_role_count":
        """COUNT(*) FILTER (WHERE "STAXRF" = '30') OVER (PARTITION BY "POLREF")""",
    "policy_beneficiary_count":
        """COUNT(*) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30') OVER (PARTITION BY "POLREF")""",
    "policy_beneficiary_share_total":
        """SUM(TRY_CAST(NULLIF(trim("BENPCT"), '') AS DECIMAL(9,2))) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30') OVER (PARTITION BY "POLREF")""",
    "policy_total_monthly_premium":
        """SUM(TRY_CAST(NULLIF(trim("CTBAMT"), '') AS DECIMAL(12,2))) FILTER (WHERE "STAXRF" = '30') OVER (PARTITION BY "POLREF")""",
}
# product_type decoded per policy (constant within a policy, so ANY_VALUE)
PRODUCT_ANY = ("ANY_VALUE(CASE \"PRDREF\" WHEN 'RS-11' THEN 'Personal Pension' WHEN 'RS-22' "
               "THEN 'Stakeholder Pension' WHEN 'GP-40' THEN 'Group Personal Pension' WHEN 'IR-05' "
               "THEN 'Immediate Annuity' WHEN 'TD-30' THEN 'Term Assurance' ELSE NULL END)")
GRAIN_SQL = {
    "policy_reference": 'trim("POLREF")',
    "active_role_count": """COUNT(*) FILTER (WHERE "STAXRF" = '30')""",
    "beneficiary_count": """COUNT(*) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30')""",
    "total_monthly_premium": """SUM(TRY_CAST(NULLIF(trim("CTBAMT"), '') AS DECIMAL(12,2))) FILTER (WHERE "STAXRF" = '30')""",
}


# ============================================================ classifier
@pytest.fixture(scope="module")
def ws():
    con = duckdb.connect()
    con.execute(f"""CREATE VIEW w AS SELECT c.*, p.PRDREF FROM
        read_csv_auto('{(FIX / "CO040.csv").as_posix()}', all_varchar=true) c LEFT JOIN
        read_csv_auto('{(FIX / "PO010.csv").as_posix()}', all_varchar=true) p
        ON trim(c.POLREF) = trim(p.POLREF)""")
    yield con
    con.close()


@pytest.mark.parametrize("expr,keys,kind,code", [
    ('trim("CLIREF")', None, "row", None),
    ('SUM(TRY_CAST("CTBAMT" AS DOUBLE)) OVER (PARTITION BY "POLREF")', None, "window", None),
    ("COUNT(*) FILTER (WHERE \"ROLREF\" = 'BN')", None, "aggregate", "E-EXPR-AGG-NO-GRAIN"),
    ('(SELECT 1 FROM w)', None, "invalid", "E-EXPR-SUBQUERY"),
    ("(SELECT count(*) FROM read_csv_auto('x.csv'))", None, "invalid", "E-EXPR-SUBQUERY"),
    ('g."POLREF"', None, "invalid", "E-EXPR-QUALIFIED"),
    ('SUM("CTBAMT") GROUP BY "POLREF"', None, "invalid", "E-EXPR-PARSE"),
    ('1; DROP TABLE w', None, "invalid", "E-EXPR-PARSE"),
    ('*', None, "invalid", "E-EXPR-PARSE"),
    ('row_number() OVER (PARTITION BY "POLREF")', None, "window", "E-EXPR-WINDOW-NONDET"),
    ('row_number() OVER (PARTITION BY "POLREF" ORDER BY "UNIQUE_ID")', None, "window", None),
    ('"NO_SUCH_COLUMN"', None, "row", "E-EXPR-BIND"),
    ('trim("POLREF")', ["POLREF"], "group_key", None),
    ("COUNT(*) FILTER (WHERE \"ROLREF\" = 'BN')", ["POLREF"], "aggregate", None),
    ('ANY_VALUE("PRDREF")', ["POLREF"], "aggregate", None),
    ('SUM("CTBAMT") OVER (PARTITION BY "POLREF")', ["POLREF"], "window", "E-EXPR-WINDOW-UNDER-GRAIN"),
    ('trim("CLIREF")', ["POLREF"], "row", "E-EXPR-NOT-GROUPED"),
    ('"PRDREF"', ["POLREF"], "row", "I-EXPR-ANY-VALUE"),
    # set on one role of a policy, blank on the others: NOT constant per group
    ('TRY_CAST("CTBAMT" AS DOUBLE)', ["POLREF"], "row", "E-EXPR-NOT-GROUPED"),
    ("'constant'", ["POLREF"], "row", None),
])
def test_legality_matrix(ws, expr, keys, kind, code):
    c = classify_expression(expr, ws, '"w"', keys, attr="x")
    assert (c.kind, c.error_code) == (kind, code), c.message
    if code == "I-EXPR-ANY-VALUE":
        assert c.suggested_sql == f"ANY_VALUE({expr})"


def test_window_partition_and_messages(ws):
    c = classify_expression('SUM("CTBAMT") OVER (PARTITION BY "POLREF", "ROLREF")', ws, '"w"')
    assert c.partition_by == ["POLREF", "ROLREF"]
    c = classify_expression('COUNT(*)', ws, '"w"')
    assert "OVER (PARTITION BY" in c.message and "target grain" in c.message
    c = classify_expression('t."ABC DEF"', ws, '"w"')
    assert '"ABC DEF"' in c.message                 # the reviewer's own column, not a fixed example


def test_same_within_group(ws):
    assert check_same_within_group(ws, '"PRDREF"', '"w"', ["POLREF"])[0] == 0
    n, sample = check_same_within_group(ws, '"ROLREF"', '"w"', ["POLREF"])
    assert n and sample


def test_conservation_patterns():
    assert conservation_function("SUM(TRY_CAST(x AS DOUBLE)) FILTER (WHERE s = '1')") == "sum"
    assert conservation_function("COUNT(*) FILTER (WHERE a = 'b')") == "count"
    assert conservation_function("count(x)") == "count"
    assert conservation_function("MAX(x)") is None and conservation_function("SUM(x) + 1") is None
    assert conservation_function("SUM(x) OVER (PARTITION BY p)") is None
    # distinct counts do not add up across groups (found by the CLAIMS99 test)
    assert conservation_function("COUNT(DISTINCT x)") is None
    assert conservation_function("SUM(DISTINCT x)") is None


def test_statement_builder_orders_rows(ws):
    sel = [("p", 'trim("POLREF")'), ("total", 'SUM(TRY_CAST("CTBAMT" AS DOUBLE)) OVER (PARTITION BY "POLREF")')]
    sql = build_target_sql(sel, 'SELECT * FROM w', extra=[(ORD, f'"{ORD}"')])
    assert sql.rstrip().endswith(f'ORDER BY "{ORD}"')
    ords = [r[0] for r in ws.execute(f'SELECT "{ORD}" FROM ({sql})').fetchall()]
    assert ords == sorted(ords) and len(ords) == 199      # source order, despite the window
    g = build_target_sql([("p", 'trim("POLREF")'), ("n", "COUNT(*)")], "SELECT * FROM w", ["POLREF"])
    assert 'GROUP BY "POLREF"\nORDER BY "POLREF"' in g
    assert ws.execute(f"SELECT count(*) FROM ({g})").fetchone()[0] == 30


def test_client_spec_guard():
    spec = {"mappings": [{"target_attribute": "a", "gate": "auto_accept",
                          "transformation_sql": "(SELECT 1)"},
                         {"target_attribute": "b", "gate": "auto_accept", "transformation_sql": "SUM(x)"},
                         {"target_attribute": "c", "gate": "reject", "transformation_sql": "(SELECT 2)"}]}
    errs = spec_expression_errors(spec)
    assert len(errs) == 2 and errs[0].startswith("a:") and errs[1].startswith("b:")
    assert spec_expression_errors(dict(spec, grain={"group_by": ["x"], "errors": ["bad"]}))[0] \
        .startswith("the target grain is invalid")


# ============================================================ API end to end
def _sse(text):
    return [json.loads(c[6:]) for c in text.split("\n\n") if c.startswith("data: ")]


def _accept_all(queue):
    out = {}
    for it in queue["items"]:
        a = it["target_attribute"]
        if it["kind"] == "unmapped_target":
            out[a] = {"action": "edit", "transformation_sql": it.get("suggested_sql") or "NULL"}
        elif it["gate"] == "reject":
            out[a] = {"action": "reject"}
        elif it.get("suggested_sql"):
            out[a] = {"action": "edit", "transformation_sql": it["suggested_sql"]}
        else:
            out[a] = {"action": "accept"}
    return out


@pytest.fixture(scope="module")
def api(tmp_path_factory):
    from fastapi.testclient import TestClient

    from api import server

    env_keys = ("OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY")
    env_saved = {k: os.environ.get(k) for k in env_keys}
    for k in env_keys:
        os.environ[k] = ""
    saved = {k: getattr(server, k) for k in ("UPLOAD", "WH_DB", "STATE")}
    up = tmp_path_factory.mktemp("gb_uploads")
    server.UPLOAD, server.WH_DB = up, str(up / "warehouse.duckdb")
    server.STATE = server._empty_state()
    server._SOURCE_FIELDS.update(signature=None, columns=None)
    try:
        yield {"client": TestClient(server.app), "tmp": tmp_path_factory}
    finally:
        for k, v in saved.items():
            setattr(server, k, v)
        server._SOURCE_FIELDS.update(signature=None, columns=None)
        for k, v in env_saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _run(api, sources, dicts, target):
    """Upload, run the mapping, return (spec, review queue)."""
    c = api["client"]
    c.post("/api/inputs/reset")
    for role, files in (("source", sources), ("enriched", dicts), ("target", [target])):
        c.post("/api/inputs/upload", data={"role": role},
               files=[("files", (Path(f).name, Path(f).read_bytes())) for f in files])
    evs = _sse(c.post("/api/mapping/run", json={}).text)
    assert not [e for e in evs if e["type"] == "error"], evs
    return (next(e for e in evs if e["type"] == "complete")["final_spec"],
            next(e["artifact"] for e in evs if e.get("node") == "review"))


def _certify(api, spec, decisions):
    evs = _sse(api["client"].post("/api/mapping/certify",
                                  json={"spec": spec, "decisions": decisions}).text)
    assert not [e for e in evs if e["type"] == "error"], evs
    done = next(e for e in evs if e["type"] == "complete")
    node = {e["node"]: e["artifact"] for e in evs if e["type"] == "node"}
    return done["final_spec"], done["summary"]["verdict"], node["validation"], node["review"]


def _check(report, name):
    return next((c for c in report["checks"] if c["name"] == name), None)


def _downstream(api, spec):
    """transform -> validate -> reconcile (rules certified as proposed)."""
    c = api["client"]
    tr = c.post("/api/transform/run", json={"spec": spec}).json()
    assert "error" not in tr, tr
    va = c.post("/api/validate/run", json={"spec": spec, "csv": tr["csv"]}).json()
    c.post("/api/reconcile/rules", json={"spec": spec})
    c.post("/api/reconcile/certify", json={"spec": spec, "decisions": {}, "added": []})
    rc = c.post("/api/reconcile/run", json={"spec": spec, "csv": tr["csv"]}).json()
    return tr, va, rc


def _rows(csv_text):
    return list(csv.DictReader(io.StringIO(csv_text)))


def _num(v):
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return (v or "").strip()


POLICY = ([FIX / "CO040.csv", FIX / "PO010.csv"], [FIX / "dict_CO040.json", FIX / "dict_PO010.json"])


@pytest.fixture(scope="module")
def scen_a(api):
    spec, queue = _run(api, *POLICY, FIX / "policy_client_role.json")
    return spec, _accept_all(queue)


@pytest.fixture(scope="module")
def scen_b(api):
    spec, queue = _run(api, *POLICY, FIX / "policy_summary.json")
    return spec, _accept_all(queue)


# ---------------------------------------------------------------- AC-02
def test_window_columns_flow_through_every_workspace(api, scen_a):
    spec, dec = scen_a
    _run(api, *POLICY, FIX / "policy_client_role.json")         # inputs of scenario A loaded
    d = dict(dec, **{a: {"action": "edit", "transformation_sql": s} for a, s in WINDOW_SQL.items()})
    cs, verdict, report, _q = _certify(api, spec, d)
    assert verdict == "certified"
    kinds = {m["target_attribute"]: (m["transform_kind"], m["partition_by"]) for m in cs["mappings"]}
    assert all(kinds[a] == ("window", ["POLREF"]) for a in WINDOW_SQL)
    # the reviewer named no source: it is read from the SQL itself
    assert set(next(m for m in cs["mappings"] if m["target_attribute"] == "policy_beneficiary_count")
               ["source_attributes"]) == {"ROLREF", "STAXRF", "POLREF"}

    tr, va, rc = _downstream(api, cs)
    rows = _rows(tr["csv"])
    src_order = [r["UNIQUE_ID"] for r in csv.DictReader(open(FIX / "CO040.csv", encoding="utf-8"))]
    assert [r["role_record_id"] for r in rows] == src_order          # source order kept
    exp = {r["role_record_id"]: r for r in csv.DictReader(open(FIX / "expected_scenario_a.csv", encoding="utf-8"))}
    for r in rows:
        for a in WINDOW_SQL:
            assert _num(r[a]) == _num(exp[r["role_record_id"]][a]), (a, r["role_record_id"])
    assert tr["stats"]["window_columns"] == list(WINDOW_SQL)

    assert va["verdict"] == "certified"
    for a in WINDOW_SQL:
        assert _check(va, f"transform_rule:{a}")["status"] == "pass"
        assert _check(va, f"partition_consistency:{a}")["status"] == "pass"
    assert rc["verdict"] == "certified"
    tied = {x["column"] for x in _check(rc, "control_total:numeric_sums")["sample"]}
    assert set(WINDOW_SQL) <= tied                                   # nothing silently skipped

    # AC-13: a delivered file in another order still validates (key alignment)
    lines = tr["csv"].strip("\r\n").splitlines()
    shuffled = "\n".join([lines[0]] + lines[1:][::-1]) + "\n"
    va2 = api["client"].post("/api/validate/run", json={"spec": cs, "csv": shuffled}).json()
    assert va2["verdict"] == "certified", [c for c in va2["checks"] if c["status"] == "fail"]


# ---------------------------------------------------------------- AC-03..07
@pytest.mark.parametrize("sql,needle", [
    ('(SELECT COUNT(*) FROM "__workset" g WHERE g."POLREF" = "__workset"."POLREF")', "Sub-queries aren't supported"),
    ('COUNT(*) FILTER (WHERE "STAXRF" = \'30\') GROUP BY "POLREF"', "single SQL expression"),
    ("(SELECT count(*) FROM read_csv_auto('C:/any/path/CO040.csv') g)", "Sub-queries aren't supported"),
    ('COUNT(*) FILTER (WHERE "STAXRF" = \'30\')', "set a target grain"),
    ('row_number() OVER (PARTITION BY "POLREF")', "Add ORDER BY inside OVER"),
], ids=["subquery", "literal_group_by", "file_subquery", "aggregate_without_grain", "unordered_window"])
def test_illegal_expressions_are_rejected_at_certify(api, scen_a, sql, needle):
    spec, dec = scen_a
    _run(api, *POLICY, FIX / "policy_client_role.json")
    a = "policy_active_role_count"
    cs, verdict, report, queue = _certify(api, spec, dict(dec, **{a: {"action": "edit", "transformation_sql": sql}}))
    assert verdict == "blocked"
    chk = _check(report, f"transform_kind:{a}")
    assert chk and chk["status"] == "fail" and needle in chk["detail"], report["checks"]
    item = next(i for i in queue["items"] if i["target_attribute"] == a)  # back in the queue
    assert needle in " ".join(item["validator_exceptions"])
    # and nothing downstream will execute it
    assert "error" in api["client"].post("/api/transform/run", json={"spec": cs}).json() or \
        next(m for m in cs["mappings"] if m["target_attribute"] == a)["gate"] == "reject"


# ---------------------------------------------------------------- AC-08
def test_declared_grain_flows_through_every_workspace(api, scen_b):
    spec, dec = scen_b
    _run(api, *POLICY, FIX / "policy_summary.json")
    d = dict(dec, __grain__={"action": "set_grain", "group_by": ["POLREF"]},
             **{a: {"action": "edit", "transformation_sql": s} for a, s in GRAIN_SQL.items()})
    cs, verdict, report, queue = _certify(api, spec, d)
    assert cs["grain"]["group_by"] == ["POLREF"] and cs["grain"]["errors"] == []
    # product_type (a CASE over PRDREF) is constant within each policy: sent
    # back for review with the explicit ANY_VALUE rewrite, not silently wrapped
    assert verdict == "needs_review"
    item = next(i for i in queue["items"] if i["target_attribute"] == "product_type")
    assert item["suggested_sql"].startswith("ANY_VALUE(CASE")
    cs, verdict, report, queue = _certify(api, cs, {"product_type": {"action": "accept_suggestion"}})
    assert verdict == "certified", [c for c in report["checks"] if c["status"] == "fail"]
    kinds = {m["target_attribute"]: m["transform_kind"] for m in cs["mappings"]}
    assert kinds["policy_reference"] == "group_key" and kinds["product_type"] == "aggregate"
    assert _check(report, "grain_row_count")["status"] == "pass"
    assert _check(report, "grain_key_mapped")["status"] == "pass"

    tr, va, rc = _downstream(api, cs)
    rows = _rows(tr["csv"])
    exp = list(csv.DictReader(open(FIX / "expected_scenario_b.csv", encoding="utf-8")))
    assert len(rows) == len(exp) == 30
    assert [r["policy_reference"] for r in rows] == sorted(r["policy_reference"] for r in rows)
    got = {r["policy_reference"]: r for r in rows}
    for e in exp:
        for a in ("product_type", "active_role_count", "beneficiary_count", "total_monthly_premium"):
            assert _num(got[e["policy_reference"]][a]) == _num(e[a]), (a, e["policy_reference"])
    assert tr["stats"]["grain"] == ["POLREF"] and tr["stats"]["source_rows"] == 199
    assert tr["stats"]["target_rows"] == 30

    assert va["verdict"] == "certified", [c for c in va["checks"] if c["status"] == "fail"]
    for n in ("grain_row_count", "missing_group", "unexpected_group", "key_unique_not_null"):
        assert _check(va, n)["status"] == "pass", n
    assert rc["verdict"] == "certified", [c for c in rc["checks"] if c["status"] == "fail"]
    assert _check(rc, "control_total:groups")["status"] == "pass"
    assert _check(rc, "control_total:keys")["status"] == "pass"
    for a in ("active_role_count", "beneficiary_count", "total_monthly_premium"):
        assert _check(rc, f"control_total:conservation:{a}")["status"] == "pass", a
    assert _check(rc, "control_total:rows") is None             # rows â‰  groups: not proposed

    # a delivered file missing a group is caught by name
    lines = tr["csv"].strip("\r\n").splitlines()
    va2 = api["client"].post("/api/validate/run",
                             json={"spec": cs, "csv": "\n".join(lines[:-1]) + "\n"}).json()
    assert _check(va2, "missing_group")["status"] == "fail" and va2["verdict"] == "blocked"
    api["b_spec"] = cs


# ---------------------------------------------------------------- AC-09..11
def test_grain_rejections(api, scen_b):
    spec, dec = scen_b
    _run(api, *POLICY, FIX / "policy_summary.json")
    base = dict(dec, __grain__={"action": "set_grain", "group_by": ["POLREF"]},
                **{a: {"action": "edit", "transformation_sql": s} for a, s in GRAIN_SQL.items()},
                product_type={"action": "edit", "transformation_sql": PRODUCT_ANY})
    # AC-09: a column that varies within a group cannot be certified as-is
    bad = dict(base, product_type={"action": "edit", "transformation_sql": 'trim("ROLREF")'})
    _cs, verdict, report, _q = _certify(api, spec, bad)
    assert verdict == "blocked"
    assert "different values within a POLREF group" in _check(report, "transform_kind:product_type")["detail"]
    # AC-11: no attribute carries the group key
    nokey = dict(base, policy_reference={"action": "edit", "transformation_sql": "'X'"})
    _cs, verdict, report, _q = _certify(api, spec, nokey)
    assert verdict == "blocked" and _check(report, "grain_key_mapped")["status"] == "fail"
    # an unknown group-by column is kept with its error and blocks
    _cs, verdict, report, _q = _certify(api, spec, dict(base, __grain__={"action": "set_grain",
                                                                         "group_by": ["NOPE"]}))
    assert verdict == "blocked" and _cs["grain"]["errors"]
    assert _check(report, "grain_declared_valid")["status"] == "fail"
    # AC-10: grain cleared again -> the aggregates are re-queued
    ok, verdict, _r, _q = _certify(api, spec, base)
    assert verdict == "certified"
    cleared, verdict, report, queue = _certify(api, ok, {"__grain__": {"action": "clear_grain"}})
    assert cleared["grain"] is None and verdict == "blocked"
    assert "set a target grain" in _check(report, "transform_kind:active_role_count")["detail"]
    assert any(i["target_attribute"] == "active_role_count" for i in queue["items"])


# ---------------------------------------------------------------- filter + grain
def test_row_filter_applies_before_grouping(api, scen_b):
    spec, dec = scen_b
    _run(api, *POLICY, FIX / "policy_summary.json")
    d = dict(dec, __grain__={"action": "set_grain", "group_by": ["POLREF"]},
             __row_filter__={"action": "set_filter", "filter": {"root": {"logic": "AND", "conditions": [
                 {"field": "ROLREF", "op": "in", "values": ["PH", "PY"]}]}}},
             **{a: {"action": "edit", "transformation_sql": s} for a, s in GRAIN_SQL.items()},
             product_type={"action": "edit", "transformation_sql": PRODUCT_ANY})
    cs, verdict, _r, _q = _certify(api, spec, d)
    assert verdict == "certified"
    tr, va, rc = _downstream(api, cs)
    roles = list(csv.DictReader(open(FIX / "CO040.csv", encoding="utf-8")))
    kept = [r for r in roles if r["ROLREF"] in ("PH", "PY")]
    per = defaultdict(int)
    for r in kept:
        per[r["POLREF"]] += r["STAXRF"] == "30"
    got = {r["policy_reference"]: int(r["active_role_count"]) for r in _rows(tr["csv"])}
    assert got == dict(per)                                     # counts over the kept rows only
    assert tr["stats"]["source_rows"] == len(kept)
    assert va["verdict"] == "certified" and rc["verdict"] == "certified"


# ---------------------------------------------------------------- previews
def test_preview_endpoints(api, scen_b):
    spec, _dec = scen_b
    _run(api, *POLICY, FIX / "policy_summary.json")
    c = api["client"]
    g = c.post("/api/mapping/grain-preview", json={"spec": spec, "group_by": ["POLREF"]}).json()
    assert (g["source_rows"], g["groups"], g["max_group_size"]) == (199, 30, 11)
    assert c.post("/api/mapping/grain-preview", json={"spec": spec, "group_by": ["NOPE"]}).json()["error"]
    p = c.post("/api/mapping/preview-expression", json={
        "spec": spec, "expression": GRAIN_SQL["beneficiary_count"], "group_by": ["POLREF"]}).json()
    assert p["kind"] == "aggregate" and p["error_code"] is None and len(p["sample"]) == 10
    assert set(p["sample"][0]) == {"value", "group: POLREF"}
    p = c.post("/api/mapping/preview-expression", json={
        "spec": spec, "expression": '"PRDREF"', "group_by": ["POLREF"]}).json()
    assert p["error_code"] == "I-EXPR-ANY-VALUE" and p["suggested_sql"] == 'ANY_VALUE("PRDREF")'
    p = c.post("/api/mapping/preview-expression", json={
        "spec": spec, "expression": WINDOW_SQL["policy_beneficiary_count"]}).json()
    assert p["kind"] == "window" and p["partition_by"] == ["POLREF"]
    p = c.post("/api/mapping/preview-expression", json={"spec": spec, "expression": "(SELECT 1)"}).json()
    assert p["error_code"] == "E-EXPR-SUBQUERY" and p["sample"] == []


# ---------------------------------------------------------------- AC-12
def test_scripts_are_deterministic_and_reproduce_the_app(api):
    cs = api.get("b_spec")
    if cs is None:
        pytest.skip("depends on test_declared_grain_flows_through_every_workspace")
    _run(api, *POLICY, FIX / "policy_summary.json")
    c = api["client"]
    c.post("/api/reconcile/rules", json={"spec": cs})
    c.post("/api/reconcile/certify", json={"spec": cs, "decisions": {}, "added": []})
    gens = {}
    for name, ep in (("etl.py", "/api/transform/codegen"), ("validate.py", "/api/validate/codegen"),
                     ("reconcile.py", "/api/reconcile/codegen")):
        outs = {c.post(ep, json={"spec": cs}).json()["code"] for _ in range(5)}
        assert len(outs) == 1, f"{name} is not deterministic"
        gens[name] = outs.pop()
    assert "# Target grain : one row per POLREF" in gens["etl.py"]
    assert 'GROUP BY "POLREF"' in gens["etl.py"] and 'GROUP BY "POLREF"' in gens["reconcile.py"]
    tr = c.post("/api/transform/run", json={"spec": cs}).json()
    rc = c.post("/api/reconcile/run", json={"spec": cs, "csv": tr["csv"]}).json()
    work = api["tmp"].mktemp("gb_scripts")
    for f in ("CO040.csv", "PO010.csv"):
        shutil.copy(FIX / f, work / f)
    for name, code in gens.items():
        (work / name).write_text(code, encoding="utf-8")

    def run(script):
        return subprocess.run([sys.executable, "-I", "-X", "utf8", script], cwd=work,
                              capture_output=True, text=True, encoding="utf-8", timeout=300)
    target = cs["target_table"] + ".csv"
    try:
        import pandas  # noqa: F401
        out = run("etl.py")
        assert out.returncode == 0, out.stderr
        a = [r for r in csv.reader(open(work / target, encoding="utf-8", newline="")) if r]
        b = [r for r in csv.reader(io.StringIO(tr["csv"])) if r]
        assert a == b
    except ImportError:
        (work / target).write_text(tr["csv"], encoding="utf-8", newline="")
    out = run("reconcile.py")
    assert out.returncode == 0, out.stdout + out.stderr
    script = json.loads((work / "reconciliation_report.json").read_text(encoding="utf-8"))
    assert sorted(x["name"] for x in script["checks"]) == sorted(x["name"] for x in rc["checks"])
    assert all(x["status"] in ("pass",) for x in script["checks"]
               if x["name"].startswith("control_total")), script["checks"]
    assert run("validate.py").returncode == 0


# ---------------------------------------------------------------- AC-14: any data
def test_generic_on_an_unrelated_hostile_dataset(api):
    """Claims data with spaces and unicode in column names, a single file, a
    different target: a window per claim type, then a grain on claim type.
    Nothing in the feature knows about policies."""
    tmp = api["tmp"].mktemp("claims")
    td = json.loads((HOSTILE / "target_claim.json").read_text(encoding="utf-8"))
    td["attributes"].append({"name": "type_total_paid", "type": "decimal", "nullable": True,
                             "description": "Total paid for all claims of the same type."})
    (tmp / "target_claim_window.json").write_text(json.dumps(td), encoding="utf-8")
    summary = {"table": "claim_type_summary", "attributes": [
        {"name": "claim_type", "type": "string", "description": "Kind of claim."},
        {"name": "claims", "type": "integer", "description": "Number of claims of this type."},
        {"name": "total_paid", "type": "decimal", "description": "Total settled amount."},
        {"name": "assessors", "type": "integer", "description": "Distinct assessors."}]}
    (tmp / "claim_type_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    src, dic = [HOSTILE / "CLAIMS99.csv"], [HOSTILE / "dict_CLAIMS99.json"]
    rows = list(csv.DictReader(open(HOSTILE / "CLAIMS99.csv", encoding="utf-8")))
    per_type = defaultdict(lambda: [0, Decimal(0), set()])
    for r in rows:
        t = per_type[r["CLAIM TYPE"]]
        t[0] += 1
        t[1] += Decimal(r["AMT PAID"])
        t[2].add(r["ASSESSOR"])

    # window: a per-type total on every claim
    spec, queue = _run(api, src, dic, tmp / "target_claim_window.json")
    win = 'SUM(TRY_CAST("AMT PAID" AS DECIMAL(14,2))) OVER (PARTITION BY "CLAIM TYPE")'
    cs, verdict, _r, _q = _certify(api, spec, dict(_accept_all(queue),
                                                   type_total_paid={"action": "edit", "transformation_sql": win}))
    assert verdict != "blocked", [c for c in _r["checks"] if c["status"] == "fail"]
    tr, va, rc = _downstream(api, cs)
    assert "error" not in va and _check(va, "partition_consistency:type_total_paid")["status"] == "pass"
    assert _check(va, "transform_rule:type_total_paid")["status"] == "pass"
    ref = next(m["transformation_sql"] for m in cs["mappings"] if m["target_attribute"] == "claim_reference")
    assert ref                                                  # rows identifiable
    got = _rows(tr["csv"])
    assert len(got) == len(rows)
    for r_src, r_out in zip(rows, got):                         # source order
        assert _num(r_out["type_total_paid"]) == _num(per_type[r_src["CLAIM TYPE"]][1])

    # grain: one row per claim type
    spec, queue = _run(api, src, dic, tmp / "claim_type_summary.json")
    d = dict(_accept_all(queue), __grain__={"action": "set_grain", "group_by": ["CLAIM TYPE"]},
             claim_type={"action": "edit", "transformation_sql": 'trim("CLAIM TYPE")'},
             claims={"action": "edit", "transformation_sql": "COUNT(*)"},
             total_paid={"action": "edit", "transformation_sql": 'SUM(TRY_CAST("AMT PAID" AS DECIMAL(14,2)))'},
             assessors={"action": "edit", "transformation_sql": 'COUNT(DISTINCT "ASSESSOR")'})
    cs, verdict, report, _q = _certify(api, spec, d)
    assert verdict == "certified", [c for c in report["checks"] if c["status"] == "fail"]
    tr, va, rc = _downstream(api, cs)
    out = {r["claim_type"]: r for r in _rows(tr["csv"])}
    assert set(out) == set(per_type)
    for t, (n, total, assessors) in per_type.items():
        assert int(out[t]["claims"]) == n and _num(out[t]["total_paid"]) == _num(total)
        assert int(out[t]["assessors"]) == len(assessors)
    assert va["verdict"] == "certified", [c for c in va["checks"] if c["status"] == "fail"]
    assert rc["verdict"] == "certified", [c for c in rc["checks"] if c["status"] == "fail"]
    assert _check(rc, "control_total:conservation:total_paid")["status"] == "pass"
    assert _check(rc, "control_total:conservation:claims")["status"] == "pass"
