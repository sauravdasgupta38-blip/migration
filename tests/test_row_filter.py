"""Reviewer-defined source row filter — compiler, review decision, and every
workspace end to end (certify -> transform -> validate -> reconcile -> scripts).

Runs offline: the LLM tiers are switched off for this module so results are
deterministic, and the server's upload area is redirected to a temp folder so a
running UI's uploads are never touched.
"""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from engine.agents.contracts import MappingEntry, MappingSpec
from engine.agents.reviewer import apply_decisions
from engine.row_filter import (RowFilterError, compile_row_filter,
                               describe_row_filter, normalise_row_filter,
                               type_family)

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "row_filter"
HOSTILE = ROOT / "tests" / "fixtures"


def C(field, op, value=None, values=None, t=""):
    c = {"field": field, "op": op, "field_type": t}
    if value is not None:
        c["value"] = value
    if values is not None:
        c["values"] = values
    return c


def G(logic, *conds):
    return {"logic": logic, "conditions": list(conds)}


def F(*conds, logic="AND"):
    return {"root": G(logic, *conds)}


# =========================================================== compiler (unit)
COLS = {"T": {"inferred_type": "FREE_TEXT"}, "N": {"inferred_type": "DECIMAL"},
        "D": {"inferred_type": "DATE_YYYYMMDD"}}


@pytest.fixture(scope="module")
def small():
    con = duckdb.connect()
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        ('a',       '5',    '20200101'),
        (' b ',     '12.5', '00000000'),
        ('',        'abc',  '20211231'),
        (NULL,      '',     ''),
        ('O''Brien', NULL,  'garbage')) v(T, N, D)""")
    yield con
    con.close()


def kept(con, rf, cols=COLS):
    n = normalise_row_filter(rf, cols)
    assert n["errors"] == [], n["errors"]
    return con.execute(f"SELECT count(*) FROM t WHERE {n['compiled_sql']}").fetchone()[0]


@pytest.mark.parametrize("cond,expected", [
    # text: values are trimmed; empty = NULL or blank
    (C("T", "is_empty"), 2), (C("T", "is_not_empty"), 3),
    (C("T", "eq", "b"), 1),
    (C("T", "ne", "a"), 4),                     # blank rows count as "not equal"
    (C("T", "in", values=["a", "b"]), 2), (C("T", "not_in", values=["a"]), 4),
    (C("T", "contains", "Bri"), 1), (C("T", "starts_with", "O'"), 1), (C("T", "ends_with", "b"), 1),
    # numbers: TRY_CAST, so 'abc' compares as unknown, never as a match
    (C("N", "is_empty"), 2), (C("N", "gt", "5"), 1), (C("N", "ge", "5"), 2),
    (C("N", "lt", "12.5"), 1), (C("N", "le", "5"), 1), (C("N", "between", values=["5", "12.5"]), 2),
    (C("N", "eq", "5.0"), 1), (C("N", "ne", "5"), 3),
    # dates: YYYYMMDD text, 00000000 = no date, garbage never matches a range
    (C("D", "is_empty"), 2), (C("D", "gt", "2020-06-30"), 1),
    (C("D", "between", values=["2020-01-01", "2020-12-31"]), 1),
    (C("D", "eq", "20200101"), 1), (C("D", "in", values=["2020-01-01", "2021-12-31"]), 2),
    (C("D", "lt", "2021-01-01"), 1),
])
def test_each_operator_selects_the_right_rows(small, cond, expected):
    assert kept(small, F(cond)) == expected


def test_compiled_sql_is_type_aware():
    sql = compile_row_filter(normalise_row_filter(F(C("D", "is_empty"), C("N", "gt", "5")), COLS), COLS)
    assert "trim(\"D\") IN ('', '00000000')" in sql
    assert "TRY_CAST(trim(\"N\") AS DECIMAL(38,10)) > CAST('5' AS DECIMAL(38,10))" in sql
    sql = compile_row_filter(normalise_row_filter(F(C("D", "between", values=["20200101", "2020-12-31"])), COLS), COLS)
    assert "BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'" in sql


@pytest.mark.parametrize("cond,needle", [
    (C("NOPE", "eq", "1"), "is not a field"),
    (C("T", "like", "x"), "unknown condition"),
    (C("T", "gt", "1"), "cannot be used on a text field"),
    (C("N", "contains", "1"), "cannot be used on a number field"),
    (C("N", "eq", "abc"), "is not a number"),
    (C("D", "eq", "2020-13-01"), "is not a valid date"),
    (C("D", "eq", "yesterday"), "is not a date"),
    (C("N", "between", values=["1"]), "exactly two values"),
    (C("T", "in", values=[]), "at least one value"),
    (C("T", "eq", ""), "use 'is empty'"),
    (C("T", "eq", "a\nb"), "control characters"),
])
def test_invalid_conditions_are_rejected_with_a_reason(cond, needle):
    errs = normalise_row_filter(F(cond), COLS)["errors"]
    assert errs and needle in errs[0], errs
    with pytest.raises(RowFilterError):
        compile_row_filter(F(dict(cond, field_type=COLS.get(cond["field"], {}).get("inferred_type", ""))), COLS)


def test_structural_limits():
    too_many = F(*[C("T", "is_empty")] * 21)
    assert any("at most 20" in e for e in normalise_row_filter(too_many, COLS)["errors"])
    assert normalise_row_filter(F(G("AND")), COLS)["errors"] == ["a condition group is empty."]
    assert "unknown combinator" in normalise_row_filter(F(C("T", "is_empty"), logic="XOR"), COLS)["errors"][0]
    deep = C("T", "is_empty")
    for _ in range(5):
        deep = G("AND", deep)
    assert any("nested" in e for e in normalise_row_filter(F(deep), COLS)["errors"])
    assert normalise_row_filter(F(), COLS) is None           # empty filter = no filter


def test_values_cannot_inject_sql(small):
    rf = F(C("T", "eq", "O'Brien'; DROP TABLE t; --"))
    assert kept(small, rf) == 0
    assert small.execute("SELECT count(*) FROM t").fetchone()[0] == 5   # table intact
    assert kept(small, F(C("T", "eq", "O'Brien"))) == 1
    # a field name is only ever a real column, so it cannot carry SQL either
    errs = normalise_row_filter(F(C('T"; DROP TABLE t; --', "is_empty")), COLS)["errors"]
    assert errs and "is not a field" in errs[0]


def test_bracket_precedence_is_respected(small):
    a, b, c = C("T", "is_empty"), C("N", "is_empty"), C("D", "eq", "2020-01-01")
    assert kept(small, F(a, G("OR", b, c))) == 1               # A AND (B OR C)
    assert kept(small, F(G("AND", a, b), c, logic="OR")) == 2  # (A AND B) OR C


def test_description_is_plain_and_single_line():
    rf = normalise_row_filter(F(C("D", "is_empty"), G("OR", C("T", "eq", "PH"), C("N", "gt", "100"))), COLS)
    assert rf["description"] == "D is empty AND (T equals 'PH' OR N is greater than 100)"
    assert describe_row_filter(rf) == rf["description"]
    assert "\n" not in describe_row_filter(F(C("T", "eq", "x\ny")))


def test_dictionary_decides_the_type_not_the_client():
    rf = normalise_row_filter(F(C("D", "eq", "20200101", t="FREE_TEXT")), COLS)
    cond = rf["root"]["conditions"][0]
    assert cond["field_type"] == "DATE_YYYYMMDD" and cond["value"] == "2020-01-01"
    assert type_family("DATE_YYYYMMDD") == "date" and type_family("AMOUNT") == "number"
    assert type_family("CATEGORICAL_CODE") == "text" and type_family("") == "text"


def test_hostile_column_names_are_quoted_everywhere():
    """Spaces, unicode and apostrophes in names and values (tests/fixtures/CLAIMS99)."""
    d = json.loads((HOSTILE / "dict_CLAIMS99.json").read_text(encoding="utf-8"))
    cols = {c["name"]: c for c in d["columns"]}
    uni = next(n for n in cols if n not in ("CLM REF", "CLAIM TYPE", "AMT PAID", "SETTLE DT",
                                             "ASSESSOR", "NOTES"))
    greek = next(iter(cols[uni]["value_decode"]))
    rf = normalise_row_filter(F(C("CLAIM TYPE", "in", values=["DTH", "TPD"]),
                                C("AMT PAID", "gt", "20000"),
                                G("OR", C(uni, "eq", greek), C("SETTLE DT", "ge", "2024-06-01"))), cols)
    assert rf["errors"] == []
    con = duckdb.connect()
    con.execute("CREATE TABLE c AS SELECT * FROM read_csv_auto(?, all_varchar=true, header=true)",
                [str(HOSTILE / "CLAIMS99.csv")])
    got = con.execute(f"SELECT count(*) FROM c WHERE {rf['compiled_sql']}").fetchone()[0]
    rows = list(csv.DictReader(open(HOSTILE / "CLAIMS99.csv", encoding="utf-8")))
    exp = sum(1 for r in rows if r["CLAIM TYPE"].strip() in ("DTH", "TPD")
              and float(r["AMT PAID"]) > 20000
              and (r[uni].strip() == greek or r["SETTLE DT"].strip() >= "20240601"))
    assert got == exp and got > 0


# =========================================================== review decision
def _spec():
    return MappingSpec(source_table="t", target_table="x", mappings=[
        MappingEntry(target_attribute="a", cardinality="1:1", transformation_sql="1", gate="review")])


def test_apply_decisions_sets_keeps_and_clears_the_filter():
    s = apply_decisions(_spec(), {"__row_filter__": {"action": "set_filter", "filter": F(C("D", "is_empty"))},
                                  "a": {"action": "accept"}}, None, COLS)
    assert s.row_filter.description == "D is empty" and s.row_filter.errors == []
    assert s.row_filter.compiled_sql and s.row_filter.declared_at
    assert s.mappings[0].gate == "auto_accept"            # attribute decisions still apply
    s = apply_decisions(s, {}, None, COLS)                 # no key: keep what is certified
    assert s.row_filter.description == "D is empty"
    s = apply_decisions(s, {"__row_filter__": {"action": "clear_filter"}}, None, COLS)
    assert s.row_filter is None


def test_invalid_or_malformed_filters_are_kept_with_their_errors():
    s = apply_decisions(_spec(), {"__row_filter__": {"action": "set_filter",
                                                     "filter": F(C("N", "gt", "abc"))}}, None, COLS)
    assert s.row_filter.errors == ["'N': 'abc' is not a number."]
    s = apply_decisions(_spec(), {"__row_filter__": {"action": "set_filter",
                                                     "filter": {"root": "nonsense"}}}, None, COLS)
    assert s.row_filter is None or s.row_filter.errors
    s = apply_decisions(_spec(), {"__row_filter__": {"action": "set_filter",
                                                     "filter": {"root": {"logic": "AND", "conditions": [{"x": 1}]}}}},
                        None, COLS)
    assert s.row_filter.errors and "is not a field" in s.row_filter.errors[0]
    s = apply_decisions(_spec(), {"__row_filter__": {"action": "set_filter",
                                                     "filter": F(C("T", "in", values=5))}}, None, COLS)
    assert s.row_filter.errors and "could not be read" in s.row_filter.errors[0]


# =========================================================== API end to end
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
def app(tmp_path_factory):
    """Run the mapping once (offline) and certify it with no filter: every test
    then certifies THAT spec with only a row-filter decision, the way the UI's
    Amend flow does."""
    from fastapi.testclient import TestClient

    from api import server

    env_keys = ("OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY")
    env_saved = {k: os.environ.get(k) for k in env_keys}
    for k in env_keys:
        os.environ[k] = ""                       # deterministic, no network
    saved = {k: getattr(server, k) for k in ("UPLOAD", "WH_DB", "STATE")}
    up = tmp_path_factory.mktemp("rf_uploads")
    server.UPLOAD, server.WH_DB = up, str(up / "warehouse.duckdb")
    server.STATE = server._empty_state()
    server._SOURCE_FIELDS.update(signature=None, columns=None)
    client = TestClient(server.app)
    try:
        client.post("/api/inputs/reset")
        for role, files in (("source", ["CO040.csv", "PO010.csv"]),
                            ("enriched", ["dict_CO040.json", "dict_PO010.json"]),
                            ("target", ["policy_client_role.json"])):
            client.post("/api/inputs/upload", data={"role": role},
                        files=[("files", (f, (FIX / f).read_bytes())) for f in files])
        evs = _sse(client.post("/api/mapping/run", json={}).text)
        assert not [e for e in evs if e["type"] == "error"], evs
        queue = next(e["artifact"] for e in evs if e.get("node") == "review")
        spec = next(e for e in evs if e["type"] == "complete")["final_spec"]
        fields = next(e for e in evs if e["type"] == "complete")["source_fields"]
        evs = _sse(client.post("/api/mapping/certify",
                               json={"spec": spec, "decisions": _accept_all(queue)}).text)
        done = next(e for e in evs if e["type"] == "complete")
        base = done["final_spec"]
        tr = client.post("/api/transform/run", json={"spec": base}).json()
        va = client.post("/api/validate/run", json={"spec": base, "csv": tr["csv"]}).json()
        yield {"client": client, "server": server, "base": base, "fields": fields,
               "base_verdict": done["summary"]["verdict"], "base_rows": tr["stats"]["row_count"],
               "base_validate": va["verdict"], "tmp": tmp_path_factory.mktemp("rf_scripts")}
    finally:
        for k, v in saved.items():
            setattr(server, k, v)
        server._SOURCE_FIELDS.update(signature=None, columns=None)
        for k, v in env_saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _certify(app, spec, rf_decision):
    evs = _sse(app["client"].post("/api/mapping/certify",
                                  json={"spec": spec, "decisions": {"__row_filter__": rf_decision}}).text)
    assert not [e for e in evs if e["type"] == "error"], evs
    done = next(e for e in evs if e["type"] == "complete")
    report = next(e["artifact"] for e in evs if e.get("node") == "validation")
    return done["final_spec"], done["summary"]["verdict"], report


def _check(report, name):
    return next((c for c in report["checks"] if c["name"] == name), None)


def _workset_rows():
    """CO040 LEFT JOIN PO010 on POLREF, computed in plain Python — the expected
    population is not derived from the SQL under test."""
    pol = {r["POLREF"].strip(): r for r in csv.DictReader(open(FIX / "PO010.csv", encoding="utf-8"))}
    out = []
    for r in csv.DictReader(open(FIX / "CO040.csv", encoding="utf-8")):
        p = pol.get(r["POLREF"].strip(), {})
        out.append(dict(r, STAPOL=p.get("STAPOL", ""), EXTMOD=p.get("EXTMOD", "")))
    return out


def _empty_date(v):
    return v.strip() in ("", "00000000")


def _num(v):
    try:
        return float(v)
    except ValueError:
        return None


CASES = [
    ("enddte_empty", F(C("ENDDTE", "is_empty")), 158, lambda r: _empty_date(r["ENDDTE"])),
    ("enddte_empty_and_active", F(C("ENDDTE", "is_empty"), C("STAXRF", "eq", "30")), 152,
     lambda r: _empty_date(r["ENDDTE"]) and r["STAXRF"].strip() == "30"),
    ("joined_field_and_bracket",
     F(C("STAPOL", "in", values=["20", "25"]), G("OR", C("ROLREF", "eq", "PH"), C("ROLREF", "eq", "PY"))), 17,
     lambda r: r["STAPOL"].strip() in ("20", "25") and r["ROLREF"].strip() in ("PH", "PY")),
    ("policy_not_exited", F(C("EXTMOD", "eq", "0")), 86, lambda r: r["EXTMOD"].strip() == "0"),
    ("no_contribution", F(C("CTBAMT", "is_empty")), 173, lambda r: not r["CTBAMT"].strip()),
    ("started_2020_or_later", F(C("STRDTE", "ge", "2020-01-01")), 38,
     lambda r: not _empty_date(r["STRDTE"]) and r["STRDTE"].strip() >= "20200101"),
    ("contribution_over_100", F(C("CTBAMT", "gt", "100")), 5,
     lambda r: (_num(r["CTBAMT"]) or 0) > 100),
]


@pytest.mark.parametrize("name,rf,expected,pred", CASES, ids=[c[0] for c in CASES])
def test_filter_flows_through_every_workspace(app, name, rf, expected, pred):
    c = app["client"]
    rows = _workset_rows()
    assert sum(1 for r in rows if pred(r)) == expected          # the oracle agrees
    spec, verdict, report = _certify(app, app["base"], {"action": "set_filter", "filter": rf})
    assert verdict == app["base_verdict"]
    assert spec["row_filter"]["kept_rows"] == expected and spec["row_filter"]["source_rows"] == len(rows)
    eff = _check(report, "row_filter_effect")
    assert eff["status"] == "pass" and eff["sql"] and f"kept {expected} of {len(rows)}" in eff["detail"]
    assert _check(report, "row_count_preserved")["status"] == "pass"

    tr = c.post("/api/transform/run", json={"spec": spec}).json()
    assert tr["stats"]["row_count"] == expected
    assert tr["stats"]["rows_excluded_by_filter"] == len(rows) - expected
    delivered = {r["role_record_id"].strip() for r in csv.DictReader(io.StringIO(tr["csv"]))}
    assert delivered == {r["UNIQUE_ID"].strip() for r in rows if pred(r)}   # the RIGHT rows

    va = c.post("/api/validate/run", json={"spec": spec, "csv": tr["csv"]}).json()
    assert va["verdict"] == app["base_validate"]
    assert _check(va, "row_count_preserved")["status"] == "pass"
    absent = _check(va, "row_filter_excluded_absent")
    assert absent["status"] == "pass" and f"none of the {len(rows) - expected}" in absent["detail"]

    rr = c.post("/api/reconcile/rules", json={"spec": spec}).json()
    assert "control_total:row_filter" in [r["kind"] for r in rr["rules"]]
    c.post("/api/reconcile/certify", json={"spec": spec, "decisions": {}, "added": []})
    rc = c.post("/api/reconcile/run", json={"spec": spec, "csv": tr["csv"]}).json()
    assert _check(rc, "control_total:rows")["status"] == "pass"
    rf_chk = _check(rc, "control_total:row_filter")
    assert f"{len(rows)} source row(s) = {expected} in scope + {len(rows) - expected} excluded" in rf_chk["detail"]

    code = c.post("/api/transform/codegen", json={"spec": spec}).json()["code"]
    assert "# Row filter (certified by reviewer): " + spec["row_filter"]["description"] in code
    # (indented: the filtered source sits inside the target statement)
    assert any(ln.lstrip().startswith("WHERE (") for ln in code.splitlines())


def test_filter_that_keeps_nothing_blocks_certification(app):
    spec, verdict, report = _certify(app, app["base"], {"action": "set_filter",
                                                        "filter": F(C("UNIQUE_ID", "eq", "does-not-exist"))})
    assert verdict == "blocked"
    assert _check(report, "row_filter_effect")["status"] == "fail"


def test_invalid_filter_blocks_certification_and_cannot_run(app):
    spec, verdict, report = _certify(app, app["base"], {"action": "set_filter",
                                                        "filter": F(C("CTBAMT", "gt", "abc"))})
    assert verdict == "blocked"
    assert "'CTBAMT': 'abc' is not a number." in _check(report, "row_filter_valid")["detail"]
    assert spec["row_filter"]["errors"]                         # kept visible, not dropped
    tr = app["client"].post("/api/transform/run", json={"spec": spec}).json()
    assert "error" in tr and "row filter is invalid" in tr["error"]
    assert "error" in app["client"].post("/api/transform/codegen", json={"spec": spec}).json()


def test_keep_and_clear_through_certify(app):
    spec, _, _ = _certify(app, app["base"], {"action": "set_filter", "filter": F(C("ENDDTE", "is_empty"))})
    evs = _sse(app["client"].post("/api/mapping/certify", json={"spec": spec, "decisions": {}}).text)
    kept_spec = next(e for e in evs if e["type"] == "complete")["final_spec"]
    assert kept_spec["row_filter"]["kept_rows"] == 158           # no key: filter kept
    cleared, verdict, _ = _certify(app, kept_spec, {"action": "clear_filter"})
    assert cleared["row_filter"] is None and verdict == app["base_verdict"]
    tr = app["client"].post("/api/transform/run", json={"spec": cleared}).json()
    assert tr["stats"]["row_count"] == app["base_rows"] and tr["stats"]["row_filter"] is None


def test_client_supplied_sql_and_types_are_not_trusted(app):
    # a tampered field_type is overwritten from the dictionary at certify
    rf = F(C("ENDDTE", "is_empty", t="FREE_TEXT"))
    spec, _, _ = _certify(app, app["base"], {"action": "set_filter", "filter": rf})
    assert spec["row_filter"]["root"]["conditions"][0]["field_type"] == "DATE_YYYYMMDD"
    assert spec["row_filter"]["kept_rows"] == 158
    # a tampered compiled_sql is never executed: workspaces recompile the structure
    spec["row_filter"]["compiled_sql"] = "1=1"
    tr = app["client"].post("/api/transform/run", json={"spec": spec}).json()
    assert tr["stats"]["row_count"] == 158


def test_source_fields_and_preview_endpoints(app):
    c, base = app["client"], app["base"]
    names = {f["name"]: f for f in app["fields"]}
    assert names["ENDDTE"]["type_family"] == "date" and names["STAPOL"]["origin_table"] == "PO010"
    assert names["ROLREF"]["value_decode"]["PH"] == "Policy Holder"
    rf = F(C("ENDDTE", "is_empty"), G("OR", C("ROLREF", "eq", "PH"), C("ROLREF", "eq", "PY")))
    p = c.post("/api/mapping/filter-preview", json={"spec": base, "filter": rf}).json()
    assert (p["source_rows"], p["kept_rows"], p["excluded_rows"]) == (199, 44, 155)
    assert p["description"] == "ENDDTE is empty AND (ROLREF equals 'PH' OR ROLREF equals 'PY')"
    assert 0 < len(p["sample_excluded"]) <= 5 and set(p["sample_excluded"][0]) == {"ENDDTE", "ROLREF"}
    bad = c.post("/api/mapping/filter-preview", json={"spec": base, "filter": F(C("NOPE", "eq", "1"))}).json()
    assert bad["errors"] == ["'NOPE' is not a field of the source data."]
    none = c.post("/api/mapping/filter-preview", json={"spec": base, "filter": F()}).json()
    assert none["kept_rows"] == none["source_rows"] == 199
    v = c.post("/api/mapping/field-values", json={"spec": base, "field": "ROLREF"}).json()
    assert sum(x["count"] for x in v["values"]) == 199 and v["values"][0]["count"] >= v["values"][-1]["count"]
    assert "error" in c.post("/api/mapping/field-values", json={"spec": base, "field": "x\"; --"}).json()
    assert "error" in c.post("/api/mapping/filter-preview",
                             json={"spec": {"source_table": "no_such"}, "filter": rf}).json()


def test_no_filter_generates_byte_identical_scripts(app):
    """A spec without a filter (key absent, or None) generates exactly what it
    generated before the feature existed: no extra lines, no WHERE."""
    c, base = app["client"], dict(app["base"])
    assert base.get("row_filter") is None
    without = {k: v for k, v in base.items() if k != "row_filter"}
    for ep in ("/api/transform/codegen", "/api/validate/codegen"):
        a = c.post(ep, json={"spec": base}).json()["code"]
        b = c.post(ep, json={"spec": without}).json()["code"]
        assert a == b and "Row filter" not in a
    c.post("/api/reconcile/rules", json={"spec": base})
    c.post("/api/reconcile/certify", json={"spec": base, "decisions": {}, "added": []})
    r = c.post("/api/reconcile/codegen", json={"spec": base}).json()["code"]
    # (the script always rebuilds the source as `source_all` / `source` — the
    # GROUP BY feature needs it — but with no filter, nothing is filtered)
    assert "Row filter" not in r and "\nWHERE " not in r.split("CREATE OR REPLACE TABLE expected")[0]


def test_generated_scripts_are_deterministic_and_reproduce_the_app(app):
    c = app["client"]
    rf = F(C("ENDDTE", "is_empty"), G("OR", C("ROLREF", "eq", "PH"), C("ROLREF", "eq", "PY")))
    spec, _, _ = _certify(app, app["base"], {"action": "set_filter", "filter": rf})
    c.post("/api/reconcile/rules", json={"spec": spec})
    c.post("/api/reconcile/certify", json={"spec": spec, "decisions": {}, "added": []})
    gens = {}
    for name, ep in (("etl.py", "/api/transform/codegen"), ("validate.py", "/api/validate/codegen"),
                     ("reconcile.py", "/api/reconcile/codegen")):
        outs = {c.post(ep, json={"spec": spec}).json()["code"] for _ in range(5)}
        assert len(outs) == 1, f"{name} is not deterministic"
        gens[name] = outs.pop()

    tr = c.post("/api/transform/run", json={"spec": spec}).json()
    rc = c.post("/api/reconcile/run", json={"spec": spec, "csv": tr["csv"]}).json()
    work = app["tmp"]
    for f in ("CO040.csv", "PO010.csv"):
        shutil.copy(FIX / f, work / f)
    for name, code in gens.items():
        (work / name).write_text(code, encoding="utf-8")
    def run(script):
        # -I ignores PYTHON* env vars, so UTF-8 output is requested with -X utf8
        return subprocess.run([sys.executable, "-I", "-X", "utf8", script], cwd=work,
                              capture_output=True, text=True, encoding="utf-8", timeout=300)

    try:
        import pandas  # noqa: F401 — the generated ETL script reads with pandas
        have_pandas = True
    except ImportError:
        have_pandas = False
    target = spec["target_table"] + ".csv"
    if have_pandas:
        out = run("etl.py")
        assert out.returncode == 0, out.stderr
        assert "Wrote 44 rows" in out.stdout
        a = [r for r in csv.reader(open(work / target, encoding="utf-8", newline="")) if r]
        b = [r for r in csv.reader(io.StringIO(tr["csv"])) if r]
        assert a == b                                            # same rows, same values, same order
    else:
        (work / target).write_text(tr["csv"], encoding="utf-8", newline="")
    out = run("reconcile.py")
    assert out.returncode == 0, out.stdout + out.stderr
    script = json.loads((work / "reconciliation_report.json").read_text(encoding="utf-8"))
    assert sorted(x["name"] for x in script["checks"]) == sorted(x["name"] for x in rc["checks"])
    by = {x["name"]: x for x in script["checks"]}
    assert by["control_total:rows"]["status"] == "pass"
    assert "199 source row(s) = 44 in scope + 155 excluded" in by["control_total:row_filter"]["detail"]
    out = run("validate.py")
    assert out.returncode == 0, out.stdout + out.stderr
    if not have_pandas:
        pytest.skip("pandas not installed: the generated ETL script was not executed "
                    "(reconciliation and validation scripts were)")
