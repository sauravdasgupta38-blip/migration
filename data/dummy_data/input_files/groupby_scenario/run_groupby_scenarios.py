"""End-to-end test of a reviewer-authored GROUP BY transformation.

Drives the real FastAPI endpoints in-process, in the order the UI calls them:

    upload -> /api/mapping/run -> reviewer decisions -> /api/mapping/certify
           -> /api/transform/run -> /api/validate/run
           -> /api/reconcile/rules -> /api/reconcile/certify -> /api/reconcile/run

No application code is modified. The upload workspace is redirected to a
private temp folder (TEMP/TMP) so the real UI's uploaded files are untouched.

    .venv\\Scripts\\python.exe data\\dummy_data\\input_files\\groupby_scenario\\run_groupby_scenarios.py [A|B|all]

(Deliberately not named *_test.py: pytest would collect it, and importing it
redirects TEMP for the whole test session.)
"""
import csv
import io
import json
import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
_tmp = Path(tempfile.mkdtemp(prefix="groupby_test_"))
os.environ["TEMP"] = os.environ["TMP"] = str(_tmp)
tempfile.tempdir = None                      # re-read TEMP
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient    # noqa: E402
from api import server                       # noqa: E402

C = TestClient(server.app)
GROUP_ATTRS_A = ["policy_active_role_count", "policy_beneficiary_count",
                 "policy_beneficiary_share_total", "policy_total_monthly_premium"]

# ------------------------------------------------------------- reviewer SQL
# The natural way to express "GROUP BY POLREF, then attach the result to every
# role row" as a per-column expression: a windowed aggregate.
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
# The other classic form: a correlated scalar sub-query (GROUP BY per key).
SUBQ_SQL = {
    "policy_active_role_count":
        """(SELECT COUNT(*) FROM "__workset" g WHERE g."POLREF" = "__workset"."POLREF" AND g."STAXRF" = '30')""",
    "policy_beneficiary_count":
        """(SELECT COUNT(*) FROM "__workset" g WHERE g."POLREF" = "__workset"."POLREF" AND g."ROLREF" = 'BN' AND g."STAXRF" = '30')""",
    "policy_beneficiary_share_total":
        """(SELECT SUM(TRY_CAST(NULLIF(trim(g."BENPCT"), '') AS DECIMAL(9,2))) FROM "__workset" g WHERE g."POLREF" = "__workset"."POLREF" AND g."ROLREF" = 'BN' AND g."STAXRF" = '30')""",
    "policy_total_monthly_premium":
        """(SELECT SUM(TRY_CAST(NULLIF(trim(g."CTBAMT"), '') AS DECIMAL(12,2))) FROM "__workset" g WHERE g."POLREF" = "__workset"."POLREF" AND g."STAXRF" = '30')""",
}
# Scenario B: a genuine grain change (199 role rows -> 30 policy rows).
AGG_SQL_B = {
    "policy_reference": 'trim("POLREF")',
    "active_role_count": """COUNT(*) FILTER (WHERE "STAXRF" = '30')""",
    "beneficiary_count": """COUNT(*) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30')""",
    "total_monthly_premium": """SUM(TRY_CAST(NULLIF(trim("CTBAMT"), '') AS DECIMAL(12,2))) FILTER (WHERE "STAXRF" = '30')""",
}


# ------------------------------------------------------------------ helpers
def sse(resp_text: str) -> list[dict]:
    return [json.loads(c[6:]) for c in resp_text.split("\n\n") if c.startswith("data: ")]


def upload(target: Path) -> None:
    C.post("/api/inputs/reset")
    for role, files in (("source", [HERE / "CO040.csv", HERE / "PO010.csv"]),
                        ("enriched", [HERE / "dict_CO040.json", HERE / "dict_PO010.json"]),
                        ("target", [target])):
        C.post("/api/inputs/upload", data={"role": role},
               files=[("files", (f.name, f.read_bytes())) for f in files])


def run_mapping() -> tuple[dict, dict, dict]:
    evs = sse(C.post("/api/mapping/run", json={}).text)
    err = [e for e in evs if e["type"] == "error"]
    assert not err, err
    node = {e["node"]: e["artifact"] for e in evs if e["type"] == "node"}
    done = next(e for e in evs if e["type"] == "complete")
    return done["final_spec"], node["review"], node["validation"]


def certify(spec: dict, decisions: dict) -> tuple[dict, dict, dict]:
    evs = sse(C.post("/api/mapping/certify", json={"spec": spec, "decisions": decisions}).text)
    err = [e for e in evs if e["type"] == "error"]
    assert not err, err
    node = {e["node"]: e["artifact"] for e in evs if e["type"] == "node"}
    done = next(e for e in evs if e["type"] == "complete")
    return done["final_spec"], node["review"], node["validation"]


def ui_default_decisions(queue: dict) -> dict:
    """What 'Accept all suggested' produces in app.js (decisionFor)."""
    out = {}
    for it in queue["items"]:
        if it["kind"] == "unmapped_target":
            out[it["target_attribute"]] = {"action": "edit",
                                           "transformation_sql": it.get("suggested_sql") or "NULL"}
        elif it["gate"] == "reject":
            out[it["target_attribute"]] = {"action": "reject"}
        elif it.get("suggested_sql"):
            out[it["target_attribute"]] = {"action": "edit", "transformation_sql": it["suggested_sql"]}
        else:
            out[it["target_attribute"]] = {"action": "accept"}
    return out


def mapping_of(spec, attr):
    return next((m for m in spec["mappings"] if m["target_attribute"] == attr), None)


def show_agent_view(spec, queue, attrs):
    items = {i["target_attribute"]: i for i in queue["items"]}
    unm = {u["attribute"] for u in spec.get("unmapped_target", [])}
    for a in attrs:
        m = mapping_of(spec, a)
        it = items.get(a)
        kinds = sorted({i["kind"] for i in queue["items"] if i["target_attribute"] == a})
        print(f"  - {a}")
        if m:
            print(f"      agent: gate={m['gate']}  src={m['source_attributes']}  match={m.get('match_source')}"
                  f"  conf={m.get('confidence')}")
            print(f"      sql  : {m.get('transformation_sql') or '(none)'}")
        print(f"      unmapped_target={a in unm}  review cards={kinds or 'NONE (auto-accepted, not editable in UI)'}")


def failing_checks(report, attrs=None):
    out = []
    for c in report["checks"]:
        if c["status"] in ("fail", "warn") and (attrs is None or c.get("target_attribute") in attrs
                                                 or (c["status"] == "fail" and not c.get("target_attribute"))):
            out.append(f"[{c['status']}/{c['severity']}] {c['name']}: {c['detail'][:230]}")
    return out


def compare_expected(csv_text: str, expected_path: Path, key: str, attrs: list[str]):
    got = list(csv.DictReader(io.StringIO(csv_text)))
    exp = list(csv.DictReader(open(expected_path, encoding="utf-8")))
    res = {"rows_got": len(got), "rows_expected": len(exp), "mismatch": {}}
    if len(got) != len(exp):
        return res
    def norm(v):
        v = (v or "").strip()
        try:
            return f"{float(v):.2f}"
        except ValueError:
            return v
    gm = {r[key]: r for r in got}
    for a in attrs:
        bad = [(e[key], e[a], gm.get(e[key], {}).get(a)) for e in exp
               if norm(gm.get(e[key], {}).get(a)) != norm(e[a])]
        res["mismatch"][a] = bad[:3] if bad else 0
    return res


def downstream(spec: dict, expected: Path, key: str, attrs: list[str]) -> dict:
    out = {}
    tr = C.post("/api/transform/run", json={"spec": spec, "preview_limit": 5}).json()
    if "error" in tr:
        out["transform"] = "ERROR: " + tr["error"][:300]
        print("    transform  :", out["transform"])
        return out
    out["transform"] = f"OK rows={tr['stats']['row_count']}"
    print("    transform  :", out["transform"])
    if key == "role_record_id":
        got_order = [r["role_record_id"] for r in csv.DictReader(io.StringIO(tr["csv"]))]
        src_order = [r["UNIQUE_ID"] for r in csv.DictReader(open(HERE / "CO040.csv", encoding="utf-8"))]
        out["row_order_preserved"] = got_order == src_order
        print("    row order  : preserved" if got_order == src_order else
              f"    row order  : CHANGED (first delivered {got_order[:3]} vs source {src_order[:3]})")
    cmp = compare_expected(tr["csv"], expected, key, attrs)
    out["expected"] = cmp
    print("    vs expected:", json.dumps(cmp, default=str)[:600])

    va = C.post("/api/validate/run", json={"spec": spec, "csv": tr["csv"]}).json()
    if "error" in va:
        out["validate"] = "ERROR: " + va["error"][:300]
    else:
        rel = [c for c in va["checks"] if c.get("target_attribute") in attrs or c["status"] == "fail"]
        out["validate"] = va.get("verdict")
        print("    validation :", va.get("verdict"), va.get("headline", "")[:160] if isinstance(va.get("headline"), str) else "")
        for c in rel:
            if c["status"] != "pass" or c["name"].startswith("transform_rule"):
                print(f"       [{c['status']}] {c['name']}: {c['detail'][:180]}")

    rr = C.post("/api/reconcile/rules", json={"spec": spec}).json()
    if "error" in rr:
        out["reconcile"] = "ERROR: " + rr["error"]
        return out
    C.post("/api/reconcile/certify", json={"spec": spec, "decisions": {}, "added": []})
    rc = C.post("/api/reconcile/run", json={"spec": spec, "csv": tr["csv"]}).json()
    if "error" in rc:
        out["reconcile"] = "ERROR: " + rc["error"][:300]
    else:
        out["reconcile"] = rc.get("verdict")
        print("    reconcile  :", rc.get("verdict"))
        for c in rc.get("checks", []):
            if c["name"].startswith("control_total:numeric_sums"):
                cols = [x.get("column") for x in (c.get("sample") or [])]
                print(f"       {c['name']} [{c['status']}] columns tied out: {cols}")
                missing = [a for a in attrs if a not in cols]
                print(f"       group-by columns MISSING from control totals: {missing}")
                out["recon_missing_totals"] = missing
            elif c["status"] != "pass":
                print(f"       [{c['status']}] {c['name']}: {c.get('detail','')[:160]}")
    return out


# --------------------------------------------------------------- scenarios
def scenario_a() -> dict:
    print("\n=== SCENARIO A: group-by aggregates repeated on every role row (grain kept) ===")
    upload(HERE / "policy_client_role.json")
    spec, queue, report = run_mapping()
    print(f"mapping verdict: {report['verdict']}  stats: {spec['stats']}")
    print("How the agent handled the 4 group-by attributes:")
    show_agent_view(spec, queue, GROUP_ATTRS_A)

    results = {}
    for label, sqlset in (("A0 baseline (group-by attrs excluded)", None),
                          ("A1 window-function SQL", WINDOW_SQL),
                          ("A2 correlated sub-query SQL", SUBQ_SQL)):
        print(f"\n--- {label}: reviewer writes the mapping by hand ---")
        dec = ui_default_decisions(queue)
        for a in GROUP_ATTRS_A:
            if sqlset is None:
                dec[a] = {"action": "reject"}
            else:
                dec[a] = {"action": "edit", "transformation_sql": sqlset[a]}
        if sqlset is None:
            cspec, cqueue, creport = certify(spec, dec)
            print(f"  certify verdict: {creport['verdict']}")
            for line in failing_checks(creport):
                print("    ", line)
            results[label] = {"verdict": creport["verdict"],
                              "downstream": downstream(cspec, HERE / "expected_scenario_a.csv",
                                                       "role_record_id", [])}
            continue
        cspec, cqueue, creport = certify(spec, dec)
        r = {"verdict": creport["verdict"]}
        print(f"  certify verdict: {creport['verdict']}")
        for a in GROUP_ATTRS_A:
            m = mapping_of(cspec, a)
            print(f"    {a}: gate after validation = {m['gate'] if m else 'MISSING'}")
            r[a] = m["gate"] if m else None
        for line in failing_checks(creport, GROUP_ATTRS_A):
            print("    ", line)
        if creport["verdict"] == "certified":
            r["downstream"] = downstream(cspec, HERE / "expected_scenario_a.csv", "role_record_id",
                                         GROUP_ATTRS_A)
        else:
            # the UI will not proceed past mapping; still exercise transform to
            # show what WOULD happen if the SQL got through
            print("  (mapping not certified - UI stops here. Forcing gates to auto_accept to probe tabs 2-4)")
            for a in GROUP_ATTRS_A:
                m = mapping_of(cspec, a)
                if m:
                    m["gate"] = "auto_accept"
            r["downstream_forced"] = downstream(cspec, HERE / "expected_scenario_a.csv", "role_record_id",
                                                GROUP_ATTRS_A)
        results[label] = r
    return results


def scenario_b() -> dict:
    print("\n=== SCENARIO B: genuine GROUP BY - target grain is one row per policy ===")
    upload(HERE / "scenario_b_target" / "policy_summary.json")
    spec, queue, report = run_mapping()
    print(f"mapping verdict: {report['verdict']}  stats: {spec['stats']}")
    attrs = ["policy_reference", "product_type", "active_role_count", "beneficiary_count",
             "total_monthly_premium"]
    show_agent_view(spec, queue, attrs)
    # round 1: the reviewer declares the grain (one row per POLREF) and writes
    # the aggregates; every other column is left as the agent mapped it
    dec = ui_default_decisions(queue)
    dec["__grain__"] = {"action": "set_grain", "group_by": ["POLREF"]}
    for a, sql in AGG_SQL_B.items():
        dec[a] = {"action": "edit", "transformation_sql": sql}
    cspec, cqueue, creport = certify(spec, dec)
    print(f"  round 1 (grain set) verdict: {creport['verdict']}")
    for line in failing_checks(creport):
        print("    ", line)
    sugg = {it["target_attribute"]: it["suggested_sql"] for it in cqueue["items"]
            if (it.get("suggested_sql") or "").startswith("ANY_VALUE(")}
    print(f"  re-queued with an ANY_VALUE suggestion: {sorted(sugg)}")
    # round 2: accept the suggested rewrite(s), as the 'Accept suggested fix' button does
    cspec, cqueue, creport = certify(cspec, {a: {"action": "accept_suggestion"} for a in sugg})
    print(f"  round 2 (suggestions accepted) verdict: {creport['verdict']}")
    for line in failing_checks(creport):
        print("    ", line)
    for a in attrs:
        m = mapping_of(cspec, a)
        print(f"    {a}: gate={m['gate'] if m else 'MISSING'}  kind={m.get('transform_kind') if m else ''}"
              f"  sql={(m.get('transformation_sql') or '')[:70] if m else ''}")
    if creport["verdict"] != "certified":
        print("  (not certified - forcing gates to probe the transform)")
        for a in attrs:
            m = mapping_of(cspec, a)
            if m:
                m["gate"] = "auto_accept"
    d = downstream(cspec, HERE / "scenario_b_target" / "expected_scenario_b.csv", "policy_reference",
                   ["product_type", "active_role_count", "beneficiary_count", "total_monthly_premium"])
    return {"verdict": creport["verdict"], "downstream": d}


if __name__ == "__main__":
    which = (sys.argv[1] if len(sys.argv) > 1 else "all").upper()
    summary = {}
    if which in ("A", "ALL"):
        summary["A"] = scenario_a()
    if which in ("B", "ALL"):
        summary["B"] = scenario_b()
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))
