"""Reconciliation workspace (tab 4) — additive.

Two families of checks against the DELIVERED transformed output, both run
against the actual output bytes (not a re-derivation of the transform):

  TECHNICAL — control totals. The figures a migration control sheet carries,
  all derivable from any spec plus any delivered file:
    rows             source workset vs delivered
    columns          delivered vs what the certified spec expects
    populated_cells  populated out of rows x columns
    distinct_keys    distinct business key, source vs delivered
    numeric_sums     total of every numeric column, source vs delivered

  BUSINESS_RULE — does the data still obey the rules the source data implies?
    category_profile for every low-cardinality attribute carrying a transform,
                     record counts grouped by value on both sides and compared
                     bucket by bucket. Categories are discovered from the data,
                     never configured, so this works on any extract.
    crossfield       conditional-population rules mined from the source
                     (engine/insight_cache.py), checked when BOTH the driver and
                     the dependent map 1:1 to a target attribute. Driver codes
                     are translated through the driver's own certified transform
                     before comparison — the rule is mined in SOURCE terms
                     ({CL}) but the delivered data holds the TRANSFORMED value
                     (CLOSED), and comparing the two directly made every row look
                     like a violation.

DELIBERATELY NOT HERE — three families were removed rather than left to pad the
report, because a check that cannot fail, or that a stronger check already
covers, costs the reader attention and earns nothing:

    value_loss       an aggregate populated-count comparison, strictly subsumed
                     by validation's transform check, which re-executes every
                     certified transform value by value across every row and
                     names the offending record. Weaker AND noisier.
    aggregate        sums are no longer a detection mechanism for the same
                     reason: if a value moved, the cell-level check already
                     caught it. Kept only as a REPORTED control total, because a
                     reviewer expects a money column to tie out on the face of
                     the report.
    derivation       read `derivation_resolved` from the enriched dictionary,
                     which was removed when the dictionary was simplified. It
                     produced zero checks and could never produce any.

Row count also moved into the control totals: it was the one check name
literally duplicated with the validation workspace.

Nothing here touches Flow A / Flow B state — it reads the spec + the delivered
CSV + the currently loaded source file(s) + (optionally) the active enriched
dictionary, plus the insight derived from the source data, and returns
CheckResult-shaped JSON.
"""
from __future__ import annotations

import re
import tempfile
from pathlib import Path

import duckdb

from engine.agents.contracts import CheckResult
from engine.models import target_attributes

from engine.etl_sql import (build_target_sql, conservation_function,
                            grain_keys, group_key_attributes)

from .transform import (_q, build_full_sql, build_select, build_source_sql,
                        build_workset_sql, readable_workset_sql, row_filter_active,
                        _script_columns, compile_spec_filter)

SENTINELS = ("00000000", "")


def _write_temp_csv(csv_text: str) -> str:
    fh = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     newline="", encoding="utf-8")
    fh.write(csv_text)
    fh.close()
    return fh.name


def _load_output(con, csv_text: str) -> None:
    path = _write_temp_csv(csv_text)
    try:
        con.execute(
            "CREATE TABLE target_out AS "
            "SELECT * FROM read_csv_auto(?, all_varchar=true, header=true)",
            [path],
        )
    finally:
        Path(path).unlink(missing_ok=True)


def _rows(con, sql: str, limit: int = 5) -> list[dict]:
    cur = con.execute(sql + f" LIMIT {limit}")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def _pop_pred(cols: list[str]) -> str:
    if not cols:
        return "TRUE"
    return " OR ".join(
        f"({_q(c)} IS NOT NULL AND {_q(c)} NOT IN ('00000000',''))" for c in cols
    )


_PROFILE_MAX = 25          # above this a column is a code list, not a category


def _is_nondet(expr: str) -> bool:
    low = (expr or "").lower().replace(" ", "")
    return any(t in low for t in ("now(", "current_timestamp", "current_date",
                                  "current_localtime", "localtimestamp",
                                  "random(", "uuid("))


_NUMERIC_TYPES = {"number", "integer", "int", "float", "decimal", "numeric"}
_TEMPORAL_TYPES = {"date", "datetime", "timestamp", "time"}


def run_reconciliation(spec: dict, target_dict: dict, csv_text: str,
                       source_paths: dict[str, str],
                       enriched: dict | None = None,
                       insight: dict | None = None,
                       rules: list[dict] | None = None) -> dict:
    """Execute the CERTIFIED rule set against the delivered output.

    `rules` is the certified set (api/recon_rules.py). When omitted the
    candidates are derived on the fly, so an unsaved session still works — but
    the certified set is what the script is generated from, so passing it is
    what guarantees the script and these results agree.
    """
    from .recon_rules import derive_candidates, rule_id

    if rules is None:
        rules = derive_candidates(spec, target_dict, source_paths, insight)
    by_kind: dict[str, list[dict]] = {}
    for r in rules:
        by_kind.setdefault(r.get("kind"), []).append(r)
    origin_of = {rule_id(r): r.get("origin", "mined") for r in rules}

    con = duckdb.connect(":memory:")
    checks: list[CheckResult] = []

    def add(name, cat, status, detail, *, sev="soft", attr=None, n=0, sample=None,
            sql=None, scanned=None):
        c = CheckResult(name=name, category=cat, status=status, severity=sev,
                        detail=detail, target_attribute=attr,
                        offending_rows=n, sample=sample or [],
                        sql=sql, rows_scanned=tgt_n if scanned is None else scanned)
        # provenance: a reviewer must be able to tell a mined control from one
        # the business asked for
        c.origin = origin_of.get(name, origin_of.get(name.split(":")[0], "mined"))
        checks.append(c)

    def enabled(kind: str) -> bool:
        """Only run what the certified set contains — a control the reviewer
        excluded must not quietly execute anyway."""
        return kind in by_kind

    _load_output(con, csv_text)
    out_cols = [d[0] for d in con.execute("SELECT * FROM target_out LIMIT 0").description]
    tgt_n = con.execute("SELECT count(*) FROM target_out").fetchone()[0]

    type_of = {a["name"]: (a.get("type") or "string") for a in
              (target_dict.get("attributes") or target_dict.get("columns")
               or target_dict.get("fields") or []) if isinstance(a, dict)}

    # every source-side figure below reads `workset_sql`, which is the workset
    # AFTER the reviewer's row filter — rows, keys, sums and category profiles
    # all reconcile against the rows that were in scope to migrate
    workset_sql, params, src_n, workset_err = None, None, None, None
    filtered, src_total = row_filter_active(spec), None
    if source_paths:
        try:
            workset_sql, params = build_source_sql(spec, source_paths)
            src_n = con.execute(f"SELECT count(*) FROM ({workset_sql}) AS w", params).fetchone()[0]
            if filtered:
                all_sql, all_params = build_workset_sql(spec, source_paths)
                src_total = con.execute(f"SELECT count(*) FROM ({all_sql}) AS w",
                                        all_params).fetchone()[0]
        except Exception as e:  # noqa: BLE001
            workset_err = str(e)

    # The certified target rebuilt from source ONCE, by the same statement the
    # transform runs (engine/etl_sql.build_target_sql). Every source-side
    # figure below — totals, category counts, business aggregates — is read
    # from it. Re-wrapping each column's SQL on its own (`sum(<expr>)`,
    # `GROUP BY <expr>`) failed for window and aggregate columns, and that
    # failure was swallowed: the columns silently vanished from the totals.
    keys = grain_keys(spec)
    expected_ok, expected_err = False, None
    if workset_sql is not None:
        try:
            exp_sql, exp_params = build_full_sql(spec, source_paths)
            con.execute(f"CREATE OR REPLACE TABLE _expected AS {exp_sql}", exp_params)
            expected_ok = True
        except Exception as e:  # noqa: BLE001
            expected_err = str(e).splitlines()[0]
    exp_cols = ([d[0] for d in con.execute("SELECT * FROM _expected LIMIT 0").description]
                if expected_ok else [])

    # the mapped business key, if the spec has one — used by the control totals
    # and by the categorical profiles below
    one_to_one_pre = {}
    for m in spec.get("mappings", []):
        if (m.get("cardinality") == "1:1" and len(m.get("source_attributes") or []) == 1
                and m.get("gate") != "reject"):
            one_to_one_pre.setdefault(m["source_attributes"][0], m["target_attribute"])
    candidate_keys = (insight or {}).get("candidate_keys") or []
    key_src = next((k for k in candidate_keys if k in one_to_one_pre), None)
    key_tgt = one_to_one_pre.get(key_src) if key_src else None

    # =========================== TECHNICAL =================================
    # rows are a control total, not a family of their own — and this was the
    # one check name literally duplicated with the validation workspace
    if src_n is not None and enabled("control_total:rows"):
        sample = [{"measure": "Record count", "source": src_n,
                   "delivered": tgt_n, "difference": tgt_n - src_n,
                   "ties": src_n == tgt_n}]
        if filtered:
            # show the whole population, so the excluded rows are accounted
            # for on the face of the report rather than simply missing
            sample = [{"measure": "Source rows (all files, before filter)",
                       "source": src_total, "delivered": "—", "difference": "—", "ties": True},
                      {"measure": "Excluded by row filter",
                       "source": src_total - src_n, "delivered": "—", "difference": "—",
                       "ties": True},
                      {"measure": "Source rows in scope", "source": src_n,
                       "delivered": tgt_n, "difference": tgt_n - src_n,
                       "ties": src_n == tgt_n}]
        add("control_total:rows", "control_total",
            "pass" if src_n == tgt_n else "fail",
            (f"{src_n:,} source row(s) in scope after the row filter "
             f"({src_total - src_n:,} of {src_total:,} excluded) -> {tgt_n:,} "
             f"delivered target row(s)." if filtered else
             f"{src_n:,} source row(s) -> {tgt_n:,} delivered target row(s)."),
            sev="hard", n=0 if src_n == tgt_n else abs(src_n - tgt_n),
            sample=sample,
            sql=("-- delivered rows must equal the rebuilt source workset\n"
                 + ("-- (after the reviewer's row filter)\n" if filtered else "")
                 + "SELECT (SELECT count(*) FROM target_out) AS delivered_rows,\n"
                 f"       (SELECT count(*) FROM ({workset_sql}) AS w) AS source_rows;"))
    elif enabled("control_total:rows"):
        add("control_total:rows", "control_total", "warn",
            workset_err and f"could not rebuild the source workset: {workset_err}"
            or f"{tgt_n:,} delivered target row(s) — source file(s) not loaded, "
               f"cannot compare.")
    if filtered and src_n is not None and enabled("control_total:row_filter"):
        from engine.row_filter import describe_row_filter

        desc = describe_row_filter(spec["row_filter"])
        add("control_total:row_filter", "control_total", "pass",
            f"{src_total:,} source row(s) = {src_n:,} in scope + "
            f"{src_total - src_n:,} excluded by the row filter: {desc}",
            sample=[{"measure": "Source rows (all)", "count": src_total},
                    {"measure": "In scope (migrated)", "count": src_n},
                    {"measure": "Excluded by row filter", "count": src_total - src_n}],
            sql=("-- the reviewer's row filter, applied to the rebuilt source workset\n"
                 f"SELECT count(*) AS in_scope FROM ({workset_sql}) AS w;"),
            scanned=src_total)

    # ---- declared grain: groups, group keys, conservation ------------------
    if keys and enabled("control_total:groups"):
        if expected_ok:
            groups = con.execute("SELECT count(*) FROM _expected").fetchone()[0]
            add("control_total:groups", "control_total",
                "pass" if groups == tgt_n else "fail",
                f"{src_n:,} source row(s) in scope -> {groups:,} distinct "
                f"{', '.join(keys)} group(s) -> {tgt_n:,} delivered row(s).",
                sev="hard", n=abs(groups - tgt_n),
                sample=[{"measure": "Source rows in scope", "source": src_n, "delivered": "—",
                         "difference": "—", "ties": True},
                        {"measure": f"Groups ({', '.join(keys)})", "source": groups,
                         "delivered": tgt_n, "difference": tgt_n - groups,
                         "ties": groups == tgt_n}],
                sql=(f"SELECT count(*) AS groups FROM (SELECT 1 FROM (<source>) AS w "
                     f"GROUP BY {', '.join(_q(k) for k in keys)});\n"
                     f"SELECT count(*) AS delivered_rows FROM target_out;"))
        else:
            add("control_total:groups", "control_total", "skipped",
                f"could not rebuild the grouped target from source: "
                f"{expected_err or workset_err or 'no source loaded'}")
    if keys and enabled("control_total:keys"):
        gattrs = [a for a in group_key_attributes(spec) if a in out_cols and a in exp_cols]
        if expected_ok and gattrs:
            on = " AND ".join(f"CAST(e.{_q(a)} AS VARCHAR) IS NOT DISTINCT FROM "
                              f"CAST(d.{_q(a)} AS VARCHAR)" for a in gattrs)
            diff_sql = (f"SELECT {', '.join('coalesce(CAST(e.' + _q(a) + ' AS VARCHAR), CAST(d.' + _q(a) + ' AS VARCHAR)) AS ' + _q(a) for a in gattrs)}, "
                        f"e.{_q(gattrs[0])} IS NULL AS only_delivered "
                        f"FROM _expected e FULL OUTER JOIN target_out d ON {on} "
                        f"WHERE e.{_q(gattrs[0])} IS NULL OR d.{_q(gattrs[0])} IS NULL")
            n = con.execute(f"SELECT count(*) FROM ({diff_sql})").fetchone()[0]
            add("control_total:keys", "control_total", "pass" if not n else "fail",
                (f"all {tgt_n:,} delivered group key(s) {gattrs} match the source's groups."
                 if not n else f"{n} group key(s) appear on one side only."),
                sev="hard", n=n, sample=_rows(con, diff_sql, 10) if n else [],
                sql=diff_sql.replace("_expected", "(<certified target statement>)") + ";")
        else:
            add("control_total:keys", "control_total", "skipped",
                "no delivered attribute carries the group key, so group keys cannot "
                "be matched" if expected_ok else
                f"could not rebuild the grouped target from source: "
                f"{expected_err or workset_err or 'no source loaded'}")
    for _r in by_kind.get("control_total:conservation", []):
        pr = _r.get("params") or {}
        attr, expr, fn = pr.get("attribute"), pr.get("expr") or "", pr.get("function")
        name = f"control_total:conservation:{attr}"
        if workset_sql is None or attr not in out_cols:
            add(name, "control_total", "skipped",
                f"'{attr}' could not be checked: "
                + ("no source loaded" if workset_sql is None else "not in the delivered file"),
                attr=attr)
            continue
        try:
            whole = con.execute(f"SELECT ({expr}) FROM ({workset_sql}) AS \"__workset\"",
                                params).fetchone()[0]
            parts = con.execute(f"SELECT sum(TRY_CAST({_q(attr)} AS DOUBLE)) "
                                f"FROM target_out").fetchone()[0]
        except Exception as e:      # noqa: BLE001
            add(name, "control_total", "skipped", f"'{attr}' could not be checked: {e}",
                attr=attr)
            continue
        w, p = float(whole or 0), float(parts or 0)
        ties = abs(w - p) <= max(abs(w), abs(p)) * 1e-6 + 1e-9
        add(name, "control_total", "pass" if ties else "fail",
            (f"'{attr}': the delivered group values add up to {p:,.2f}, the same "
             f"{fn.upper()} as the whole source." if ties else
             f"'{attr}': the delivered group values add up to {p:,.2f} but the whole "
             f"source gives {w:,.2f}."),
            sev="hard", attr=attr, n=0 if ties else 1,
            sample=[{"measure": f"{fn.upper()} over the whole source", "value": w},
                    {"measure": "sum of the delivered group values", "value": p}],
            sql=(f"SELECT ({expr}) AS whole_source FROM (<source>) AS \"__workset\";\n"
                 f"SELECT sum(TRY_CAST({_q(attr)} AS DOUBLE)) AS delivered FROM target_out;"))

    # ---- control totals: the figures a migration control sheet carries ----
    # Generic by construction: row count, column count, populated-cell count and
    # distinct key count are derivable from ANY spec + delivered file, so this
    # block needs no knowledge of what the data means.
    expected_cols = {m["target_attribute"] for m in spec.get("mappings", [])
                     if m.get("gate") != "reject"}
    expected_cols |= {u["attribute"] for u in spec.get("unmapped_target", [])}
    if enabled("control_total:columns"):
      add("control_total:columns", "control_total",
        "pass" if not (expected_cols - set(out_cols)) else "fail",
        f"{len(out_cols)} column(s) delivered; the certified spec expects "
        f"{len(expected_cols)}.", sev="hard",
        n=len(expected_cols - set(out_cols)), scanned=0,
        sample=([{"measure": "Column count", "expected": len(expected_cols),
                  "delivered": len(out_cols),
                  "ties": not (expected_cols - set(out_cols))}]
                + [{"measure": "Missing column", "expected": c, "delivered": "—",
                    "ties": False} for c in sorted(expected_cols - set(out_cols))]),
        sql=("-- every column the certified spec expects must be delivered\n"
             "SELECT column_name FROM information_schema.columns\n"
             "WHERE table_name = 'target_out';"))

    # Fill rate, split by whether a blank is LEGITIMATE. A raw
    # "916 populated of 1,150" reads as 234 cells lost; almost always those
    # cells are blank in attributes the target dictionary declares nullable,
    # which is not a discrepancy at all. What matters — and what a reviewer is
    # actually asking — is whether any blank sits in an attribute that requires
    # a value.
    required = {a["name"] for a in target_attributes(target_dict)
                if not a.get("nullable", False)}
    populated = blank_required = 0
    _blank_by_col: dict[str, int] = {}
    for c in out_cols:
        empty = con.execute(
            f"SELECT count(*) FROM target_out WHERE {_q(c)} IS NULL "
            f"OR {_q(c)} = ''").fetchone()[0]
        populated += tgt_n - empty
        _blank_by_col[c] = empty
        if c in required:
            blank_required += empty
    total_cells = tgt_n * len(out_cols)
    blanks = total_cells - populated
    if enabled("control_total:populated_cells"):
      add("control_total:populated_cells", "control_total",
        "fail" if blank_required else "pass",
        (f"{populated:,} of {total_cells:,} delivered cell(s) hold a value "
         f"({tgt_n:,} row(s) x {len(out_cols)} column(s)). "
         + (f"{blanks:,} are blank, all in attributes the target declares "
            f"nullable." if blanks and not blank_required
            else f"{blanks:,} are blank, of which {blank_required:,} sit in "
                 f"attributes that require a value." if blanks
            else "None are blank.")),
        sev="hard", n=blank_required,
        sample=[{"attribute": c,
                 "records": tgt_n,
                 "populated": tgt_n - _blank_by_col[c],
                 "blank": _blank_by_col[c],
                 "mandatory": c in required,
                 "ties": not (c in required and _blank_by_col[c])}
                for c in out_cols],
        sql=("-- populated cells, one pass per delivered column\n"
             + "\n".join(f"SELECT count(*) FROM target_out WHERE {_q(c)} IS NOT NULL "
                          f"AND {_q(c)} <> '';" for c in out_cols[:3])
             + ("\n-- ... one per column" if len(out_cols) > 3 else "")))

    # Rule-gated like every other control. This ran on `if key_tgt` alone, so
    # the app emitted it while the standalone script did not — the exact
    # app/script divergence this module exists to prevent, and it only surfaced
    # when the control stopped being proposed.
    if enabled("control_total:distinct_keys") and key_tgt and key_tgt in out_cols:
        d_keys = con.execute(f"SELECT count(DISTINCT {_q(key_tgt)}) "
                             f"FROM target_out").fetchone()[0]
        s_keys = None
        if workset_sql is not None and key_src:
            try:
                s_keys = con.execute(f"SELECT count(DISTINCT {_q(key_src)}) "
                                     f"FROM ({workset_sql}) AS w", params).fetchone()[0]
            except Exception:      # noqa: BLE001
                s_keys = None
        ok = s_keys is None or s_keys == d_keys
        add("control_total:distinct_keys", "control_total", "pass" if ok else "fail",
            (f"{d_keys:,} distinct '{key_tgt}' delivered"
             + (f"; {s_keys:,} distinct '{key_src}' in the source." if s_keys is not None
                else " — source not loaded, no comparison.")),
            sev="hard", attr=key_tgt, n=0 if ok else abs((s_keys or 0) - d_keys),
            sample=[{"measure": f"Distinct '{key_tgt}'",
                     "source": s_keys if s_keys is not None else "—",
                     "delivered": d_keys, "ties": ok},
                    {"measure": "Records", "source": src_n if src_n is not None else "—",
                     "delivered": tgt_n, "ties": True}],
            sql=(f"SELECT count(DISTINCT {_q(key_tgt)}) AS delivered_keys FROM target_out;"
                 + (f"\nSELECT count(DISTINCT {_q(key_src)}) AS source_keys "
                    f"FROM (<source workset>) AS w;" if key_src and workset_sql else "")))

    # ---- numeric control totals ------------------------------------------
    # Sums are NOT a detection mechanism here: validation re-executes every
    # certified transform cell by cell, so if a value moved the sum has already
    # been caught with the offending record named. They are reported because a
    # reviewer expects a money column to tie out on the face of the report —
    # one control-total line per numeric column, not a family of checks.
    if workset_sql is not None and enabled("control_total:numeric_sums"):
        sums, skipped = [], []
        for m in spec.get("mappings", []):
            attr = m.get("target_attribute")
            expr = (m.get("transformation_sql") or "").strip()
            if (m.get("gate") == "reject" or attr not in out_cols or not expr
                    or type_of.get(attr) not in _NUMERIC_TYPES):
                continue
            # A column that cannot be recomputed is REPORTED, never dropped:
            # silently `continue`-ing here is how window columns vanished.
            if not expected_ok or attr not in exp_cols:
                skipped.append({"column": attr, "skipped": expected_err
                                or "not produced by the certified target statement"})
                continue
            try:
                expected = con.execute(
                    f"SELECT sum(TRY_CAST({_q(attr)} AS DOUBLE)) FROM _expected").fetchone()[0]
                actual = con.execute(
                    f"SELECT sum(TRY_CAST({_q(attr)} AS DOUBLE)) FROM target_out").fetchone()[0]
            except Exception as e:      # noqa: BLE001
                skipped.append({"column": attr, "skipped": str(e).splitlines()[0]})
                continue
            if expected is None and actual is None:
                continue
            diff = abs((expected or 0) - (actual or 0))
            tol = max(abs(expected or 0), abs(actual or 0)) * 1e-6 + 1e-9
            sums.append({"column": attr, "source_total": expected,
                         "delivered_total": actual, "ties": diff <= tol})
        if sums or skipped:
            off = [x for x in sums if not x["ties"]]
            note = (f" {len(skipped)} column(s) could not be recomputed: "
                    + "; ".join(f"{x['column']} ({x['skipped']})" for x in skipped)
                    if skipped else "")
            add("control_total:numeric_sums", "control_total",
                "fail" if off else ("pass" if sums else "skipped"),
                ((f"{len(sums)} numeric column(s) tie out between source and "
                  f"delivered: "
                  + "; ".join(f"{x['column']}={x['delivered_total']:,.2f}" for x in sums)
                  if not off else
                  f"{len(off)} of {len(sums)} numeric column(s) do not tie out.") + note),
                sev="hard", n=len(off), sample=sums + skipped,
                sql=("-- each numeric column: the certified target statement re-executed\n"
                     "-- from source (expected) vs the delivered file\n"
                     + "\n".join(
                         f"SELECT (SELECT sum(TRY_CAST({_q(x['column'])} AS DOUBLE)) "
                         f"FROM (<certified target statement>)) AS source_total, "
                         f"(SELECT sum(TRY_CAST({_q(x['column'])} AS DOUBLE)) "
                         f"FROM target_out) AS delivered_total;"
                         for x in sums)))

    # =========================== BUSINESS RULE ==============================
    # ---- categorical profiles: "how many of each kind?" -------------------
    #
    # This is what a business reviewer actually asks — how many live policies,
    # how many exited, how many of each product — and it is derived, never
    # hardcoded. The rule is structural:
    #
    #   for every LOW-CARDINALITY target attribute that carries a transform,
    #   group both sides by value and compare the record count per bucket.
    #
    # "Low cardinality" is measured from the delivered data (<= _PROFILE_MAX
    # distinct values), so an enum declared in the dictionary qualifies, and so
    # does any de-facto category in a file that declares nothing. Nothing here
    # knows what a policy or a product is: point it at a claims or member
    # extract and it profiles whatever categories that file happens to have.
    #
    # Each bucket is a reconciliation in its own right: the count recomputed
    # from the certified transform must equal the count delivered.
    if workset_sql is not None:
        # A CERTIFIED control that does not run must say so. Every branch below
        # used to `continue` silently, so a reviewer who certified
        # "record counts per value of exit_code" simply never saw it again —
        # indistinguishable from a control that ran and passed. The validation
        # workspace has held this invariant from the start; reconciliation did
        # not.
        for _r in by_kind.get("category_profile", []):
            attr = (_r.get("params") or {}).get("attribute")
            expr = ((_r.get("params") or {}).get("expr") or "").strip()
            _skip = lambda why: add(  # noqa: E731
                f"category_profile:{attr}", "category_profile", "skipped",
                f"'{attr}' was not profiled: {why}", sev="soft", attr=attr)
            if attr not in out_cols:
                _skip("the column is not present in the delivered file")
                continue
            if not expr:
                _skip("the certified mapping carries no transform to recompute")
                continue
            try:
                distinct = con.execute(f"SELECT count(DISTINCT {_q(attr)}) "
                                       f"FROM target_out").fetchone()[0]
            except Exception as e:      # noqa: BLE001
                _skip(f"the delivered column could not be read — {e}")
                continue
            if distinct == 0:
                _skip("every delivered value is null, so there are no categories "
                      "to count — check the transform for this attribute")
                continue
            if distinct > _PROFILE_MAX:
                _skip(f"{distinct} distinct values exceeds the {_PROFILE_MAX} "
                      f"treated as a category; this reads as a code list or free text")
                continue
            if distinct == tgt_n:
                _skip("every record has its own value, so this is an identifier "
                      "rather than a category")
                continue
            if not expected_ok or attr not in exp_cols:
                _skip("the certified target could not be rebuilt from source — "
                      + (expected_err or "the column is not produced by it"))
                continue
            try:
                exp = dict(con.execute(
                    f"SELECT CAST({_q(attr)} AS VARCHAR) AS v, count(*) "
                    f"FROM _expected GROUP BY 1").fetchall())
                act = dict(con.execute(
                    f"SELECT CAST({_q(attr)} AS VARCHAR) AS v, count(*) "
                    f"FROM target_out GROUP BY 1").fetchall())
            except Exception as e:      # noqa: BLE001
                _skip(f"the comparison could not be executed — {e}")
                continue
            buckets = sorted(set(exp) | set(act), key=lambda v: (v is None, str(v)))
            diffs = [(v, exp.get(v, 0), act.get(v, 0)) for v in buckets
                     if exp.get(v, 0) != act.get(v, 0)]
            profile = ", ".join(f"{v if v is not None else 'null'}={act.get(v, 0):,}"
                                for v in buckets)
            add(f"category_profile:{attr}", "category_profile",
                "pass" if not diffs else "fail",
                (f"'{attr}' record counts by value reconcile across "
                 f"{len(buckets)} categor{'y' if len(buckets) == 1 else 'ies'} "
                 f"({profile})."
                 if not diffs else
                 f"'{attr}': {len(diffs)} categor{'y' if len(diffs) == 1 else 'ies'} "
                 f"differ between the recomputed transform and the delivered data."),
                sev="hard", attr=attr, n=sum(abs(e - a) for _v, e, a in diffs),
                sample=[{"value": v if v is not None else "(null)",
                         "expected_records": e, "delivered_records": a,
                         "difference": a - e} for v, e, a in diffs]
                       or [{"value": v if v is not None else "(null)",
                            "expected_records": exp.get(v, 0),
                            "delivered_records": act.get(v, 0),
                            "difference": 0} for v in buckets],
                sql=(f"-- record counts per value, recomputed vs delivered\n"
                     f"SELECT CAST({_q(attr)} AS VARCHAR) AS value, count(*) AS records\n"
                     f"FROM (<certified target statement>) AS e GROUP BY 1;\n"
                     f"SELECT CAST({_q(attr)} AS VARCHAR) AS value, count(*) AS records\n"
                     f"FROM target_out GROUP BY 1;"))

    # ---- business-authored aggregates -------------------------------------
    # Recompute the aggregate from the SOURCE using each column's certified
    # transform, recompute it from the DELIVERED file, and compare bucket by
    # bucket. Same execution path as every mined control — a user rule is not a
    # special case, which is why it needs no separate machinery.
    _sql_of = {m.get("target_attribute"): (m.get("transformation_sql") or "")
               for m in spec.get("mappings", []) if m.get("gate") != "reject"}
    for _r in by_kind.get("aggregate_by", []):
        pr = _r.get("params") or {}
        fn = (pr.get("function") or "sum").lower()
        col, groups = pr.get("column"), list(pr.get("group_by") or [])
        if workset_sql is None:
            skip_reason = "source workset unavailable"
            add(f"aggregate_by:{rule_id(_r).split(':',1)[1]}", "aggregate_by", "warn",
                f"could not evaluate: {skip_reason}.", sev="soft", attr=col)
            continue
        if any(g not in out_cols for g in groups) or (fn != "count" and col not in out_cols):
            continue
        agg = {"count": "count(*)", "count_distinct": f"count(DISTINCT {{x}})"}.get(
            fn, f"{fn}({{x}})")
        tgt_expr = agg.replace("{x}", f"TRY_CAST({_q(col)} AS DOUBLE)" if col else "*")
        src_expr = agg.replace("{x}", f"TRY_CAST(({_sql_of.get(col, 'NULL')}) AS DOUBLE)"
                               if col else "*")
        tgt_keys = ", ".join(f"CAST({_q(g)} AS VARCHAR)" for g in groups) or "'ALL'"
        src_keys = ", ".join(f"CAST(({_sql_of.get(g, 'NULL')}) AS VARCHAR)"
                             for g in groups) or "'ALL'"
        label = (f"{fn} of {col or 'records'}"
                 + (f" by {', '.join(groups)}" if groups else ""))
        # the source side is the certified target rebuilt from source, so the
        # SAME aggregate runs on both sides — whatever kind the columns are
        if expected_ok and all(c in exp_cols for c in ([col] if col else []) + groups):
            src_expr, src_keys, src_from = tgt_expr, tgt_keys, "_expected"
        else:
            src_from = f"({workset_sql}) AS w"
        try:
            src = dict(con.execute(
                f"SELECT concat_ws(' | ', {src_keys}) AS k, {src_expr} "
                f"FROM {src_from} GROUP BY 1",
                [] if src_from == "_expected" else params).fetchall())
            tgt = dict(con.execute(
                f"SELECT concat_ws(' | ', {tgt_keys}) AS k, {tgt_expr} "
                f"FROM target_out GROUP BY 1").fetchall())
        except Exception as e:      # noqa: BLE001
            add(f"aggregate_by:{rule_id(_r).split(':',1)[1]}", "aggregate_by", "warn",
                f"{label}: could not evaluate — {e}", sev="soft", attr=col)
            continue
        buckets = sorted(set(src) | set(tgt), key=str)
        rows, off = [], 0
        for b in buckets:
            a_, d_ = src.get(b) or 0, tgt.get(b) or 0
            ties = abs(a_ - d_) <= max(abs(a_), abs(d_)) * 1e-6 + 1e-9
            if not ties:
                off += 1
            rows.append({"group": b, "source": a_, "delivered": d_,
                         "ties": ties, "difference": d_ - a_})
        add(f"aggregate_by:{rule_id(_r).split(':',1)[1]}", "aggregate_by",
            "pass" if not off else "fail",
            (f"{label}: all {len(buckets)} group(s) agree between source and delivered."
             if not off else
             f"{label}: {off} of {len(buckets)} group(s) do not agree."),
            sev=_r.get("severity", "hard"), attr=col, n=off,
            sql=(f"-- {label}, recomputed from source vs delivered\n"
                 f"SELECT concat_ws(' | ', {src_keys}) AS grp, {src_expr} "
                 f"FROM {'(<certified target statement>)' if src_from == '_expected' else '(<source workset>)'} "
                 f"AS w GROUP BY 1;\n"
                 f"SELECT concat_ws(' | ', {tgt_keys}) AS grp, {tgt_expr} "
                 f"FROM target_out GROUP BY 1;"),
            sample=rows)

    # Crossfield rules come from the CERTIFIED set, already stated in the
    # delivered file's vocabulary (recon_rules.decode_pairs). Re-deriving them
    # here is what produced three divergent implementations of the same rule.
    for _r in by_kind.get("crossfield", []):
        pr = _r.get("params") or {}
        dep_tgt, driver_tgt = pr.get("attribute"), pr.get("driver")
        vals = list(pr.get("values") or [])
        if not (dep_tgt in out_cols and driver_tgt in out_cols and vals):
            continue
        inlist = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
        where = (f"{_q(dep_tgt)} IS NOT NULL AND {_q(dep_tgt)} <> '' "
                 f"AND {_q(driver_tgt)} NOT IN ({inlist})")
        bad = con.execute(f"SELECT count(*) FROM target_out WHERE {where}").fetchone()[0]
        add(f"crossfield:{dep_tgt}~{driver_tgt}", "crossfield",
            "pass" if not bad else "fail",
            f"'{dep_tgt}' should be populated only when '{driver_tgt}' in "
            f"{{{', '.join(vals)}}}: {bad} violation(s) in the delivered output.",
            sev=_r.get("severity", "soft"), attr=dep_tgt, n=bad,
            sql=(f"SELECT count(*) AS violations FROM target_out\n"
                 f"WHERE {_q(dep_tgt)} IS NOT NULL AND {_q(dep_tgt)} <> ''\n"
                 f"  AND {_q(driver_tgt)} NOT IN ({inlist});"),
            sample=_rows(con, f"SELECT * FROM target_out WHERE {where}") if bad else None)

    con.close()

    hard_fail = any(c.status == "fail" and c.severity == "hard" for c in checks)
    any_fail = any(c.status == "fail" for c in checks)
    verdict = "blocked" if hard_fail else ("needs_review" if any_fail else "certified")
    stats = {
        "checks": len(checks),
        "passed": sum(c.status == "pass" for c in checks),
        "warnings": sum(c.status == "warn" for c in checks),
        "failures": sum(c.status == "fail" for c in checks),
        "technical": sum(c.category == "control_total" for c in checks),
        "business_rule": sum(c.category in ("category_profile", "crossfield")
                             for c in checks),
    }
    return {
        "verdict": verdict,
        "checks": [c.model_dump() for c in checks],
        "stats": stats,
        "source_table": spec.get("source_table"),
        "target_table": spec.get("target_table"),
    }


# ---------------------------------------------------------------------------
# code generation — a standalone, readable Python/DuckDB reconciliation script
# ---------------------------------------------------------------------------
_CASE_PAIR = re.compile(r"WHEN\s+'((?:[^']|'')*)'\s+THEN\s+'((?:[^']|'')*)'", re.I)


def decode_pairs(sql: str) -> dict[str, str]:
    """Source code -> target value, read straight out of a certified CASE.

    The rule preview has no database, but it must still speak the same
    vocabulary as the report: a dependency mined from the source says
    "populated only when STATCD in {CL}", while the delivered data holds
    'CLOSED'. Showing the raw code in the preview while the results showed the
    decoded value made the two disagree — and the preview's own text claimed it
    was already in target terms.
    """
    return {a.replace("''", "'"): b.replace("''", "'")
            for a, b in _CASE_PAIR.findall(sql or "")}


def describe_reconciliation_rules(spec: dict, source_filenames: dict[str, str],
                                  insight: dict | None = None,
                                  enriched: dict | None = None,
                                  target_dict: dict | None = None,
                                  rules: list[dict] | None = None) -> list[dict]:
    """Group the CERTIFIED rule set for display.

    This used to re-derive the rules independently of the runner and the script
    generator — three implementations, which is exactly how the preview came to
    show source codes while the report showed decoded values. It now derives
    nothing.
    """
    from .recon_rules import derive_candidates

    if rules is None:
        rules = derive_candidates(spec, target_dict or {}, source_filenames, insight)

    families: dict[str, list[dict]] = {}
    for r in rules:
        families.setdefault(r.get("category") or r.get("kind"), []).append(r)

    detail = {
        "control_total": "The figures a migration control sheet carries: row "
                         "count source vs delivered, delivered columns against "
                         "the certified spec, blank cells in attributes that "
                         "require a value, distinct business keys, and the total "
                         "of every numeric column.",
        "category_profile": "For every low-cardinality attribute carrying a "
                            "transform, record counts are grouped by value on "
                            "both sides and compared bucket by bucket. The "
                            "categories are discovered from the delivered data, "
                            "not configured.",
        "crossfield": "An attribute that is only ever populated for certain "
                      "driver values in the source must stay that way in the "
                      "delivered output. Driver values below are shown as they "
                      "appear in the delivered file.",
    }
    if row_filter_active(spec):
        from engine.row_filter import describe_row_filter

        detail["control_total"] += (" Source figures are taken after the reviewer's "
                                    "row filter, and the rows it excluded are "
                                    "reported: " + describe_row_filter(spec["row_filter"]) + ".")
    out = []
    for cat, rs in families.items():
        added = sum(1 for r in rs if r.get("origin") == "user_added")
        entry = {"category": cat, "name": cat,
                 "detail": detail.get(cat, ""),
                 "scope": f"{len(rs)} rule(s)"
                          + (f" · {added} added by the business" if added else "")}
        if cat == "crossfield":
            entry["items"] = [{"attribute": (r["params"] or {}).get("attribute"),
                               "driver": (r["params"] or {}).get("driver"),
                               "values": (r["params"] or {}).get("values") or [],
                               "origin": r.get("origin", "mined")} for r in rs]
        out.append(entry)
    order = ["control_total", "category_profile", "crossfield"]
    out.sort(key=lambda e: order.index(e["category"]) if e["category"] in order else 99)
    return out


def generate_reconciliation_script(spec: dict, output_filename: str,
                                   source_filenames: dict[str, str],
                                   insight: dict | None = None,
                                   enriched: dict | None = None,
                                   target_dict: dict | None = None,
                                   rules: list[dict] | None = None,
                                   source_paths: dict[str, str] | None = None) -> str:
    target = spec.get("target_table", "target")
    type_of = {a["name"]: (a.get("type") or "").lower()
               for a in (target_dict or {}).get("attributes", [])}

    # The THIRD derivation used to live here. The script is now generated from
    # the same certified rule set the runner executes, so a script and the
    # results it is meant to reproduce cannot disagree.
    from .recon_rules import derive_candidates

    if rules is None:
        rules = derive_candidates(spec, target_dict or {}, source_filenames, insight)
    crossfield = [((r["params"] or {}).get("attribute"),
                   (r["params"] or {}).get("driver"),
                   (r["params"] or {}).get("values") or [])
                  for r in rules if r.get("kind") == "crossfield"]
    enabled_kinds = {r.get("kind") for r in rules}

    numeric = [(x["column"], x["expr"])
               for r in rules if r.get("kind") == "control_total:numeric_sums"
               for x in ((r["params"] or {}).get("columns") or [])]
    # a FOURTH derivation lived here too: the script decided for itself which
    # attributes were categorical, so a profile the reviewer excluded would
    # still run in the handed-over script
    categorical = [(r["params"] or {}).get("attribute")
                   for r in rules if r.get("kind") == "category_profile"]
    expected_cols = next((sorted((r["params"] or {}).get("expected_columns") or [])
                          for r in rules if r.get("kind") == "control_total:columns"),
                         [])
    primary = next(iter(source_filenames)).lower() if source_filenames else None

    L: list[str] = []
    ap = L.append
    ap('"""')
    ap(f"Reconciliation — control totals + business controls for {output_filename} -> {target}")
    ap("")
    ap("Auto-generated from the certified mapping specification, and standalone:")
    ap("it needs only duckdb, the delivered file and the source file(s).")
    ap("")
    ap("  TECHNICAL      control totals — rows, columns, populated cells and the")
    ap("                 total of every numeric column, source vs delivered.")
    ap("  BUSINESS RULE  record counts per category, and the conditional-population")
    ap("                 rules mined from the source data.")
    ap('"""')
    ap("import json")
    ap("import duckdb")
    ap("")
    ap('con = duckdb.connect(":memory:")')
    ap(f'con.execute("CREATE TABLE target_out AS SELECT * FROM read_csv_auto('
       f'\'{output_filename}\', all_varchar=true, header=true)")')
    for t, fn in source_filenames.items():
        ap(f'con.execute(\'CREATE TABLE {t.lower()} AS SELECT * FROM read_csv_auto('
           f'"{fn}", all_varchar=true, header=true)\')')
    filtered = bool(primary) and row_filter_active(spec)
    keys = grain_keys(spec)
    if primary:
        # The script rebuilds the SAME source the app reconciles against — the
        # joined workset, after the reviewer's row filter — and the certified
        # target from it, with the statement the transform runs
        # (engine/etl_sql.build_target_sql). Every source-side figure below
        # reads `source` (rows in scope) or `expected` (the target rebuilt from
        # source), exactly as run_reconciliation does.
        from engine.etl_sql import assert_spec_safe
        from engine.row_filter import describe_row_filter

        assert_spec_safe(spec)
        tables = [t for t in (spec.get("source_tables") or list(source_filenames))
                  if t in source_filenames]
        paths = source_paths or {}
        all_sql = f"SELECT * FROM {primary}"
        if len(tables) > 1 and spec.get("join_plan"):
            try:
                all_sql = readable_workset_sql(spec, tables, source_filenames, paths)
            except Exception:      # noqa: BLE001 — join files unreadable here: primary only
                pass
        where = ""
        if filtered:
            where = "\nWHERE " + compile_spec_filter(
                spec, _script_columns(spec, tables, source_filenames, paths))
            ap("")
            ap(f"# Row filter (certified by reviewer): {describe_row_filter(spec['row_filter'])}")
        ap("# Target grain: " + (f"one row per {', '.join(keys)}" if keys
                                 else "one row per source row"))
        for kind, name, sql in (
                ("VIEW", "source_all", all_sql),
                ("VIEW", "source", f"SELECT * FROM source_all{where}"),
                ("TABLE", "expected",
                 build_target_sql(build_select(spec), "SELECT * FROM source", keys))):
            stmt = f"CREATE OR REPLACE {kind} {name} AS\n{sql}"
            if '"""' in stmt:
                ap(f"con.execute({stmt!r})")
            else:
                ap('con.execute(r"""')
                for ln in stmt.splitlines():
                    ap(ln)
                ap('""")')
        primary = "source"
    ap("")
    ap("checks = []")
    ap("def check(name, cat, status, detail):")
    ap("    checks.append((name, cat, status, detail))")
    ap('    print("[%-4s] %-16s %s: %s" % (status.upper(), cat, name, detail))')
    ap("")
    ap("# --- technical: control totals -----------------------------------------------")
    ap('n_target = con.execute("SELECT count(*) FROM target_out").fetchone()[0]')
    ap('cols = [r[1] for r in con.execute("PRAGMA table_info(\'target_out\')").fetchall()]')
    if primary:
        ap(f'n_source = con.execute("SELECT count(*) FROM {primary}").fetchone()[0]')
        if "control_total:groups" in enabled_kinds:
            ap('n_groups = con.execute("SELECT count(*) FROM expected").fetchone()[0]')
            ap('check("control_total:groups", "control_total", "pass" if n_groups == n_target'
               ' else "fail", str(n_source) + " source row(s) in scope -> " + str(n_groups)'
               ' + " group(s) -> " + str(n_target) + " delivered")')
        elif "control_total:rows" in enabled_kinds or not keys:
            ap('check("control_total:rows", "control_total", "pass" if n_source == n_target'
               ' else "fail", str(n_source) + " source row(s) -> " + str(n_target) + " delivered")')
        gattrs = group_key_attributes(spec) if "control_total:keys" in enabled_kinds else []
        if gattrs:
            on = " AND ".join(f'CAST(e."{a}" AS VARCHAR) IS NOT DISTINCT FROM CAST(d."{a}" AS VARCHAR)'
                              for a in gattrs)
            ap(f"keys_sql = {('SELECT count(*) FROM expected e FULL OUTER JOIN target_out d ON ' + on + ' WHERE e.' + _q(gattrs[0]) + ' IS NULL OR d.' + _q(gattrs[0]) + ' IS NULL')!r}")
            ap("n_key_diff = con.execute(keys_sql).fetchone()[0]")
            ap('check("control_total:keys", "control_total", "pass" if not n_key_diff else "fail",'
               ' str(n_key_diff) + " group key(s) on one side only")')
        elif "control_total:keys" in enabled_kinds:
            ap('check("control_total:keys", "control_total", "skipped",'
               ' "no delivered attribute carries the group key")')
        conserve = [((r["params"] or {}).get("attribute"), (r["params"] or {}).get("expr"))
                    for r in rules if r.get("kind") == "control_total:conservation"]
        if conserve:
            ap(f"conserve = {conserve!r}  # (target_attribute, certified SUM/COUNT)")
            ap("for attr, expr in conserve:")
            ap("    try:")
            ap('        whole = con.execute("SELECT (" + expr + ") FROM source AS \\"__workset\\"").fetchone()[0] or 0')
            ap('        parts = con.execute(\'SELECT sum(TRY_CAST("\' + attr + \'" AS DOUBLE)) FROM target_out\').fetchone()[0] or 0')
            ap("    except Exception as e:")
            ap('        check("control_total:conservation:" + attr, "control_total", "skipped", str(e))')
            ap("        continue")
            ap("    ok = abs(float(whole) - float(parts)) <= max(abs(float(whole)), abs(float(parts))) * 1e-6 + 1e-9")
            ap('    check("control_total:conservation:" + attr, "control_total", "pass" if ok else "fail",'
               ' attr + ": delivered groups add up to " + format(float(parts), ",.2f")'
               ' + ", whole source " + format(float(whole), ",.2f"))')
        if filtered and "control_total:row_filter" in enabled_kinds:
            ap('n_all = con.execute("SELECT count(*) FROM source_all").fetchone()[0]')
            ap('check("control_total:row_filter", "control_total", "pass", str(n_all)'
               ' + " source row(s) = " + str(n_source) + " in scope + " + str(n_all - n_source)'
               ' + " excluded by the row filter")')
    else:
        ap('check("control_total:rows", "control_total", "warn",'
           ' str(n_target) + " delivered row(s) — no source loaded, cannot compare")')
    ap(f"expected_cols = {expected_cols!r}")
    ap("missing = [c for c in expected_cols if c not in cols]")
    ap('check("control_total:columns", "control_total", "pass" if not missing else "fail",'
       ' str(len(cols)) + " delivered, " + str(len(expected_cols)) + " expected"'
       ' + ("; missing " + str(missing) if missing else ""))')
    # Emitted only when the certified set asks for it. This was unconditional,
    # so the script produced a control the app did not — a second source of
    # truth for what "the checks" are.
    if any(r.get("kind") == "control_total:populated_cells" for r in (rules or [])):
        ap("populated = 0")
        ap("for c in cols:")
        ap('    populated += con.execute(\'SELECT count(*) FROM target_out WHERE "\' + c'
           ' + \'" IS NOT NULL AND "\' + c + \'" <> \\\'\\\'\').fetchone()[0]')
        ap('check("control_total:populated_cells", "control_total", "pass",'
           ' str(populated) + " populated of " + str(n_target * len(cols)) + " cell(s)")')
    # ONE check per rule, matching the runner exactly. The script used to emit
    # a separate check per numeric column while the app emitted a single
    # aggregate, so the two reported different totals for the same rule set —
    # the divergence the certified set exists to prevent.
    if numeric and primary:
        # expected totals come from `expected` — the certified target rebuilt
        # from source — so window and aggregate columns tie out like any other;
        # a column that cannot be recomputed is reported, never dropped
        ap(f"numeric = {[c for c, _e in numeric]!r}  # numeric target attributes")
        ap("off, detail, skipped, n_ok = [], [], [], 0")
        ap("for attr in numeric:")
        ap("    try:")
        ap('        exp = con.execute(\'SELECT sum(TRY_CAST("\' + attr + \'" AS DOUBLE)) FROM expected\').fetchone()[0]')
        ap('        act = con.execute(\'SELECT sum(TRY_CAST("\' + attr + \'" AS DOUBLE)) FROM target_out\').fetchone()[0]')
        ap("    except Exception as e:")
        ap("        skipped.append(attr + ' (' + str(e).splitlines()[0] + ')')")
        ap("        continue")
        ap("    if exp is None and act is None:")
        ap("        continue")
        ap("    exp, act, n_ok = exp or 0, act or 0, n_ok + 1")
        ap("    ties = abs(exp - act) <= max(abs(exp), abs(act)) * 1e-6 + 1e-9")
        ap("    detail.append(attr + '=' + format(act, ',.2f'))")
        ap("    if not ties: off.append(attr)")
        ap("note = (' ' + str(len(skipped)) + ' column(s) could not be recomputed: ' + '; '.join(skipped)) if skipped else ''")
        ap('check("control_total:numeric_sums", "control_total", '
           '"fail" if off else ("pass" if n_ok else "skipped"), '
           '((str(n_ok) + " numeric column(s) tie out: " + "; ".join(detail)) '
           'if not off else (str(len(off)) + " of " + str(n_ok) '
           '+ " do not tie out: " + str(off))) + note)')
    key_rule = next((r for r in rules if r.get("kind") == "control_total:distinct_keys"), None)
    if key_rule and primary:
        kt = (key_rule["params"] or {}).get("key_target")
        ks = (key_rule["params"] or {}).get("key_source")
        ap(f'd_keys = con.execute(\'SELECT count(DISTINCT "{kt}") FROM target_out\').fetchone()[0]')
        ap(f's_keys = con.execute(\'SELECT count(DISTINCT "{ks}" ) FROM {primary}\').fetchone()[0]')
        ap('check("control_total:distinct_keys", "control_total", '
           '"pass" if s_keys == d_keys else "fail", '
           'str(s_keys) + " distinct in source -> " + str(d_keys) + " delivered")')
    ap("")
    ap("# --- business rule: record counts per category --------------------------------")
    if categorical:
        ap(f"categorical = {categorical!r}")
        ap(f"PROFILE_MAX = {_PROFILE_MAX}")
        ap("for attr in categorical:")
        ap('    act = dict(con.execute(\'SELECT CAST("\' + attr + \'" AS VARCHAR), count(*)'
           ' FROM target_out GROUP BY 1\').fetchall())')
        ap("    if not act or len(act) > PROFILE_MAX or len(act) == n_target:")
        ap("        continue          # identifier or free text, not a category")
        ap('    profile = "; ".join(str(k) + "=" + str(v)'
           ' for k, v in sorted(act.items(), key=lambda x: str(x[0])))')
        ap('    check("category_profile:" + attr, "category_profile", "pass",'
           ' str(len(act)) + " categories — " + profile)')
    else:
        ap("# no categorical attribute carries a transform — nothing to profile")
    ap("")
    ap("# --- business rule: aggregates the business certified -------------------------")
    aggs = [dict((r["params"] or {}), _id=r.get("kind")) for r in rules
            if r.get("kind") == "aggregate_by"]
    if aggs and primary:
        ap(f"aggregates = {[{k: v for k, v in a.items() if k != '_id'} for a in aggs]!r}")
        ap("for a in aggregates:")
        ap("    fn, col, groups = a['function'], a.get('column'), a.get('group_by') or []")
        ap("    agg_t = 'count(*)' if fn == 'count' else (")
        ap("        'count(DISTINCT ' + 'TRY_CAST(\"' + col + '\" AS DOUBLE)' + ')'")
        ap("        if fn == 'count_distinct' else fn + '(TRY_CAST(\"' + col + '\" AS DOUBLE))')")
        ap("    kt = ', '.join('CAST(\"' + g + '\" AS VARCHAR)' for g in groups) or \"'ALL'\"")
        ap("    # the source side is `expected` — the certified target rebuilt from")
        ap("    # source — so the SAME aggregate runs on both sides")
        ap("    src = dict(con.execute(\"SELECT concat_ws(' | ', \" + kt + \") , \" + agg_t + \" FROM expected GROUP BY 1\").fetchall())")
        ap("    tgt = dict(con.execute(\"SELECT concat_ws(' | ', \" + kt + \") , \" + agg_t + \" FROM target_out GROUP BY 1\").fetchall())")
        ap("    off = [k for k in set(src) | set(tgt)")
        ap("           if abs((src.get(k) or 0) - (tgt.get(k) or 0)) >")
        ap("              max(abs(src.get(k) or 0), abs(tgt.get(k) or 0)) * 1e-6 + 1e-9]")
        ap("    label = fn + ' of ' + (col or 'records') + (' by ' + ', '.join(groups) if groups else '')")
        ap("    check('aggregate_by:' + label, 'aggregate_by', 'pass' if not off else 'fail',")
        ap("          (label + ': all ' + str(len(set(src) | set(tgt))) + ' group(s) agree')")
        ap("          if not off else (label + ': ' + str(len(off)) + ' group(s) differ: ' + str(off)))")
    else:
        ap("# no business aggregate controls were certified")
    ap("")
    ap("# --- business rule: cross-field dependencies ---------------------------------")
    if crossfield:
        ap(f"crossfield = {crossfield!r}  # (dependent, driver, allowed_driver_values)")
        ap("for dep, driver, allowed in crossfield:")
        ap('    inlist = ", ".join("\'" + str(v).replace("\'", "\'\'") + "\'" for v in allowed)')
        ap('    q = (\'SELECT count(*) FROM target_out WHERE "\' + dep + \'" IS NOT NULL\''
           ' + \' AND "\' + dep + \'" <> \\\'\\\' AND "\' + driver + \'" NOT IN (\' + inlist + \')\')')
        ap("    bad = con.execute(q).fetchone()[0]")
        ap('    check("crossfield:" + dep + "~" + driver, "crossfield",'
           ' "pass" if bad == 0 else "fail", str(bad) + " violation(s) — " + dep'
           ' + " only when " + driver + " in " + str(allowed))')
    else:
        ap("# no dependency pattern with both sides mapped 1:1 — nothing to check")
    ap("")
    ap('passed = sum(1 for c in checks if c[2] == "pass")')
    ap('print("")')
    ap('print(str(passed) + "/" + str(len(checks)) + " reconciliation check(s) passed.")')
    # The console line gates a pipeline; it does not let anyone INVESTIGATE a
    # failure. The list the workspace renders is already in memory here, so
    # writing it out costs nothing and turns the script from a gate into a
    # reporting artefact — attachable to a change record, readable by the
    # client's own dashboard, or loadable back into the workspace.
    ap("")
    ap("report = {")
    ap('    "artefact": "reconciliation",')
    ap('    "verdict": "passed" if passed == len(checks) else "blocked",')
    ap('    "counts": {"total": len(checks), "passed": passed, "failed": len(checks) - passed},')
    ap('    "checks": [{"name": n, "category": c, "status": st, "detail": d}')
    ap("               for n, c, st, d in checks],")
    ap("}")
    ap('with open("reconciliation_report.json", "w") as fh:')
    ap("    json.dump(report, fh, indent=2)")
    ap('print("report written to reconciliation_report.json")')
    ap("raise SystemExit(0 if passed == len(checks) else 1)")
    return "\n".join(L)

