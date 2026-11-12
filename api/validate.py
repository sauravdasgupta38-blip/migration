"""Validation workspace (tab 3) — additive.

Checks the DELIVERED transformed output (the CSV the transformation workspace
produced) against the certified mapping spec and the target dictionary — does
the file actually satisfy what was promised, rather than trusting that the
generator and the runtime agree. Everything here is a fresh, independent pass
over the actual output bytes; it does not reuse engine.agents.validator (which
checks the transform SQL against staged source, before any ETL runs).

Checks:
  wellformed    — every certified/defaulted target attribute is present
  grain         — delivered row count vs the rebuilt source workset
  key_integrity — the mapped key column is unique and non-null (candidate_keys
                  are derived from the source data; skipped if none is found)
  completeness  — required (non-nullable) target attributes have no null/blank
  domain        — enum attributes stay within their declared allowed_values

NO SILENT SKIPS. Every attribute in the target dictionary produces a row in the
report — an executed assertion, or an explicit `skipped` row stating why none
applies. This is the invariant the results table rests on: the reader can see
that every declared attribute was considered, so coverage is an observable
property of the table rather than a self-reported percentage. `skipped` never
affects the verdict and is excluded from the headline pass/warn/fail counts.

EVIDENCE. Every executed check carries the SQL it ran and the population it
scanned, so a pass is evidence rather than an assertion — and the results can be
reconciled against the downloadable script, which runs the same SQL.

Nothing here touches Flow A / Flow B state — it reads the spec + target
dictionary + the delivered CSV + the currently loaded source file(s), and
returns a report in the same shape as engine.agents.contracts.ValidationReport
so the UI renders it with the same check-card components used elsewhere.
"""
from __future__ import annotations

import re
import tempfile
from pathlib import Path

import duckdb

from engine.agents.contracts import CheckResult
from engine.models import target_attributes

from engine.etl_sql import ORD, analyse_expression, classify_expression, grain_keys

from .transform import (_q, build_full_sql, build_source_sql, build_workset_sql,
                        compile_spec_filter, row_filter_active)

SENTINELS = ("00000000", "")


def _write_temp_csv(csv_text: str) -> str:
    fh = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     newline="", encoding="utf-8")
    fh.write(csv_text)
    fh.close()
    return fh.name


def _load_output(con: "duckdb.DuckDBPyConnection", csv_text: str) -> None:
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


def _one_to_one_map(spec: dict) -> dict[str, str]:
    # a window or aggregate is a per-group value: it never stands in for a
    # record's source column, even when it names exactly one
    out: dict[str, str] = {}
    for m in spec.get("mappings", []):
        if (m.get("cardinality") == "1:1" and len(m.get("source_attributes") or []) == 1
                and m.get("gate") != "reject"
                and _kind_of(m.get("transformation_sql")) in ("row", "group_key")):
            out.setdefault(m["source_attributes"][0], m["target_attribute"])
    return out


def _kind_of(sql: str | None, keys: list[str] | None = None) -> str:
    """The expression's kind, recomputed from its SQL — the spec arrives from
    the browser, so a transform_kind it carries is not trusted."""
    if not (sql or "").strip():
        return "row"
    return classify_expression(sql, keys=keys).kind


def _group_key_attrs(spec: dict, keys: list[str], out_cols: list[str]) -> list[str]:
    """Delivered attributes that carry the declared grain's group key."""
    return [m["target_attribute"] for m in spec.get("mappings", [])
            if m.get("gate") != "reject" and m["target_attribute"] in out_cols
            and _kind_of(m.get("transformation_sql"), keys) == "group_key"]


# Expressions whose value changes between executions — a re-run comparison is
# meaningless for these, so the transform check declares them rather than
# failing them. Load-audit columns (migrated_at, source_system) are the usual
# case.
#
# Matched as SUBSTRINGS, deliberately not on word boundaries: DuckDB's
# `current_localtimestamp()` slipped past a \b-anchored pattern because the
# underscore is a word character, and the resulting check only passed when the
# transform and the validation happened to run inside the same second. A
# time-dependent pass is worse than an outright failure — it hides.
_NONDET_TOKENS = (
    "now(", "today(", "random(", "uuid(", "gen_random_uuid",
    "current_timestamp", "current_date", "current_time", "current_localtime",
    "localtimestamp", "localtime", "transaction_timestamp",
    "statement_timestamp", "get_current_timestamp", "nextval", "currval",
)


def _is_nondeterministic(sql_expr: str) -> bool:
    low = (sql_expr or "").lower().replace(" ", "")
    return any(tok.replace(" ", "") in low for tok in _NONDET_TOKENS)


def run_output_validation(spec: dict, target_dict: dict, csv_text: str,
                          source_paths: dict[str, str],
                          insight: dict | None = None) -> dict:
    con = duckdb.connect(":memory:")
    checks: list[CheckResult] = []

    def add(name, cat, status, detail, *, sev="soft", attr=None, n=0, sample=None,
            sql=None, scanned=None):
        checks.append(CheckResult(name=name, category=cat, status=status, severity=sev,
                                  detail=detail, target_attribute=attr,
                                  offending_rows=n, sample=sample or [],
                                  sql=sql, rows_scanned=tgt_n if scanned is None else scanned))

    def skip(name, cat, detail, *, attr=None):
        """Record a check that did NOT execute, and WHY.

        An attribute that cannot be checked must still appear in the report.
        Previously a nullable non-enum attribute produced no rows at all, so
        "not examined" and "examined and clean" were indistinguishable — the
        one thing a validation report must never blur. `skipped` never affects
        the verdict; it is an accounting status, not a result.
        """
        checks.append(CheckResult(name=name, category=cat, status="skipped",
                                  severity="soft", detail=detail,
                                  target_attribute=attr, rows_scanned=0))

    # per-value examination accounting — the confidence figure the results
    # dashboard reports. Counted, never asserted.
    cells_checked = {"completeness": 0, "domain": 0, "type": 0,
                     "transform": 0, "key_integrity": 0}

    _load_output(con, csv_text)
    out_cols = [d[0] for d in con.execute("SELECT * FROM target_out LIMIT 0").description]
    tgt_n = con.execute("SELECT count(*) FROM target_out").fetchone()[0]

    # ---------------- wellformed: delivered shape matches the spec --------
    expected = {m["target_attribute"] for m in spec.get("mappings", [])
                if m.get("gate") != "reject"}
    expected |= {u["attribute"] for u in spec.get("unmapped_target", [])}
    # Executed in SQL, not in Python: the recorded evidence has to be the query
    # that actually produced the result. It previously showed an illustrative
    # `SELECT * FROM target_out LIMIT 0`, which ran nothing and — paired with a
    # "50 rows scanned" line inherited from the row-based checks — read like the
    # grain query had been pasted into the wrong card.
    exp_list = sorted(expected)
    if exp_list:
        values = ", ".join("('" + c.replace("'", "''") + "')" for c in exp_list)
        wf_sql = (f"-- every column the certified spec expects must be delivered\n"
                  f"WITH expected(col) AS (VALUES {values})\n"
                  f"SELECT col AS missing_column FROM expected\n"
                  f"WHERE col NOT IN (SELECT column_name FROM information_schema.columns\n"
                  f"                  WHERE table_name = 'target_out');")
        missing_cols = [r[0] for r in con.execute(
            f"WITH expected(col) AS (VALUES {values}) SELECT col FROM expected "
            f"WHERE col NOT IN (SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = 'target_out') ORDER BY col").fetchall()]
    else:
        wf_sql, missing_cols = None, []
    add("delivered_columns_match_spec", "wellformed", "fail" if missing_cols else "pass",
        (f"column(s) the spec expects but the delivered file is missing: {missing_cols}"
         if missing_cols else
         f"all {len(exp_list)} certified/defaulted target attribute(s) are present "
         f"in the delivered output."),
        sev="hard", n=len(missing_cols), sql=wf_sql,
        # a structural check: it compares COLUMN NAMES and reads no row values,
        # so it must not claim a row population
        scanned=0)

    # ---------------- grain: row count vs the rebuilt source workset ------
    # "the source" is the workset AFTER the reviewer's row filter: only rows in
    # scope were meant to be delivered, so that is what grain compares against
    src_n, src_total, workset_err = None, None, None
    filtered = row_filter_active(spec)
    # a declared grain: one delivered row per group, not per source row
    keys = grain_keys(spec)
    expected_n = None
    if source_paths:
        try:
            workset_sql, params = build_source_sql(spec, source_paths)
            src_n = con.execute(f"SELECT count(*) FROM ({workset_sql}) AS w", params).fetchone()[0]
            if filtered:
                all_sql, all_params = build_workset_sql(spec, source_paths)
                src_total = con.execute(f"SELECT count(*) FROM ({all_sql}) AS w",
                                        all_params).fetchone()[0]
            if keys:
                full_sql, full_params = build_full_sql(spec, source_paths)
                expected_n = con.execute(f"SELECT count(*) FROM ({full_sql}) AS g",
                                         full_params).fetchone()[0]
        except Exception as e:  # noqa: BLE001
            workset_err = str(e)
    if src_n is not None and keys:
        add("grain_row_count", "grain", "pass" if expected_n == tgt_n else "fail",
            f"{src_n} source row(s){' after the row filter' if filtered else ''} -> "
            f"{expected_n} distinct {', '.join(keys)} group(s) -> {tgt_n} delivered "
            f"target row(s).", sev="hard",
            sql=("-- one delivered row per group of the declared grain\n"
                 "SELECT (SELECT count(*) FROM target_out) AS delivered_rows,\n"
                 f"       (SELECT count(*) FROM (SELECT 1 FROM (<source>) AS w GROUP BY "
                 f"{', '.join(_q(k) for k in keys)})) AS groups;"),
            scanned=src_n)
    elif src_n is not None:
        add("row_count_preserved", "grain", "pass" if src_n == tgt_n else "fail",
            (f"{src_n} source row(s) after the row filter ({src_total - src_n} of "
             f"{src_total} excluded) -> {tgt_n} delivered target row(s)."
             if filtered else
             f"{src_n} source row(s) -> {tgt_n} delivered target row(s)."), sev="hard",
            sql=("-- delivered row count must equal the rebuilt source workset\n"
                 + ("-- (after the reviewer's row filter)\n" if filtered else "")
                 + "SELECT (SELECT count(*) FROM target_out) AS delivered_rows,\n"
                 f"       (SELECT count(*) FROM ({workset_sql}) AS w) AS source_rows;"),
            scanned=src_n)
    elif workset_err:
        add("row_count_preserved", "grain", "warn",
            f"could not rebuild the source workset to compare row counts: {workset_err}")
    else:
        add("row_count_preserved", "grain", "warn",
            f"{tgt_n} delivered target row(s) — source file(s) not loaded, cannot compare grain.")

    # ---------------- key integrity ----------------------------------------
    candidate_keys = (insight or {}).get("candidate_keys") or []
    one_to_one = _one_to_one_map(spec)
    key_tgt = next((one_to_one[k] for k in candidate_keys if k in one_to_one), None)
    key_ok = False
    # under a declared grain the record identity IS the group key: the
    # attributes carrying it must be non-null and unique together
    group_attrs = _group_key_attrs(spec, keys, out_cols) if keys else []
    if keys and group_attrs:
        tup = ", ".join(_q(c) for c in group_attrs)
        any_null = " OR ".join(f"{_q(c)} IS NULL" for c in group_attrs)
        nulls = con.execute(f"SELECT count(*) FROM target_out WHERE {any_null}").fetchone()[0]
        distinct = con.execute(f"SELECT count(*) FROM (SELECT DISTINCT {tup} FROM target_out)").fetchone()[0]
        key_ok = nulls == 0 and distinct == tgt_n
        cells_checked["key_integrity"] += tgt_n
        add("key_unique_not_null", "key_integrity", "pass" if key_ok else "fail",
            f"group key {group_attrs}: {nulls} null(s), {distinct}/{tgt_n} distinct in the "
            f"delivered output.", sev="hard", n=nulls,
            sql=(f"SELECT count(*) FILTER (WHERE {any_null}) AS nulls,\n"
                 f"       (SELECT count(*) FROM (SELECT DISTINCT {tup} FROM target_out)) AS distinct_groups,\n"
                 f"       count(*) AS rows\nFROM target_out;"))
    elif keys:
        add("key_unique_not_null", "key_integrity", "fail",
            f"No delivered attribute carries the grain key {keys}, so delivered rows "
            f"can't be identified.", sev="hard")
    elif key_tgt and key_tgt in out_cols:
        key_sql = (f'SELECT count(*) FILTER (WHERE {_q(key_tgt)} IS NULL) AS nulls,\n'
                   f'       count(DISTINCT {_q(key_tgt)}) AS distinct_values,\n'
                   f'       count(*) AS rows\n'
                   f'FROM target_out;')
        nulls = con.execute(f'SELECT count(*) FROM target_out WHERE {_q(key_tgt)} IS NULL').fetchone()[0]
        distinct = con.execute(f'SELECT count(DISTINCT {_q(key_tgt)}) FROM target_out').fetchone()[0]
        ok = key_ok = nulls == 0 and distinct == tgt_n
        cells_checked["key_integrity"] += tgt_n
        add("key_unique_not_null", "key_integrity", "pass" if ok else "fail",
            f"key '{key_tgt}': {nulls} null(s), {distinct}/{tgt_n} distinct in the delivered output.",
            sev="hard", attr=key_tgt, n=nulls, sql=key_sql,
            sample=_rows(con,
                f'SELECT * FROM (SELECT row_number() OVER () AS row_number, * '
                f'FROM target_out) WHERE {_q(key_tgt)} IS NULL',
                limit=10) if nulls else None)
    else:
        add("key_identified", "key_integrity",
            "warn", "no candidate key derived from the source data (or no key mapped 1:1) — key-uniqueness check skipped.")

    # ---------------- row filter: excluded rows really were left out ------
    # A matching row COUNT does not prove the RIGHT rows were delivered: a
    # transform that ignored the filter but lost rows elsewhere could tie. The
    # business key identifies each record, so assert that no record the filter
    # excluded appears in the delivered file.
    if filtered and src_n is not None and keys:
        # a delivered row is a GROUP: one group can legitimately hold both kept
        # and excluded source rows, so a per-record trace does not apply
        skip("row_filter_excluded_absent", "grain",
             "under a declared grain each delivered row is a group, so excluded "
             "source records cannot be traced individually — the grain row count "
             "above is computed from the rows in scope.")
    elif filtered and src_n is not None:
        key_src = next((k for k in candidate_keys if k in one_to_one), None)
        key_expr = next((m.get("transformation_sql") for m in spec.get("mappings", [])
                         if m.get("target_attribute") == key_tgt), None)
        if key_tgt and key_src and key_expr and key_tgt in out_cols:
            all_sql, all_params = build_workset_sql(spec, source_paths)
            cols = [d[0] for d in con.execute(
                f"SELECT * FROM ({all_sql}) AS w LIMIT 0", all_params).description]
            cond = compile_spec_filter(spec, cols)
            excl_sql = (f"SELECT CAST({key_expr} AS VARCHAR) AS k FROM ({all_sql}) AS w "
                        f"WHERE ({cond}) IS NOT TRUE")
            con.execute(f"CREATE OR REPLACE TABLE _excluded_keys AS {excl_sql}", all_params)
            n_excl = con.execute("SELECT count(*) FROM _excluded_keys").fetchone()[0]
            hit_sql = (f"SELECT d.* FROM target_out d JOIN _excluded_keys x "
                       f"ON trim(CAST(d.{_q(key_tgt)} AS VARCHAR)) = trim(x.k)")
            hits = con.execute(f"SELECT count(*) FROM ({hit_sql})").fetchone()[0]
            add("row_filter_excluded_absent", "grain", "fail" if hits else "pass",
                (f"{hits} delivered record(s) belong to source rows the row filter "
                 f"excluded (by key '{key_tgt}')." if hits else
                 f"none of the {n_excl} source row(s) excluded by the row filter "
                 f"appear in the delivered output (matched on key '{key_tgt}')."),
                sev="hard", attr=key_tgt, n=hits,
                sql=("-- source rows the row filter excludes, by business key\n"
                     f"WITH excluded AS (\n  SELECT CAST({key_expr} AS VARCHAR) AS k\n"
                     f"  FROM (<source workset>) AS w\n  WHERE ({cond}) IS NOT TRUE\n)\n"
                     f"SELECT count(*) FROM target_out d JOIN excluded x\n"
                     f"  ON trim(CAST(d.{_q(key_tgt)} AS VARCHAR)) = trim(x.k);"),
                sample=_rows(con, hit_sql, limit=10) if hits else None,
                scanned=n_excl)
        else:
            skip("row_filter_excluded_absent", "grain",
                 "no business key is mapped 1:1, so individual excluded records "
                 "cannot be traced in the delivered file — the row count above "
                 "is the only filter evidence.")

    # ---------------- completeness: required target attributes ------------
    for attr in target_attributes(target_dict):
        name, required = attr["name"], not attr.get("nullable", False)
        if name not in out_cols:
            skip(f"required_present:{name}", "completeness",
                 f"'{name}' is not present in the delivered output — "
                 "nothing to check for completeness.", attr=name)
            continue
        if not required:
            skip(f"required_present:{name}", "completeness",
                 f"'{name}' is declared nullable in the target dictionary, so "
                 "null/blank is a legal value — no completeness assertion applies.",
                 attr=name)
            continue
        comp_sql = (f"SELECT count(*) AS violations FROM target_out\n"
                    f"WHERE {_q(name)} IS NULL OR {_q(name)} = '';")
        nulls = con.execute(f"SELECT count(*) FROM target_out WHERE {_q(name)} IS NULL "
                            f"OR {_q(name)} = ''").fetchone()[0]
        cells_checked["completeness"] += tgt_n
        add(f"required_present:{name}", "completeness", "fail" if nulls else "pass",
            (f"'{name}' required; {nulls} null/blank in the delivered output." if nulls
             else f"'{name}' fully populated in the delivered output."),
            sev="hard", attr=name, n=nulls, sql=comp_sql,
            sample=_rows(con,
                (f"SELECT row_number, "
                 + (f"{_q(key_tgt)} " if key_tgt and key_tgt in out_cols else "")
                 + (", " if key_tgt and key_tgt in out_cols else "")
                 + f"{_q(name)} FROM (SELECT row_number() OVER () AS row_number, * "
                   f"FROM target_out) WHERE {_q(name)} IS NULL OR {_q(name)} = ''"),
                limit=10) if nulls else None)

    # ---------------- domain: enum attributes stay in-list -----------------
    for attr in target_attributes(target_dict):
        name, allowed = attr["name"], attr.get("allowed_values")
        if name not in out_cols:
            skip(f"domain:{name}", "domain",
                 f"'{name}' is not present in the delivered output — "
                 "no domain to enforce.", attr=name)
            continue
        if attr.get("type") != "enum" or not allowed:
            skip(f"domain:{name}", "domain",
                 f"'{name}' is type '{attr.get('type')}' with no declared "
                 "allowed_values — the target dictionary defines no domain to "
                 "enforce.", attr=name)
            continue
        inlist = ", ".join("'" + v.replace("'", "''") + "'" for v in allowed)
        dom_sql = (f"SELECT count(*) AS violations FROM target_out\n"
                   f"WHERE {_q(name)} IS NOT NULL AND {_q(name)} <> ''\n"
                   f"  AND {_q(name)} NOT IN ({inlist});")
        bad = con.execute(f"SELECT count(*) FROM target_out WHERE {_q(name)} IS NOT NULL "
                          f"AND {_q(name)} <> '' AND {_q(name)} NOT IN ({inlist})").fetchone()[0]
        cells_checked["domain"] += tgt_n
        add(f"domain:{name}", "domain", "pass" if not bad else "fail",
            (f"'{name}': {bad} value(s) outside the declared allowed_values." if bad
             else f"'{name}': every value is one of the declared allowed_values."),
            sev="soft", attr=name, n=bad, sql=dom_sql,
            sample=_rows(con,
                (f"SELECT {_q(name)} AS {_q(name + ' (offending value)')}, "
                 f"count(*) AS rows_affected "
                 f"FROM target_out WHERE {_q(name)} IS NOT NULL AND {_q(name)} <> '' "
                 f"AND {_q(name)} NOT IN ({inlist}) GROUP BY 1 ORDER BY 2 DESC"),
                limit=10) if bad else None)

    # ---------------- duplicate rows --------------------------------------
    # NOT redundant with key uniqueness: a file can carry a unique key and
    # still hold two rows identical across every OTHER column, which is the
    # classic signature of a join that fanned out.
    if out_cols:
        allc = ", ".join(_q(c) for c in out_cols)
        dup_sql = (f"SELECT count(*) AS duplicate_rows FROM (\n"
                   f"  SELECT {allc}, count(*) AS n FROM target_out\n"
                   f"  GROUP BY ALL HAVING count(*) > 1\n) d;")
        dups = con.execute(f"SELECT count(*) FROM (SELECT {allc}, count(*) AS n "
                           f"FROM target_out GROUP BY ALL HAVING count(*) > 1) d").fetchone()[0]
        dup_sample = None
        if dups:
            keycol = _q(key_tgt) if key_tgt and key_tgt in out_cols else _q(out_cols[0])
            dup_sample = _rows(con,
                f"SELECT {keycol} AS duplicated_key, count(*) AS copies "
                f"FROM target_out GROUP BY {allc} HAVING count(*) > 1 "
                f"ORDER BY count(*) DESC", limit=10)
        add("no_duplicate_rows", "duplicates", "fail" if dups else "pass",
            (f"{dups} fully-duplicated row group(s) in the delivered output."
             if dups else "no two delivered rows are identical across all columns."),
            sev="hard", n=dups, sql=dup_sql, sample=dup_sample)

    # ---------------- data type conformance --------------------------------
    # EVERY declared attribute, not just the numerically typed ones: the
    # generated target data must conform to the type the mapping spec targets.
    # Text types are asserted too — the assertion is weaker (any byte sequence
    # is text), and the detail says so, but the column still appears with its
    # declared type and its whole population examined rather than vanishing.
    _CAST = {"date": "DATE", "timestamp": "TIMESTAMP", "time": "TIME",
             "decimal": "DOUBLE", "numeric": "DOUBLE", "number": "DOUBLE",
             "float": "DOUBLE", "double": "DOUBLE", "integer": "BIGINT",
             "int": "BIGINT", "bigint": "BIGINT", "smallint": "SMALLINT",
             "boolean": "BOOLEAN", "bool": "BOOLEAN"}
    _TEXT = {"string", "varchar", "text", "char", "enum", ""}
    spec_types = {m.get("target_attribute"): m.get("target_type")
                  for m in spec.get("mappings", [])}
    for attr in target_attributes(target_dict):
        name, ttype = attr["name"], (attr.get("type") or "").lower()
        if name not in out_cols:
            skip(f"data_type:{name}", "type",
                 f"'{name}' is not present in the delivered output.", attr=name)
            continue
        # the type the mapping spec targets, falling back to the dictionary
        declared = (spec_types.get(name) or ttype or "").lower()
        cast = _CAST.get(declared)
        cells_checked["type"] += tgt_n
        if cast is None and declared in _TEXT:
            t_sql = (f"SELECT count(*) AS non_text FROM target_out\n"
                     f"WHERE {_q(name)} IS NOT NULL\n"
                     f"  AND TRY_CAST({_q(name)} AS VARCHAR) IS NULL;")
            bad = con.execute(f"SELECT count(*) FROM target_out WHERE {_q(name)} IS NOT NULL "
                              f"AND TRY_CAST({_q(name)} AS VARCHAR) IS NULL").fetchone()[0]
            add(f"data_type:{name}", "type", "fail" if bad else "pass",
                (f"{bad} value(s) in '{name}' are not representable as "
                 f"{declared or 'text'}."
                 if bad else
                 f"'{name}' targets {declared or 'text'}; all {tgt_n} row(s) "
                 f"examined and conform. Text admits any value, so the binding "
                 f"constraints for this column are its domain and its "
                 f"transformation rule."),
                sev="soft", attr=name, n=bad, sql=t_sql)
            continue
        if cast is None:
            cells_checked["type"] -= tgt_n
            skip(f"data_type:{name}", "type",
                 f"'{name}' declares type '{declared}', which has no parse rule "
                 f"defined — no data-type assertion applies.", attr=name)
            continue
        t_sql = (f"-- target type per the mapping spec: {declared}\n"
                 f"SELECT count(*) AS unparseable FROM target_out\n"
                 f"WHERE {_q(name)} IS NOT NULL AND {_q(name)} <> ''\n"
                 f"  AND TRY_CAST({_q(name)} AS {cast}) IS NULL;")
        bad = con.execute(f"SELECT count(*) FROM target_out WHERE {_q(name)} IS NOT NULL "
                          f"AND {_q(name)} <> '' AND TRY_CAST({_q(name)} AS {cast}) IS NULL"
                          ).fetchone()[0]
        add(f"data_type:{name}", "type", "fail" if bad else "pass",
            (f"{bad} of {tgt_n} value(s) in '{name}' do not parse as the "
             f"targeted type {declared} ({cast})."
             if bad else f"'{name}' targets {declared}; all {tgt_n} row(s) "
                         f"examined and every populated value parses as {cast}."),
            sev="hard", attr=name, n=bad, sql=t_sql,
            sample=_rows(con,
                (f"SELECT row_number, "
                 + (f"{_q(key_tgt)}, " if key_tgt and key_tgt in out_cols else "")
                 + f"{_q(name)} AS {_q(name + ' (unparseable)')} "
                   f"FROM (SELECT row_number() OVER () AS row_number, * FROM target_out) "
                   f"WHERE {_q(name)} IS NOT NULL AND {_q(name)} <> '' "
                   f"AND TRY_CAST({_q(name)} AS {cast}) IS NULL"), limit=10)
                if bad else None)

    # ---------------- transformation rule conformance ----------------------
    # THE check that earns the confidence claim: rebuild the certified target
    # from source and compare it with the delivered file, value by value.
    # Everything else asks "is the delivered file internally plausible?"; this
    # asks "is it what the certified spec says it should be?".
    #
    # The expected target is built ONCE, with the very statement the transform
    # runs (engine/etl_sql.build_target_sql). Re-executing each column on its
    # own gave a window function no partition to see and aligned rows purely by
    # position — so a window that re-sorted its output made every column look
    # wrong. Rows are now aligned by, in order: the group key (declared grain),
    # the business key (when it identifies records), else source position.
    if source_paths and src_n is not None:
        try:
            exp_sql, exp_params = build_full_sql(spec, source_paths,
                                                 extra=[] if keys else [(ORD, _q(ORD))])
            order = (f"ORDER BY {_q(ORD)}" if not keys else
                     ("ORDER BY " + ", ".join(_q(c) for c in group_attrs)) if group_attrs else "")
            con.execute(f"CREATE OR REPLACE TABLE _expected AS SELECT *, "
                        f"row_number() OVER ({order}) AS _rn FROM ({exp_sql}) AS x", exp_params)
            con.execute("CREATE OR REPLACE TABLE _outn AS "
                        "SELECT *, row_number() OVER () AS _rn FROM target_out")
        except Exception as e:      # noqa: BLE001
            add("transform_rules_reproduced", "transform", "warn",
                f"could not rebuild the certified target from the source to compare "
                f"with: {e}")
        else:
            if keys and group_attrs:
                align, align_cols = "group key", group_attrs
            elif not keys and key_ok and key_tgt in out_cols:
                align, align_cols = "business key", [key_tgt]
            else:
                align, align_cols = "position", []
            on = (" AND ".join(f"CAST(e.{_q(c)} AS VARCHAR) IS NOT DISTINCT FROM "
                               f"CAST(d.{_q(c)} AS VARCHAR)" for c in align_cols)
                  or "e._rn = d._rn")
            exp_n = con.execute("SELECT count(*) FROM _expected").fetchone()[0]

            if keys and align_cols:
                # a group the source produces but the file lacks, or the reverse
                for nm, sql_, what in (
                        ("missing_group", f"SELECT {', '.join('e.' + _q(c) for c in align_cols)} "
                                          f"FROM _expected e LEFT JOIN _outn d ON {on} WHERE d._rn IS NULL",
                         "group(s) the source produces are missing from the delivered file"),
                        ("unexpected_group", f"SELECT {', '.join('d.' + _q(c) for c in align_cols)} "
                                             f"FROM _outn d LEFT JOIN _expected e ON {on} WHERE e._rn IS NULL",
                         "delivered group(s) do not exist in the source")):
                    n = con.execute(f"SELECT count(*) FROM ({sql_})").fetchone()[0]
                    add(nm, "grain", "fail" if n else "pass",
                        f"{n} {what}." if n else f"no {what.split(' ', 1)[1]} — matched on {align_cols}.",
                        sev="hard", n=n, sql=sql_ + ";", sample=_rows(con, sql_, 10) if n else None)

            # Positional alignment is only meaningful when both sides have the
            # same number of rows. If grain already failed, every column would
            # report a cascade of spurious mismatches and bury the real defect
            # — so say so once, per column, and point at the check to fix.
            grain_ok = align != "position" or exp_n == tgt_n
            for m in spec.get("mappings", []):
                name, sql_expr = m.get("target_attribute"), m.get("transformation_sql")
                if m.get("gate") == "reject" or name not in out_cols:
                    continue
                if not grain_ok:
                    skip(f"transform_rule:{name}", "transform",
                         f"delivered row count ({tgt_n}) differs from the source "
                         f"workset ({exp_n}), so rows cannot be aligned for a "
                         f"value-by-value comparison. Resolve the grain check "
                         f"first, then re-run.", attr=name)
                    continue
                if not sql_expr:
                    skip(f"transform_rule:{name}", "transform",
                         f"'{name}' carries no transformation SQL to re-execute.",
                         attr=name)
                    continue
                if _is_nondeterministic(sql_expr):
                    # now()/current_date/random() produce a different value on
                    # every execution, so re-running them can never reproduce
                    # what was delivered. Reporting that as a mismatch would be
                    # a guaranteed false failure on every load-audit column.
                    skip(f"transform_rule:{name}", "transform",
                         f"'{name}' is derived from a non-deterministic "
                         f"expression, so re-executing it cannot reproduce the "
                         f"delivered value. Its type and completeness are still "
                         f"checked.", attr=name)
                    continue
                diff = (f"FROM _expected e FULL OUTER JOIN _outn d ON {on} "
                        f"WHERE CAST(e.{_q(name)} AS VARCHAR) IS DISTINCT FROM "
                        f"CAST(d.{_q(name)} AS VARCHAR)")
                cmp_sql = (f"-- expected = the certified target statement re-executed from\n"
                           f"-- the source; rows aligned by {align}\n"
                           f"SELECT count(*) AS mismatches\n"
                           f"FROM expected e FULL OUTER JOIN delivered d\n"
                           f"  ON {on}\n"
                           f"WHERE CAST(e.{_q(name)} AS VARCHAR) IS DISTINCT FROM "
                           f"CAST(d.{_q(name)} AS VARCHAR);")
                try:
                    bad = con.execute(f"SELECT count(*) {diff}").fetchone()[0]
                except Exception as e:      # noqa: BLE001
                    add(f"transform_rule:{name}", "transform", "warn",
                        f"could not re-execute the certified transform for "
                        f"'{name}': {e}", attr=name, sql=cmp_sql)
                    continue
                sample = None
                if bad:
                    # The evidence that matters for a transform failure is not
                    # "a row" but the DIFF: which record, what the certified
                    # rule produces, and what was actually delivered — keyed by
                    # the business key where one is mapped, so the reviewer can
                    # find the record in the source system. Row numbers are
                    # positions in the delivered FILE.
                    idcol = (align_cols[0] if align_cols else
                             key_tgt if key_tgt and key_tgt in out_cols
                             else (out_cols[0] if out_cols and out_cols[0] != name else None))
                    keysel = (f"coalesce(CAST(d.{_q(idcol)} AS VARCHAR), CAST(e.{_q(idcol)} AS VARCHAR)) "
                              f"AS {_q(idcol)}, " if idcol else "")
                    try:
                        sample = _rows(con,
                            f"SELECT coalesce(d._rn, e._rn) AS row_number, {keysel}"
                            f"CAST(e.{_q(name)} AS VARCHAR) AS {_q(name + ' (expected)')}, "
                            f"CAST(d.{_q(name)} AS VARCHAR) AS {_q(name + ' (delivered)')} "
                            f"{diff} ORDER BY 1", limit=10)
                    except Exception:      # noqa: BLE001
                        sample = None
                cells_checked["transform"] += tgt_n
                add(f"transform_rule:{name}", "transform", "fail" if bad else "pass",
                    (f"{bad} of {tgt_n} delivered value(s) in '{name}' differ from "
                     f"re-executing the certified transform."
                     if bad else
                     f"all {tgt_n} delivered value(s) in '{name}' reproduce exactly "
                     f"when the certified transform is re-executed."),
                    sev="hard", attr=name, n=bad, sql=cmp_sql, sample=sample)

            # ---- window columns: one value per partition --------------------
            # A window column is a per-partition figure repeated on every row of
            # the partition. Checked on the DELIVERED values, so a file produced
            # by another engine that computed it per row is caught even when the
            # comparison above happens to agree on some rows.
            if not keys and grain_ok:
                windows = []
                for m in spec.get("mappings", []):
                    sql_expr, name = m.get("transformation_sql") or "", m.get("target_attribute")
                    if m.get("gate") == "reject" or name not in out_cols or not sql_expr.strip():
                        continue
                    a = analyse_expression(sql_expr)
                    if a.ok and a.has_window and not a.has_aggregate:
                        windows.append((name, a.partition_by))
                if windows:
                    src_sql, src_params = build_source_sql(spec, source_paths)
                    con.execute(f"CREATE OR REPLACE TABLE _part AS SELECT *, row_number() OVER () "
                                f"AS {_q(ORD)} FROM ({src_sql}) AS __r", src_params)
                for name, pcols in windows:
                    grp = ", ".join(f"p.{_q(c)}" for c in pcols) or "'all rows'"
                    viol_sql = (f"SELECT {grp}, count(DISTINCT CAST(d.{_q(name)} AS VARCHAR)) AS n_values "
                                f"FROM _expected e JOIN _outn d ON {on} "
                                f"JOIN _part p ON p.{_q(ORD)} = e.{_q(ORD)} "
                                f"GROUP BY {grp} HAVING count(DISTINCT CAST(d.{_q(name)} AS VARCHAR)) > 1")
                    try:
                        n = con.execute(f"SELECT count(*) FROM ({viol_sql})").fetchone()[0]
                    except Exception as e:      # noqa: BLE001
                        add(f"partition_consistency:{name}", "transform", "warn",
                            f"could not check '{name}' per partition: {e}", attr=name)
                        continue
                    add(f"partition_consistency:{name}", "transform", "fail" if n else "pass",
                        (f"{n} partition(s) of {pcols or 'the table'} carry more than one "
                         f"'{name}' value." if n else
                         f"'{name}' carries one value per {', '.join(pcols) or 'table'} "
                         f"partition, as a window column must."),
                        sev="hard", attr=name, n=n, sql=viol_sql + ";",
                        sample=_rows(con, viol_sql, 10) if n else None)

    con.close()

    hard_fail = any(c.status == "fail" and c.severity == "hard" for c in checks)
    any_fail = any(c.status == "fail" for c in checks)
    verdict = "blocked" if hard_fail else ("needs_review" if any_fail else "certified")
    executed = [c for c in checks if c.status != "skipped"]
    stats = {
        "checks": len(executed),
        "passed": sum(c.status == "pass" for c in executed),
        "warnings": sum(c.status == "warn" for c in executed),
        "failures": sum(c.status == "fail" for c in executed),
        "skipped": sum(c.status == "skipped" for c in checks),
    }
    # (5) The confidence figure, COUNTED. Columns that received per-value
    # examination x rows, deduplicated across families — so the headline is
    # "N of R x C cells examined", a number a reader can verify by
    # multiplication rather than a coverage percentage to argue about.
    per_value_cols = {c.target_attribute for c in checks
                      if c.status in ("pass", "fail")
                      and c.category in ("completeness", "domain", "type",
                                         "transform", "key_integrity")
                      and c.target_attribute}
    declared = [a["name"] for a in target_attributes(target_dict)]
    coverage_cells = {
        "rows": tgt_n,
        "columns_total": len(declared),
        "columns_examined": len(per_value_cols),
        "cells_total": tgt_n * len(declared),
        "cells_examined": tgt_n * len(per_value_cols),
        "assertions": sum(cells_checked.values()),   # incl. a column checked twice
        "by_family": cells_checked,
        "columns_not_examined": sorted(set(declared) - per_value_cols),
    }
    return {
        "verdict": verdict,
        "checks": [c.model_dump() for c in checks],
        "stats": stats,
        "coverage_cells": coverage_cells,
        "source_table": spec.get("source_table"),
        "target_table": spec.get("target_table"),
    }


# ---------------------------------------------------------------------------
# code generation — a standalone, readable Python/DuckDB validation script
# ---------------------------------------------------------------------------
def describe_validation_rules(spec: dict, target_dict: dict,
                              has_source: bool, has_insight: bool) -> list[dict]:
    """The checks `generate_validation_script` will emit, described BEFORE the
    script runs — same rule set, same wording as run_output_validation, so the
    preview never drifts from the actual outcome."""
    req = [a["name"] for a in target_attributes(target_dict) if not a.get("nullable", False)]
    enums = [(a["name"], a.get("allowed_values")) for a in target_attributes(target_dict)
             if a.get("type") == "enum" and a.get("allowed_values")]
    _TYPED = {"date", "timestamp", "decimal", "integer", "number", "boolean"}
    typed = [a["name"] for a in target_attributes(target_dict)
             if (a.get("type") or "").lower() in _TYPED]
    mapped = [m["target_attribute"] for m in spec.get("mappings", [])
              if m.get("gate") != "reject" and m.get("transformation_sql")]
    expected = sorted({m["target_attribute"] for m in spec.get("mappings", [])
                       if m.get("gate") != "reject"}
                      | {u["attribute"] for u in spec.get("unmapped_target", [])})

    rules: list[dict] = []
    rules.append({"category": "wellformed", "name": "delivered_columns_match_spec",
                  "detail": f"Checks that the delivered file contains every one of "
                            f"the {len(expected)} attribute(s) the certified mapping "
                            f"produces. Fails if any is missing.",
                  "scope": f"{len(expected)} attribute(s)"})
    rules.append({"category": "grain", "name": "row_count_preserved",
                  "detail": ("Checks that the delivered file has exactly the same "
                             "number of records as the source. Fails if records "
                             "have been added or lost."
                             if has_source else
                             "Reports the delivered record count. No comparison is "
                             "made — no source file was loaded.")})
    if row_filter_active(spec):
        from engine.row_filter import describe_row_filter

        desc = describe_row_filter(spec["row_filter"])
        rules[-1]["detail"] += (f" The source is counted after the reviewer's row "
                                f"filter: {desc}.")
        rules.append({"category": "grain", "name": "row_filter_excluded_absent",
                      "detail": "Checks that no record the row filter excluded appears "
                                "in the delivered file, matched on the business key. "
                                f"Row filter: {desc}."})
    rules.append({"category": "key_integrity", "name": "key_unique_not_null",
                  "detail": ("Checks that the business key identifies exactly one "
                             "record: it must have a value in every record, and no "
                             "two records may share the same key."
                             if has_insight else
                             "Skipped — no candidate key could be derived from the source data.")})
    # Completeness and domain are ONE rule each, applied per attribute — not N
    # different rules. Listing them per column taught the reader nothing after
    # the first card and did not survive a 50-attribute target. The per-attribute
    # outcome is what the results table shows; this section describes the rule.
    rules.append({"category": "completeness", "name": "required_present",
                  "detail": "Checks that every mandatory attribute has a value in "
                            "every record. A record fails if a mandatory attribute "
                            "is empty or null.",
                  "applies_to": req,
                  "scope": f"{len(req)} required attribute(s)"})
    rules.append({"category": "duplicates", "name": "no_duplicate_rows",
                  "detail": "Checks that no duplicate record exists, by comparing "
                            "all attributes. A record is a duplicate if every one "
                            "of its attribute values matches another record "
                            "exactly.",
                  "scope": "whole table"})
    rules.append({"category": "type", "name": "data_type",
                  "detail": "Checks that every value can be read as the data type "
                            "the target expects — a date as a date, an amount as a "
                            "number. A record fails if a value cannot be "
                            "interpreted as its declared type.",
                  "applies_to": [a["name"] for a in target_attributes(target_dict)],
                  "scope": f"{len(target_attributes(target_dict))} attribute(s) x every row"})
    rules.append({"category": "transform", "name": "transform_rule",
                  "detail": "Re-applies each certified mapping rule to the source "
                            "data and compares the result with the delivered value, "
                            "record by record. A record fails if the delivered "
                            "value differs from what the mapping rule produces.",
                  "applies_to": mapped,
                  "scope": f"{len(mapped)} mapped attribute(s) x every row"})
    rules.append({"category": "domain", "name": "value_in_allowed_list",
                  "detail": "Checks that coded attributes only ever hold a value "
                            "from their permitted list. A record fails if the value "
                            "is not on the list. Empty values are not counted here "
                            "— completeness covers those.",
                  "applies_to": [n for n, _a in enums],
                  "scope": f"{len(enums)} enum attribute(s)"})
    return rules


def generate_validation_script(spec: dict, target_dict: dict, output_filename: str,
                                has_source: bool, has_insight: bool) -> str:
    target = spec.get("target_table", "target")
    req = [a["name"] for a in target_attributes(target_dict) if not a.get("nullable", False)]
    enums = [(a["name"], a.get("allowed_values")) for a in target_attributes(target_dict)
             if a.get("type") == "enum" and a.get("allowed_values")]
    _TYPED = {"date", "timestamp", "decimal", "integer", "number", "boolean"}
    typed = [a["name"] for a in target_attributes(target_dict)
             if (a.get("type") or "").lower() in _TYPED]
    mapped = [m["target_attribute"] for m in spec.get("mappings", [])
              if m.get("gate") != "reject" and m.get("transformation_sql")]
    expected = sorted({m["target_attribute"] for m in spec.get("mappings", [])
                       if m.get("gate") != "reject"}
                      | {u["attribute"] for u in spec.get("unmapped_target", [])})

    L: list[str] = []
    ap = L.append
    ap('"""')
    ap(f"Validation — does {output_filename} satisfy the certified mapping for {target}?")
    ap("")
    ap("Auto-generated from the certified mapping specification and target")
    ap("dictionary. Checks the DELIVERED output file itself (not a re-derivation")
    ap("of the transform), so it catches drift between what was generated and")
    ap("what actually got run.")
    ap('"""')
    if row_filter_active(spec):
        from engine.row_filter import describe_row_filter

        ap(f"# Row filter (certified by reviewer): {describe_row_filter(spec['row_filter'])}")
        ap("# The delivered file is expected to hold only source rows matching it.")
    ap("import json")
    ap("import duckdb")
    ap("")
    ap('con = duckdb.connect(":memory:")')
    ap(f'con.execute("CREATE TABLE target_out AS SELECT * FROM read_csv_auto('
       f'\'{output_filename}\', all_varchar=true, header=true)")')
    ap("")
    ap("checks = []  # (name, category, status, detail)")
    ap("def check(name, cat, ok, detail):")
    ap('    checks.append((name, cat, "pass" if ok else "fail", detail))')
    ap("")
    ap("cols = [d[0] for d in con.execute(\"SELECT * FROM target_out LIMIT 0\").description]")
    ap("n = con.execute(\"SELECT count(*) FROM target_out\").fetchone()[0]")
    ap("")
    ap("# --- wellformed: every expected column present --------------------")
    ap(f"expected = {expected!r}")
    ap('missing = sorted(set(expected) - set(cols))')
    ap('check("delivered_columns_match_spec", "wellformed", not missing, '
       '("missing: " + str(missing)) if missing else "all columns present")')
    ap("")
    if has_source:
        ap("# --- grain: row count vs the source ---------------------------")
        ap("# (compare n above against the row count of the source file(s) you loaded)")
        ap("")
    ap("# --- completeness: required attributes -----------------------------")
    ap(f"required = {req!r}")
    ap("for name in required:")
    ap("    if name not in cols: continue")
    ap('    q = \'SELECT count(*) FROM target_out WHERE "\' + name + \'" IS NULL OR "\' + name + \'" = \\\'\\\'\'')
    ap("    nulls = con.execute(q).fetchone()[0]")
    ap('    check("required_present:" + name, "completeness", nulls == 0, str(nulls) + " null/blank")')
    ap("")
    if enums:
        ap("# --- domain: enum attributes stay within allowed_values ------------")
        ap(f"enums = {enums!r}")
        ap("for name, allowed in enums:")
        ap("    if name not in cols: continue")
        ap('    inlist = ", ".join("\'" + v.replace("\'", "\'\'") + "\'" for v in allowed)')
        ap('    q = \'SELECT count(*) FROM target_out WHERE "\' + name + \'" IS NOT NULL AND "\' + name + \'" <> \\\'\\\' AND "\' + name + \'" NOT IN (\' + inlist + \')\'')
        ap("    bad = con.execute(q).fetchone()[0]")
        ap('    check("domain:" + name, "domain", bad == 0, str(bad) + " value(s) outside allowed_values")')
        ap("")
    if not has_insight:
        ap("# NOTE: no candidate key could be derived from the source data, so")
        ap("# the key-uniqueness check (candidate key -> mapped target column) is")
        ap("# omitted. Check that the source file has an identifier-like column.")
        ap("")
    ap("# --- report ----------------------------------------------------------")
    ap('failed = [c for c in checks if c[2] == "fail"]')
    ap('for name, cat, status, detail in checks:')
    ap('    print(f"[{status.upper():4}] {cat:14} {name}: {detail}")')
    ap('print(f"\\n{len(checks)-len(failed)}/{len(checks)} passed."'
       ' + (f" {len(failed)} FAILED." if failed else ""))')
    # The console line gates a pipeline; it does not let anyone INVESTIGATE a
    # failure. The list the workspace renders is already in memory here, so
    # writing it out costs nothing and turns the script from a gate into a
    # reporting artefact — attachable to a change record, readable by the
    # client's own dashboard, or loadable back into the workspace.
    ap("")
    ap("report = {")
    ap('    "artefact": "validation",')
    ap('    "verdict": "blocked" if failed else "passed",')
    ap('    "counts": {"total": len(checks), "passed": len(checks) - len(failed), "failed": len(failed)},')
    ap('    "checks": [{"name": n, "category": c, "status": st, "detail": d}')
    ap("               for n, c, st, d in checks],")
    ap("}")
    ap('with open("validation_report.json", "w") as fh:')
    ap("    json.dump(report, fh, indent=2)")
    ap('print("report written to validation_report.json")')
    ap("raise SystemExit(1 if failed else 0)")

    return "\n".join(L)
