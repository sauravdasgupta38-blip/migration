"""Transformation workspace (tab 4) — additive.

Turns a certified MappingSpec into an executable ETL and runs it against the
source file(s). Everything here is derived from the spec the mapping workspace
(tab 3) already produced, so tab 4's output is guaranteed consistent with what
was certified: the same DuckDB SQL dialect the mapping agent emitted
(strptime / TRY_CAST / NULLIF / CASE) is executed verbatim, not re-implemented.

Nothing in this module touches Flow A / Flow B state — it only reads a spec plus
the current input files and returns a target dataset.
"""
from __future__ import annotations

import io
from pathlib import Path

import duckdb

# non-migrating gate: rows the mapping workspace excluded from load
REJECT_GATE = "reject"


# ---------------------------------------------------------------------------
# workset — rebuild the exact relation the spec was written against
# ---------------------------------------------------------------------------
def _q(name: str) -> str:
    """Quote a SQL identifier."""
    return '"' + str(name).replace('"', '""') + '"'


def _read_csv(alias: str) -> str:
    return f"read_csv_auto(?, all_varchar=true, header=true)"


def resolve_source_paths(spec: dict, file_lookup) -> dict[str, str]:
    """Map every source_table named in the spec to a real file path.

    file_lookup(table_name) -> path|None. Tables come from spec.source_tables
    (e.g. 'EFAS0042', 'ESCH0009'); a mapping spec built from a single file has
    just one entry and no join_plan.
    """
    tables = spec.get("source_tables") or []
    if not tables:
        st = spec.get("source_table")
        if st and st != "__workset":
            tables = [st]
    paths: dict[str, str] = {}
    for t in tables:
        p = file_lookup(t)
        if p:
            paths[t] = p
    return paths


def build_workset_sql(spec: dict, paths: dict[str, str]) -> tuple[str, list[str]]:
    """Return (sql_defining_the_workset_relation, ordered_param_paths).

    Single file  -> SELECT * FROM read_csv_auto(<file>)
    Joined files -> primary LEFT JOIN each joined file per spec.join_plan,
                    keeping the primary file's row grain.

    This MUST reproduce engine/composite.build_workset exactly, because the
    spec's transformation_sql was written against the columns that view
    exposes. Three things therefore have to match, and previously did not:

      * join keys are compared TRIMMED — COBOL fields are space-padded, so a
        raw `=` silently produced zero matches and every joined column came
        back NULL;
      * a column name that clashes with one already taken is exposed as
        "{table}_{column}", NOT dropped — the spec may reference that exact
        renamed column, and dropping it made the transform fail with a
        missing-column error;
      * joins chain — an entry's `to_table` may be a previously joined file,
        not the primary, so each table needs its own alias.
    """
    tables = list(paths.keys())
    join_plan = [j for j in (spec.get("join_plan") or []) if j.get("table") in paths]

    if len(tables) == 1 or not join_plan:
        t = tables[0]
        return f"SELECT * FROM {_read_csv(t)}", [paths[t]]

    # the primary is the join target that is never itself joined IN
    joined_in = {j.get("table") for j in join_plan}
    primary = next((j.get("to_table") for j in join_plan
                    if j.get("to_table") not in joined_in and j.get("to_table") in paths),
                   None)
    if primary not in paths:
        primary = tables[0]

    alias = {primary: "p"}
    params: list[str] = [paths[primary]]
    taken: set[str] = set()
    select_cols: list[str] = []

    for c in _csv_columns(paths[primary]):      # the primary keeps its names
        taken.add(c)
        select_cols.append(f"p.{_q(c)} AS {_q(c)}")

    joins: list[str] = []
    for i, j in enumerate(join_plan):
        jt = j.get("table")
        a = f"j{i}"
        alias[jt] = a
        left_tbl = j.get("to_table")
        left_alias = alias.get(left_tbl, "p")   # chained joins resolve here
        left_key, right_key = j.get("to_column"), j.get("on")
        joins.append(
            f"LEFT JOIN {_read_csv(jt)} AS {a} "
            f"ON trim({left_alias}.{_q(left_key)}) = trim({a}.{_q(right_key)})"
        )
        params.append(paths[jt])
        for c in _csv_columns(paths[jt]):
            out = c if c not in taken else f"{jt}_{c}"
            taken.add(out)
            select_cols.append(f"{a}.{_q(c)} AS {_q(out)}")

    sql = (
        "SELECT " + ", ".join(select_cols) + "\n"
        f"FROM {_read_csv(primary)} AS p\n" + "\n".join(joins)
    )
    return sql, params


def row_filter_active(spec: dict) -> bool:
    from engine.row_filter import is_active

    return spec.get("row_filter") is not None and (
        is_active(spec["row_filter"]) or bool((spec["row_filter"] or {}).get("errors")))


def _relation_columns(sql: str, params: list[str]) -> list[str]:
    con = duckdb.connect(":memory:")
    try:
        rel = con.execute(f"SELECT * FROM ({sql}) AS _c LIMIT 0", params)
        return [d[0] for d in rel.description]
    finally:
        con.close()


def compile_spec_filter(spec: dict, available_columns) -> str:
    """The certified row filter as SQL, recompiled from its structure.

    The spec arrives from the browser, so its stored `compiled_sql` is never
    executed — the structure is re-validated against the real columns here. A
    filter certified with errors cannot be applied, and saying so beats
    silently migrating rows the reviewer meant to exclude."""
    from engine.row_filter import RowFilterError, compile_row_filter

    rf = spec.get("row_filter") or {}
    if rf.get("errors"):
        raise RowFilterError("the row filter is invalid: " + "; ".join(rf["errors"]))
    return compile_row_filter(rf, available_columns)


def build_source_sql(spec: dict, paths: dict[str, str]) -> tuple[str, list[str]]:
    """The workset AFTER the reviewer's row filter: the single population every
    workspace treats as 'the source'. Without a filter this is exactly
    build_workset_sql. The filter applies to workset rows (after the joins,
    before any column transformation — and before any future window or GROUP BY
    transformation, which must therefore only ever see rows in scope)."""
    ws, params = build_workset_sql(spec, paths)
    if not row_filter_active(spec):
        return ws, params
    cond = compile_spec_filter(spec, _relation_columns(ws, params))
    return f"SELECT * FROM (\n{_indent(ws)}\n) AS __wf\nWHERE {cond}", params


def _csv_columns(path: str) -> list[str]:
    con = duckdb.connect(":memory:")
    try:
        rel = con.execute(
            "SELECT * FROM read_csv_auto(?, all_varchar=true, header=true) LIMIT 0",
            [path],
        )
        return [d[0] for d in rel.description]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# SELECT — the certified column list
# ---------------------------------------------------------------------------
def build_select(spec: dict, migrated_at: str | None = None) -> list[tuple[str, str]]:
    """Ordered (target_attribute, sql_expression) for every migrating column,
    plus system-defaulted targets. Rejected mappings are skipped.

    migrated_at, when given, is injected as a plain string literal rather than
    CURRENT_TIMESTAMP. DuckDB's now()/CURRENT_TIMESTAMP is TIMESTAMP WITH TIME
    ZONE, which pulls in Python's pytz on materialisation; a text literal keeps
    the transform runnable on any interpreter without that optional dependency.
    """
    cols: list[tuple[str, str]] = []
    for m in spec.get("mappings", []):
        if m.get("gate") == REJECT_GATE:
            continue
        expr = (m.get("transformation_sql") or "").strip() or "NULL"
        cols.append((m["target_attribute"], expr))
    # unmapped targets are defaulted at load; surface them so the target shape
    # matches the dictionary. source_system / migrated_at get real defaults.
    for u in spec.get("unmapped_target", []):
        attr = u["attribute"]
        if attr == "source_system":
            cols.append((attr, "'"+ (spec.get("source_tables") or ["SOURCE"])[0] + "'"))
        elif attr == "migrated_at":
            if migrated_at:
                cols.append((attr, "'" + migrated_at.replace("'", "''") + "'"))
            else:
                cols.append((attr, "strftime(current_localtimestamp(), '%Y-%m-%d %H:%M:%S')"))
        else:
            cols.append((attr, "NULL"))
    return cols


def build_full_sql(spec: dict, paths: dict[str, str],
                   migrated_at: str | None = None,
                   extra=()) -> tuple[str, list[str]]:
    """The certified target statement over the source files, built by the one
    builder every workspace uses (engine/etl_sql.build_target_sql). The spec
    comes from the browser, so its expressions are checked before anything
    runs. `extra` adds internal projections (alignment keys) for the checks."""
    from engine.etl_sql import assert_spec_safe, build_target_sql, grain_keys

    assert_spec_safe(spec)
    source_sql, params = build_source_sql(spec, paths)
    return build_target_sql(build_select(spec, migrated_at), source_sql,
                            grain_keys(spec), extra), params


def column_kinds(spec: dict) -> dict[str, list[str]]:
    """{kind: [target attributes]} for the live mappings, derived from the SQL."""
    from engine.etl_sql import classify_expression, grain_keys

    keys, out = grain_keys(spec), {}
    con = duckdb.connect(":memory:")
    try:
        for m in spec.get("mappings", []):
            if m.get("gate") == REJECT_GATE or not (m.get("transformation_sql") or "").strip():
                continue
            k = classify_expression(m["transformation_sql"], con, keys=keys).kind
            out.setdefault(k, []).append(m["target_attribute"])
    finally:
        con.close()
    return out


def _text_fetch_sql(inner_sql: str, columns: list[str]) -> str:
    """Wrap a query so every column comes back as VARCHAR.

    DuckDB can materialise DATE / TIMESTAMP results through a code path that
    imports pytz; casting to text at the boundary keeps values clean
    (dates as 'YYYY-MM-DD') and removes that optional dependency entirely.
    Booleans and numbers survive the round-trip as their string forms, which
    is exactly what the CSV export and the grid render anyway.
    """
    casts = ", ".join(f"CAST({_q(c)} AS VARCHAR) AS {_q(c)}" for c in columns)
    return f"SELECT {casts}\nFROM (\n{_indent(inner_sql)}\n) AS materialised"


def _indent(text: str, n: int = 2) -> str:
    pad = " " * n
    return "\n".join(pad + ln for ln in text.splitlines())


# ---------------------------------------------------------------------------
# execution
# ---------------------------------------------------------------------------
def run_transform(spec: dict, paths: dict[str, str], limit: int | None = None):
    """Execute the certified transform. Returns (columns, rows, csv_text, stats).

    The result is fetched with every column cast to VARCHAR so DuckDB never
    materialises DATE/TIMESTAMP values through its pytz-dependent path — the
    transform runs whether or not pytz is installed. Dates come back as
    'YYYY-MM-DD', which is what the CSV and grid display anyway.
    """
    from datetime import datetime

    from engine.etl_sql import assert_spec_safe, build_target_sql, grain_keys

    migrated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    assert_spec_safe(spec)
    select = build_select(spec, migrated_at)
    columns = [attr for attr, _ in select]
    source_sql, params = build_source_sql(spec, paths)

    con = duckdb.connect(":memory:")
    try:
        # Load the rows in scope FIRST, then switch off file access: reviewer-
        # authored SQL runs on a connection that can no longer read files.
        con.execute(f'CREATE TEMP TABLE "__source" AS {source_sql}', params)
        in_scope = con.execute('SELECT count(*) FROM "__source"').fetchone()[0]
        con.execute("SET enable_external_access = false")
        inner_sql = build_target_sql(select, 'SELECT * FROM "__source"', grain_keys(spec))
        fetch_sql = _text_fetch_sql(inner_sql, columns)
        rel = con.execute(fetch_sql)
        columns = [d[0] for d in rel.description]
        all_rows = rel.fetchall()
        total = len(all_rows)
        rows = all_rows[:limit] if limit else all_rows
        # CSV of the full result (not the preview slice)
        buf = io.StringIO()
        import csv as _csv

        w = _csv.writer(buf)
        w.writerow(columns)
        for r in all_rows:
            w.writerow(["" if v is None else v for v in r])
        csv_text = buf.getvalue()
    finally:
        con.close()

    # cell values -> JSON-safe strings, preserving a null marker for the grid
    def cell(v):
        return None if v is None else (v if isinstance(v, (int, float, bool)) else str(v))

    stats = {
        "row_count": total,
        "column_count": len(columns),
        "returned": len(rows),
        "migrating_columns": len([m for m in spec.get("mappings", []) if m.get("gate") != REJECT_GATE]),
        "defaulted_columns": len(spec.get("unmapped_target", [])),
        "row_filter": None,
        # what one target row is, and how the rows got there
        "grain": grain_keys(spec),
        "source_rows": in_scope,
        "target_rows": total,
    }
    kinds = column_kinds(spec)
    stats["window_columns"] = kinds.get("window", [])
    stats["aggregate_columns"] = kinds.get("aggregate", [])
    if row_filter_active(spec):
        from engine.row_filter import describe_row_filter

        ws, ws_params = build_workset_sql(spec, paths)
        con = duckdb.connect(":memory:")
        try:
            all_rows_n = con.execute(f"SELECT count(*) FROM ({ws}) AS w", ws_params).fetchone()[0]
        finally:
            con.close()
        stats.update(row_filter=describe_row_filter(spec["row_filter"]),
                     source_rows_total=all_rows_n,
                     rows_excluded_by_filter=all_rows_n - in_scope)
    return columns, [[cell(v) for v in r] for r in rows], csv_text, stats


# ---------------------------------------------------------------------------
# code generation — a standalone, readable Python/Pandas + DuckDB ETL script
# ---------------------------------------------------------------------------
def readable_workset_sql(spec: dict, tables: list[str], filenames: dict[str, str],
                         paths: dict[str, str]) -> str:
    """The joined workset SQL with the positional read_csv_auto(?) calls
    rewritten to the table names a generated script registers, so the script
    reads naturally. Built the collision-safe way the runtime builds it; shared
    by the ETL and the reconciliation script generators."""
    join_plan = spec.get("join_plan") or []
    primary = None
    for j in join_plan:
        primary = j.get("to_table") or primary
    primary = primary or tables[0]
    workset_sql, _ = build_workset_sql(
        spec, {t: paths.get(t, filenames.get(t, f"{t}.csv")) for t in tables}
    )
    ordered = [primary] + [
        j.get("table") for j in join_plan if j.get("table") in tables
    ]
    readable = workset_sql
    for t in ordered:
        readable = readable.replace(
            "read_csv_auto(?, all_varchar=true, header=true)", t, 1
        )
    return readable


def _script_columns(spec: dict, tables: list[str], filenames: dict[str, str],
                    paths: dict[str, str]) -> list[str]:
    """Columns the generated script's source relation exposes, for validating
    the row filter. If the source files cannot be read here (script generated
    without them loaded), fall back to the fields the filter names: they are
    always emitted as quoted identifiers, so this cannot inject anything — the
    script itself will report a missing column."""
    try:
        ws, params = build_workset_sql(
            spec, {t: paths.get(t, filenames.get(t, f"{t}.csv")) for t in tables})
        return _relation_columns(ws, params)
    except Exception:      # noqa: BLE001
        def fields(n):
            if isinstance(n, dict) and "conditions" in n:
                return [f for x in n["conditions"] for f in fields(x)]
            return [n.get("field")] if isinstance(n, dict) else []
        return fields((spec.get("row_filter") or {}).get("root") or {})


def generate_python(spec: dict, filenames: dict[str, str],
                    paths: dict[str, str] | None = None) -> str:
    """Produce a self-contained ETL script that reproduces run_transform.

    filenames maps source_table -> the on-disk file name shown in the script.
    paths maps source_table -> a real readable path used only to introspect
    columns for the join workset (falls back to filenames when omitted). The
    script is what an engineer would hand-write: pandas for I/O, DuckDB for the
    certified SQL so the transform semantics are identical to this runtime.
    """
    from engine.etl_sql import assert_spec_safe, build_target_sql, grain_keys

    assert_spec_safe(spec)
    paths = paths or filenames
    target = spec.get("target_table", "target")
    tables = spec.get("source_tables") or (
        [spec["source_table"]] if spec.get("source_table") and spec["source_table"] != "__workset" else []
    )
    join_plan = spec.get("join_plan") or []
    # a fixed migration timestamp string keeps the generated script free of
    # DuckDB's timezone-aware CURRENT_TIMESTAMP (which needs the optional pytz)
    from datetime import datetime

    migrated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    select = build_select(spec, migrated_at)

    L: list[str] = []
    ap = L.append
    ap('"""')
    ap(f"ETL — {tables and ' + '.join(tables) or 'source'}  ->  {target}")
    ap("")
    ap("Auto-generated from the certified mapping specification.")
    ap("Reads the source file(s) with pandas, then applies the certified")
    ap("transformation with DuckDB so the SQL semantics match the mapping")
    ap("workspace exactly (strptime / TRY_CAST / CASE / NULLIF).")
    ap('"""')
    ap("import pandas as pd")
    ap("import duckdb")
    ap("")
    ap("# --- 1. load source file(s) ---------------------------------------")
    for t in tables:
        fn = filenames.get(t, f"{t}.csv")
        ap(f'{t.lower()} = pd.read_csv("{fn}", dtype=str, keep_default_na=False)')
    ap("")
    ap("con = duckdb.connect(\":memory:\")")
    for t in tables:
        ap(f'con.register("{t}", {t.lower()})')
    ap("")

    # workset
    if len(tables) > 1 and join_plan:
        ap("# --- 2. rebuild the joined workset (keeps primary row grain) ------")
        readable = readable_workset_sql(spec, tables, filenames, paths)
        ap('workset_sql = """')
        for ln in readable.splitlines():
            ap(ln)
        ap('"""')
        ap('con.execute("CREATE OR REPLACE TEMP VIEW workset AS " + workset_sql)')
        ap("")
        src_rel = "workset"
    else:
        src_rel = tables[0] if tables else "src"

    # the reviewer's row filter, recompiled from its structure — only rows in
    # scope are transformed (and grouped / partitioned)
    where = ""
    if row_filter_active(spec):
        from engine.row_filter import describe_row_filter

        where = "\nWHERE " + compile_spec_filter(
            spec, _script_columns(spec, tables, filenames, paths))
        ap(f"# Row filter (certified by reviewer): {describe_row_filter(spec['row_filter'])}")
        ap("# Only source rows matching it are transformed; the rest are not migrated.")
        ap("")
    keys = grain_keys(spec)
    kinds = column_kinds(spec)
    ap("# Target grain : " + (f"one row per {', '.join(keys)}" if keys
                              else "one row per source row"))
    ap("# Column kinds : " + " · ".join(f"{len(kinds.get(k, []))} {k}"
                                       for k in ("row", "window", "aggregate", "group_key")
                                       if kinds.get(k)))
    ap("")

    ap("# --- 3. certified column transforms -------------------------------")
    # The SQL is emitted as a RAW triple-quoted literal. Embedding it in a
    # normal string meant Python re-interpreted any backslash escape it
    # recognised: a certified expression containing '\t' reached DuckDB as a
    # literal tab, so the handed-over script executed DIFFERENT SQL from the one
    # the workspace validated — silently. A script that is not a faithful copy
    # of the certified spec cannot back a reproducibility claim.
    # The statement is the one engine/etl_sql.build_target_sql builds for the
    # app itself: source order preserved, or grouped by the declared grain.
    body = build_target_sql(select, f"SELECT * FROM {src_rel}{where}", keys) + "\n"
    if '"""' in body:
        # fall back to an escaped literal rather than emit broken Python
        ap(f"transform_sql = {body!r}")
    else:
        ap('transform_sql = r"""')
        for line in body.rstrip("\n").splitlines():
            ap(line)
        ap('"""')
    ap("")
    ap("# --- 4. execute + export ------------------------------------------")
    ap("# Export straight from DuckDB (COPY) so DATE/TIMESTAMP columns never")
    ap("# round-trip through pandas — the transform runs without optional")
    ap("# timezone packages, and dates are written as YYYY-MM-DD.")
    ap(f'con.execute(f"""COPY ({{transform_sql}}) TO \'{target}.csv\' '
       f'(HEADER, DELIMITER \',\')""")')
    ap(f'n = con.execute(f"SELECT count(*) FROM ({{transform_sql}})").fetchone()[0]')
    ap(f'print(f"Wrote {{n}} rows to {target}.csv")')
    ap("")
    ap("# Prefer a DataFrame? Read the CSV back (all-text, pytz-free):")
    ap(f'# {target}_df = pd.read_csv("{target}.csv", dtype=str, keep_default_na=False)')
    return "\n".join(L)
