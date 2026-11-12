"""The target statement — built in ONE place — and the expression classifier.

A certified mapping is a set of per-column SQL expressions plus, optionally,
a declared target grain. Four places used to assemble "the target query"
independently (the certify check, the transform, the generated script, the
validation / reconciliation re-execution), each wrapping the reviewer's SQL
differently and naming the source relation differently. SQL that passed in one
failed in another: a window function was rejected by the certify probe
`count(<expr>)` yet ran fine in the transform; a sub-query on "__workset" was
certified yet the transform had no such relation.

So:
  * build_target_sql() is the only function that assembles the statement.
    At source grain it numbers the source rows BEFORE any window runs and
    orders the output by that number, so rows always come out in source
    order. At a declared grain it groups by the grain keys and orders by them.
  * classify_expression() decides what kind each column expression is — row,
    window, aggregate or group_key — from DuckDB's own parser, never a regex
    and never the LLM, and whether that kind is legal under the spec's grain.

Two settings decide behaviour, at two levels (docs/groupby_partition_plan.md):
the GRAIN of the target table (reviewer-declared, default: one row per source
row) and the KIND of each column (derived from its SQL). With no grain and no
window column everything behaves as before, plus the explicit ORDER BY.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable

import duckdb

WORKSET = "__workset"          # the one name certified SQL is evaluated against
ORD = "__src_ord"              # source ordinal; never delivered

# window functions whose result depends on row order — legal only with an
# ORDER BY inside OVER (...), otherwise the result can change between runs
_ORDER_SENSITIVE = {"row_number", "rank", "dense_rank", "percent_rank", "cume_dist",
                    "ntile", "first_value", "last_value", "nth_value", "lag", "lead",
                    "first", "last"}
_CONSERVED = {"sum", "count", "count_star"}   # sum of group results == whole-source result


def q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _indent(text: str, n: int = 2) -> str:
    pad = " " * n
    return "\n".join(pad + ln for ln in text.splitlines())


# ------------------------------------------------------------------ grain
def grain_keys(spec: Any) -> list[str] | None:
    """The declared grain's group-by columns, or None for source grain."""
    g = spec.get("grain") if isinstance(spec, dict) else getattr(spec, "grain", None)
    if g is None:
        return None
    g = g.model_dump() if hasattr(g, "model_dump") else g
    keys = [k for k in (g.get("group_by") or []) if k]
    return keys or None


# ------------------------------------------------------------- statement
def build_target_sql(select: list[tuple[str, str]], relation_sql: str,
                     group_by: list[str] | None = None,
                     extra: Iterable[tuple[str, str]] = ()) -> str:
    """THE target statement. `select` is [(target_attribute, expression)],
    `relation_sql` the source rows in scope (workset, after the row filter).
    `extra` adds internal projections (alignment keys) for the checks.

    Source grain: rows are numbered before any window runs and the output is
    ordered by that number — source order, whatever the window partitions do.
    Declared grain: GROUP BY the keys, ordered by the keys.
    """
    lines = [f"  {expr} AS {q(attr)}" for attr, expr in select]
    lines += [f"  {expr} AS {q(alias)}" for alias, expr in extra]
    if group_by:
        keys = ", ".join(q(k) for k in group_by)
        return ("SELECT\n" + ",\n".join(lines) + "\n"
                f"FROM (\n{_indent(relation_sql)}\n) AS {q(WORKSET)}\n"
                f"GROUP BY {keys}\nORDER BY {keys}")
    return ("SELECT\n" + ",\n".join(lines) + "\n"
            f"FROM (\n  SELECT *, row_number() OVER () AS {q(ORD)}\n"
            f"  FROM (\n{_indent(relation_sql, 4)}\n  ) AS __r\n) AS {q(WORKSET)}\n"
            f"ORDER BY {q(ORD)}")


# ------------------------------------------------------------- analysis
_AGGREGATES: set[str] | None = None


def _aggregates(con) -> set[str]:
    global _AGGREGATES
    if _AGGREGATES is None:
        rows = con.execute("SELECT DISTINCT function_name FROM duckdb_functions() "
                           "WHERE function_type = 'aggregate'").fetchall()
        _AGGREGATES = {r[0].lower() for r in rows} | {"count_star"}
    return _AGGREGATES


@dataclass
class Analysis:
    ok: bool = True
    error: str = ""
    star: bool = False
    subquery: bool = False
    qualified: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    has_window: bool = False
    has_aggregate: bool = False
    partition_by: list[str] = field(default_factory=list)
    unordered_windows: list[str] = field(default_factory=list)
    top_function: str = ""      # the top-level node's function, when it is an aggregate
    top_distinct: bool = False  # ...called as fn(DISTINCT ...)


def analyse_expression(expr: str, con=None) -> Analysis:
    """Parse ONE column expression with DuckDB and report what it contains.
    Parse only — nothing is executed and no data is needed."""
    own = con is None
    con = con or duckdb.connect()
    try:
        aggs = _aggregates(con)
        raw = con.execute("SELECT json_serialize_sql(?)", [f"SELECT {expr} FROM t"]).fetchone()[0]
    finally:
        if own:
            con.close()
    tree = json.loads(raw)
    a = Analysis()
    if tree.get("error") or len(tree.get("statements") or []) != 1:
        a.ok, a.error = False, tree.get("error_message") or "not a single expression"
        return a
    node = tree["statements"][0]["node"]
    sel = node.get("select_list") or []
    if len(sel) != 1 or node.get("group_expressions") or node.get("having"):
        a.ok, a.error = False, "not a single expression"
        return a
    cols: list[str] = []

    def walk(n: Any, in_window: bool) -> None:
        if isinstance(n, list):
            for x in n:
                walk(x, in_window)
            return
        if not isinstance(n, dict):
            return
        cls = n.get("class")
        if cls == "SUBQUERY":
            a.subquery = True
            return
        if cls == "STAR":
            a.star = True
        if cls == "COLUMN_REF":
            names = n.get("column_names") or []
            if len(names) > 1:
                a.qualified.append(".".join(names))
            elif names and names[0] not in cols:
                cols.append(names[0])
        if cls == "WINDOW":
            a.has_window = True
            fn = (n.get("function_name") or "").lower()
            for p in n.get("partitions") or []:
                if isinstance(p, dict) and p.get("class") == "COLUMN_REF":
                    nm = (p.get("column_names") or [""])[-1]
                    if nm and nm not in a.partition_by:
                        a.partition_by.append(nm)
            if fn in _ORDER_SENSITIVE and not n.get("orders"):
                a.unordered_windows.append(fn)
            for v in n.values():
                walk(v, True)
            return
        if cls == "FUNCTION" and (n.get("function_name") or "").lower() in aggs \
                and not in_window:
            a.has_aggregate = True
        for v in n.values():
            walk(v, in_window)

    top = sel[0]
    walk(top, False)
    a.columns = cols
    if top.get("class") == "FUNCTION":
        fn = (top.get("function_name") or "").lower()
        if fn in aggs:
            a.top_function = fn
            a.top_distinct = bool(top.get("distinct"))
    return a


# --------------------------------------------------------- classification
@dataclass
class Classification:
    kind: str                          # row | window | aggregate | group_key | invalid
    columns: list[str] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    error_code: str | None = None
    message: str | None = None
    suggested_sql: str = ""            # e.g. ANY_VALUE(...) under a grain

    @property
    def ok(self) -> bool:
        return self.error_code is None


def _kind(a: Analysis, keys: list[str] | None) -> str:
    if a.has_aggregate:
        return "aggregate"
    if a.has_window:
        return "window"
    if keys and a.columns and set(a.columns) <= set(keys):
        return "group_key"
    return "row"


def classify_expression(expr: str, con=None, relation: str | None = None,
                        keys: list[str] | None = None, attr: str = "") -> Classification:
    """What kind of expression this is, and whether it is legal under the grain.

    `relation` (a SQL relation: a quoted name or a parenthesised query) enables
    the bind check and the same-within-group suggestion; without it only the
    parse-level rules apply — which is what consumers of a client-supplied
    spec use to refuse unsafe SQL before executing anything.
    """
    label = attr or "this column"
    a = analyse_expression(expr, con)
    if not a.ok or a.star:
        return Classification("invalid", error_code="E-EXPR-PARSE", message=(
            "This isn't a single SQL expression. Write one column expression "
            "(no GROUP BY, no semicolons). To group rows, set a target grain."))
    if a.subquery:
        return Classification("invalid", error_code="E-EXPR-SUBQUERY", message=(
            "Sub-queries aren't supported. To aggregate per key use "
            "AGG(...) OVER (PARTITION BY ...), or set a target grain."))
    if a.qualified:
        bare = a.qualified[0].split(".")[-1]
        return Classification("invalid", a.columns, error_code="E-EXPR-QUALIFIED", message=(
            f"Refer to columns by name only (e.g. \"{bare}\"), without a table prefix: "
            f"{', '.join(a.qualified)}."))
    kind = _kind(a, keys)
    c = Classification(kind, a.columns, a.partition_by)
    if not keys and kind == "aggregate":
        fn = (a.top_function or "the aggregate").upper().replace("COUNT_STAR", "COUNT(*)")
        c.error_code, c.message = "E-EXPR-AGG-NO-GRAIN", (
            f"{fn} adds up rows. Add OVER (PARTITION BY ...) to keep one row per "
            f"source row, or set a target grain.")
        return c
    if keys and kind == "window":
        c.error_code, c.message = "E-EXPR-WINDOW-UNDER-GRAIN", (
            "With a target grain, use the aggregate without OVER (...).")
        return c
    if a.unordered_windows:
        fn = a.unordered_windows[0]
        c.error_code, c.message = "E-EXPR-WINDOW-NONDET", (
            f"{fn} depends on row order. Add ORDER BY inside OVER (...).")
        return c
    if con is None or relation is None:
        if keys and kind == "row" and a.columns:
            c.error_code, c.message = "E-EXPR-NOT-GROUPED", (
                f"'{label}' is not aggregated and is not a group column "
                f"({', '.join(keys)}). Aggregate it, or make it a group column.")
        return c
    # ---- bind against the real relation (unknown columns, grouping errors)
    try:
        if keys:
            con.execute(f"SELECT ({expr}) AS v FROM {relation} "
                        f"GROUP BY {', '.join(q(k) for k in keys)} LIMIT 0")
        else:
            con.execute(f"SELECT ({expr}) AS v, * FROM {relation} LIMIT 0")
    except Exception as e:      # noqa: BLE001
        msg = str(e).splitlines()[0]
        if keys and kind == "row" and a.columns:
            violations, sample = check_same_within_group(con, expr, relation, keys)
            if violations == 0:
                c.error_code, c.suggested_sql = "I-EXPR-ANY-VALUE", f"ANY_VALUE({expr})"
                c.message = (f"'{label}' is the same within each {', '.join(keys)} group. "
                             f"Suggested: ANY_VALUE(...).")
            else:
                c.error_code = "E-EXPR-NOT-GROUPED"
                c.message = (f"'{label}' has different values within a {', '.join(keys)} "
                             f"group (e.g. {sample[:2]}). Use an aggregate, or make it a "
                             f"group column.")
            return c
        c.error_code, c.message = "E-EXPR-BIND", msg
    return c


def check_same_within_group(con, expr: str, relation: str,
                            keys: list[str]) -> tuple[int | None, list]:
    """Groups in which `expr` takes more than one value. 0 means the value is
    constant per group, so ANY_VALUE(expr) is a safe, explicit rewrite."""
    k = ", ".join(q(x) for x in keys)
    # NULL counts as a value of its own: a column set on one row of a group and
    # blank on the others is NOT the same within the group (count(DISTINCT)
    # alone ignores NULLs and would call it constant)
    n_values = f"count(DISTINCT coalesce(CAST(({expr}) AS VARCHAR), chr(0)))"
    try:
        bad = con.execute(f"SELECT count(*) FROM (SELECT {k} FROM {relation} GROUP BY {k} "
                          f"HAVING {n_values} > 1)").fetchone()[0]
        sample = con.execute(f"SELECT {k}, {n_values} AS n_values FROM {relation} "
                             f"GROUP BY {k} HAVING {n_values} > 1 "
                             f"ORDER BY 1 LIMIT 5").fetchall()
        return bad, [list(r) for r in sample]
    except Exception:            # noqa: BLE001
        return None, []


def conservation_function(expr: str) -> str | None:
    """'sum' / 'count' when the expression is a single top-level SUM or COUNT
    (optionally with FILTER): the delivered group values must then add up to
    the same aggregate taken over the whole source."""
    a = analyse_expression(expr)
    # COUNT(DISTINCT x) / SUM(DISTINCT x) do NOT add up across groups: one
    # value can belong to several groups (an assessor handling two claim types)
    if a.ok and a.top_function in _CONSERVED and not a.has_window and not a.top_distinct:
        return "count" if a.top_function == "count_star" else a.top_function
    return None


def group_key_attributes(spec: Any) -> list[str]:
    """Target attributes that carry the declared grain's group key — the
    identity of a delivered row under a grain. Empty at source grain."""
    sd = spec if isinstance(spec, dict) else spec.model_dump()
    keys = grain_keys(sd)
    if not keys:
        return []
    con = duckdb.connect()
    try:
        return [m["target_attribute"] for m in sd.get("mappings", [])
                if m.get("gate") != "reject" and (m.get("transformation_sql") or "").strip()
                and classify_expression(m["transformation_sql"], con, keys=keys).kind == "group_key"]
    finally:
        con.close()


# ------------------------------------------------------- client-spec guard
def spec_expression_errors(spec: Any) -> list[str]:
    """Parse-level checks on every live expression of a spec that arrived from
    the browser, BEFORE any of it is executed: one expression, no sub-query,
    no qualified reference, a kind legal under the declared grain. Binding
    against real columns happens when the statement runs."""
    sd = spec if isinstance(spec, dict) else spec.model_dump()
    g = sd.get("grain") or {}
    if g.get("errors"):
        return ["the target grain is invalid: " + "; ".join(g["errors"])]
    keys = grain_keys(sd)
    errs = []
    con = duckdb.connect()
    try:
        for m in sd.get("mappings", []):
            if m.get("gate") == "reject" or not (m.get("transformation_sql") or "").strip():
                continue
            c = classify_expression(m["transformation_sql"], con, keys=keys,
                                    attr=m.get("target_attribute", ""))
            if not c.ok:
                errs.append(f"{m.get('target_attribute')}: {c.message}")
    finally:
        con.close()
    return errs


def assert_spec_safe(spec: Any) -> None:
    errs = spec_expression_errors(spec)
    if errs:
        raise ValueError("the certified mapping cannot be executed: " + " | ".join(errs))
