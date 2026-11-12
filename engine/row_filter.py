"""The reviewer's source row filter — structured conditions, compiled once.

A reviewer can restrict WHICH source rows migrate ("only open roles": ENDDTE is
empty), combining conditions with AND / OR. The filter is deliberately NOT a
free-text WHERE clause, for the same reason reconciliation controls are not
prose (README, "Structured rules, never prose"):

  * the spec round-trips through the browser on every workspace call, so any
    SQL text in it is client-supplied and cannot be trusted. The structure is
    re-validated and recompiled server-side by every consumer instead;
  * a field is chosen from the source dictionary, an operator from a fixed
    list, and a value is validated for the field's type before it is quoted —
    there is no step at which an invalid or injected condition can be
    expressed;
  * one compiler means the certify check, the transform, the generated scripts,
    validation and reconciliation cannot disagree about which rows are in scope.

Everything is staged as VARCHAR, so the field's dictionary type decides how a
value is compared: text is trimmed, numbers go through TRY_CAST, YYYYMMDD dates
are parsed and 00000000 counts as "no date". Pure Python — no api imports.
"""
from __future__ import annotations

import re
from datetime import date
from typing import Any, Iterable

MAX_CONDITIONS = 20
MAX_DEPTH = 4
MAX_LIST = 200

_ALL = ("text", "number", "date")
_ORDERED = ("number", "date")

# op -> label shown to the reviewer, arity ("none" | "one" | "list" | "two"),
# and the type families it is valid for. The UI mirrors this table.
OPERATORS: dict[str, dict[str, Any]] = {
    "is_empty":     {"label": "is empty",          "arity": "none", "families": _ALL},
    "is_not_empty": {"label": "is not empty",      "arity": "none", "families": _ALL},
    "eq":           {"label": "equals",            "arity": "one",  "families": _ALL},
    "ne":           {"label": "does not equal",    "arity": "one",  "families": _ALL},
    "in":           {"label": "is one of",         "arity": "list", "families": _ALL},
    "not_in":       {"label": "is not one of",     "arity": "list", "families": _ALL},
    "gt":           {"label": "is greater than",   "arity": "one",  "families": _ORDERED},
    "ge":           {"label": "is at least",       "arity": "one",  "families": _ORDERED},
    "lt":           {"label": "is less than",      "arity": "one",  "families": _ORDERED},
    "le":           {"label": "is at most",        "arity": "one",  "families": _ORDERED},
    "between":      {"label": "is between",        "arity": "two",  "families": _ORDERED},
    "contains":     {"label": "contains",          "arity": "one",  "families": ("text",)},
    "starts_with":  {"label": "starts with",       "arity": "one",  "families": ("text",)},
    "ends_with":    {"label": "ends with",         "arity": "one",  "families": ("text",)},
}
_CMP = {"eq": "=", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}
_NUMBER_TYPES = {"INTEGER", "DECIMAL", "AMOUNT", "NUMBER", "NUMERIC"}
_NUM_RE = re.compile(r"^-?\d+(\.\d+)?$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


class RowFilterError(ValueError):
    """The filter cannot be compiled faithfully. The message is reviewer-facing."""


def type_family(inferred_type: str | None) -> str:
    t = (inferred_type or "").strip().upper()
    if t.startswith("DATE"):
        return "date"
    if t in _NUMBER_TYPES:
        return "number"
    return "text"


def _as_dict(rf: Any) -> dict:
    if rf is None:
        return {}
    if hasattr(rf, "model_dump"):
        return rf.model_dump()
    return rf


def _root(rf: Any) -> dict:
    d = _as_dict(rf)
    return d.get("root") or {}


def _q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _is_group(node: dict) -> bool:
    return isinstance(node, dict) and "conditions" in node


# ---------------------------------------------------------------- literals
def _literal(family: str, raw: Any, field: str) -> str:
    """Validate a reviewer value for the field's type, then quote it. Anything
    that does not validate is rejected — never coerced into something else."""
    if raw is None:
        raise RowFilterError(f"'{field}': a value is required.")
    v = str(raw).strip()
    if _CONTROL_RE.search(v):
        raise RowFilterError(f"'{field}': the value contains control characters.")
    if family == "number":
        if not _NUM_RE.match(v):
            raise RowFilterError(f"'{field}': '{v}' is not a number.")
        return f"CAST('{v}' AS DECIMAL(38,10))"
    if family == "date":
        return f"DATE '{_parse_date(v, field).isoformat()}'"
    if v == "":
        raise RowFilterError(f"'{field}': a value is required "
                             f"(use 'is empty' to match blank values).")
    return "'" + v.replace("'", "''") + "'"


def _parse_date(v: str, field: str) -> date:
    s = v.replace("-", "")
    if not re.fullmatch(r"\d{8}", s):
        raise RowFilterError(f"'{field}': '{v}' is not a date (use YYYY-MM-DD).")
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:]))
    except ValueError:
        raise RowFilterError(f"'{field}': '{v}' is not a valid date.") from None


def _value_expr(family: str, col: str) -> str:
    if family == "number":
        return f"TRY_CAST(trim({col}) AS DECIMAL(38,10))"
    if family == "date":
        return f"TRY_STRPTIME(NULLIF(trim({col}), '00000000'), '%Y%m%d')::DATE"
    return f"trim({col})"


def _empty_expr(family: str, col: str) -> str:
    if family == "date":
        return f"({col} IS NULL OR trim({col}) IN ('', '00000000'))"
    return f"({col} IS NULL OR trim({col}) = '')"


# ---------------------------------------------------------------- compile
def _condition_sql(c: dict, available: set[str]) -> str:
    field, op = c.get("field"), c.get("op")
    if not field or field not in available:
        raise RowFilterError(f"'{field}' is not a field of the source data.")
    spec = OPERATORS.get(op or "")
    if spec is None:
        raise RowFilterError(f"'{field}': unknown condition '{op}'.")
    family = type_family(c.get("field_type"))
    if family not in spec["families"]:
        raise RowFilterError(f"'{field}': '{spec['label']}' cannot be used on a "
                             f"{family} field.")
    col = _q(field)
    v, e = _value_expr(family, col), _empty_expr(family, col)
    arity = spec["arity"]

    if arity == "none":
        return e if op == "is_empty" else f"NOT {e}"
    if arity == "one":
        lit = _literal(family, c.get("value"), field)
        if op == "ne":
            # empty counts as "not equal" — the reviewer asked to exclude ONE
            # value, not to also drop every blank row (SQL's NULL semantics)
            return f"({e} OR {v} <> {lit})"
        if op in ("contains", "starts_with", "ends_with"):
            return f"{op}({v}, {lit})"
        return f"{v} {_CMP[op]} {lit}"
    vals = list(c.get("values") or [])
    if arity == "two":
        if len(vals) != 2:
            raise RowFilterError(f"'{field}': 'is between' needs exactly two values.")
        return (f"{v} BETWEEN {_literal(family, vals[0], field)} "
                f"AND {_literal(family, vals[1], field)}")
    # list
    if not vals:
        raise RowFilterError(f"'{field}': choose at least one value.")
    if len(vals) > MAX_LIST:
        raise RowFilterError(f"'{field}': at most {MAX_LIST} values.")
    inlist = ", ".join(_literal(family, x, field) for x in vals)
    if op == "not_in":
        return f"({e} OR {v} NOT IN ({inlist}))"
    return f"{v} IN ({inlist})"


def _group_sql(g: dict, available: set[str], depth: int, counter: list[int]) -> str:
    if depth > MAX_DEPTH:
        raise RowFilterError(f"groups can be nested at most {MAX_DEPTH} levels deep.")
    logic = str(g.get("logic") or "AND").upper()
    if logic not in ("AND", "OR"):
        raise RowFilterError(f"unknown combinator '{g.get('logic')}' (use AND or OR).")
    nodes = g.get("conditions") or []
    if not nodes:
        raise RowFilterError("a condition group is empty.")
    parts = []
    for n in nodes:
        if _is_group(n):
            parts.append(_group_sql(n, available, depth + 1, counter))
        else:
            counter[0] += 1
            if counter[0] > MAX_CONDITIONS:
                raise RowFilterError(f"at most {MAX_CONDITIONS} conditions are allowed.")
            parts.append(_condition_sql(n, available))
    return "(" + f" {logic} ".join(parts) + ")"


def compile_row_filter(rf: Any, available_columns: Iterable[str]) -> str:
    """The ONLY place filter SQL is built. Returns a boolean SQL expression over
    the workset's columns; raises RowFilterError on anything invalid."""
    root = _root(rf)
    if not _is_group(root):
        raise RowFilterError("the row filter has no conditions.")
    return _group_sql(root, set(available_columns), 1, [0])


def is_active(rf: Any) -> bool:
    """A filter that actually restricts rows (an empty root means no filter)."""
    return bool(_root(rf).get("conditions"))


# ---------------------------------------------------------------- describe
def _describe_value(family: str, raw: Any) -> str:
    v = _CONTROL_RE.sub(" ", str(raw if raw is not None else "").strip())
    return v if family in ("number", "date") else "'" + v + "'"


def _describe_node(n: dict, top: bool) -> str:
    if _is_group(n):
        logic = str(n.get("logic") or "AND").upper()
        inner = f" {logic} ".join(_describe_node(x, False) for x in n.get("conditions") or [])
        return inner if top else f"({inner})"
    field = _CONTROL_RE.sub(" ", str(n.get("field")))
    op = OPERATORS.get(n.get("op") or "", {"label": str(n.get("op")), "arity": "one"})
    fam = type_family(n.get("field_type"))
    if op["arity"] == "none":
        return f"{field} {op['label']}"
    if op["arity"] == "one":
        return f"{field} {op['label']} {_describe_value(fam, n.get('value'))}"
    vals = [_describe_value(fam, x) for x in (n.get("values") or [])]
    if op["arity"] == "two" and len(vals) == 2:
        return f"{field} {op['label']} {vals[0]} and {vals[1]}"
    return f"{field} {op['label']} {', '.join(vals)}"


def describe_row_filter(rf: Any) -> str:
    """Plain English, one line, safe to embed in a script comment."""
    root = _root(rf)
    return _describe_node(root, True) if _is_group(root) else ""


# ---------------------------------------------------------------- normalise
def _fill_types(node: dict, columns: dict[str, dict]) -> dict:
    if _is_group(node):
        return {"logic": str(node.get("logic") or "AND").upper(),
                "conditions": [_fill_types(x, columns) for x in node.get("conditions") or []]}
    c = {"field": str(node.get("field") or ""), "op": str(node.get("op") or ""),
         "value": node.get("value"), "values": [str(x) for x in (node.get("values") or [])],
         "field_type": str(node.get("field_type") or "")}
    if c["value"] is not None:
        c["value"] = str(c["value"])
    meta = columns.get(c["field"]) if columns else None
    if meta is not None:
        # the dictionary, not the client, decides how a value is compared
        c["field_type"] = str(meta.get("inferred_type") or "")
    if type_family(c["field_type"]) == "date":
        # store dates in one form (YYYY-MM-DD) whichever the reviewer typed;
        # an unparseable value is left as-is for validation to report
        def iso(v):
            try:
                return _parse_date(str(v).strip(), c["field"]).isoformat()
            except RowFilterError:
                return v
        if c["value"] is not None:
            c["value"] = iso(c["value"])
        c["values"] = [iso(x) for x in c["values"]]
    return c


def _errors(node: dict, available: set[str], depth: int, counter: list[int]) -> list[str]:
    """Every problem in the filter, not just the first — the reviewer fixes
    them all in one pass."""
    if _is_group(node):
        out = []
        if depth > MAX_DEPTH:
            return [f"groups can be nested at most {MAX_DEPTH} levels deep."]
        if str(node.get("logic") or "AND").upper() not in ("AND", "OR"):
            out.append(f"unknown combinator '{node.get('logic')}' (use AND or OR).")
        if not node.get("conditions"):
            out.append("a condition group is empty.")
        for x in node.get("conditions") or []:
            out += _errors(x, available, depth + 1, counter)
        return out
    counter[0] += 1
    try:
        _condition_sql(node, available)
    except RowFilterError as e:
        return [str(e)]
    return []


def validate_row_filter(rf: Any, columns: dict[str, dict]) -> list[str]:
    root = _root(rf)
    if not _is_group(root):
        return ["the row filter has no conditions."]
    counter = [0]
    errs = _errors(root, set(columns), 1, counter)
    if counter[0] > MAX_CONDITIONS:
        errs.append(f"at most {MAX_CONDITIONS} conditions are allowed.")
    return errs


def normalise_row_filter(rf: Any, columns: dict[str, dict] | None,
                         declared_at: str = "") -> dict | None:
    """Turn what the reviewer submitted into the stored form: types from the
    dictionary, a plain-English description, the compiled SQL for display, and
    every validation error. An empty filter is no filter (None). Invalid
    filters are KEPT with their errors so the reviewer sees why."""
    d = _as_dict(rf)
    root = d.get("root") or {}
    if not _is_group(root) or not root.get("conditions"):
        return None
    root = _fill_types(root, columns or {})
    out = {"version": 1, "root": root, "declared_by": "reviewer",
           "declared_at": declared_at, "errors": [], "description": "",
           "compiled_sql": "", "source_rows": None, "kept_rows": None}
    out["description"] = describe_row_filter(out)
    if columns:
        out["errors"] = validate_row_filter(out, columns)
        if not out["errors"]:
            out["compiled_sql"] = compile_row_filter(out, columns.keys())
    return out
