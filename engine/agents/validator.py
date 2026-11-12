"""Agent 4 — the validation agent.

Validates a MappingSpec *as a whole* (Agent 3 already validates each rule in
isolation). It materialises the proposed target by applying every transformation
in the DuckDB staging layer, then runs check families that no single rule can
reveal:

  * wellformed     — no duplicate targets, source refs exist, every transform compiles
  * grain          — row count preserved
  * key_integrity  — target key is unique and non-null
  * completeness   — required (non-nullable) targets are populated
  * crossfield     — decoded business rules still hold in the target shape
  * reconciliation — no unexpected value loss vs source

Failures carry the actual offending rows, and the agent demotes gates
accordingly. Almost entirely deterministic — validation is the last place you
want a model guessing. The LLM (optional) only summarises the report.
"""
from __future__ import annotations

import json
import os
import re
from collections import Counter
from typing import Optional

from ..etl_sql import classify_expression, q
from ..llmjson import _llm_json
from ..models import Gate, target_attributes
from ..staging import Warehouse
from .contracts import (CheckResult, GateAdjustment, MappingSpec,
                        TableInsight, ValidationReport)

SENTINELS = ("00000000", "")
_MAT = "_val_target"   # materialised target table name


def _pop_pred(cols: list[str]) -> str:
    """Rows in which at least one source column carries a real value.

    A mapping may legitimately have NO source column — a load-time default
    promoted from an unmapped target ('SYS1', false, a load timestamp). For
    those, every row qualifies, so the predicate is TRUE. Returning "" here
    would splice an empty WHERE clause into the SQL and raise a parser error.
    """
    if not cols:
        return "TRUE"
    return " OR ".join(
        f"(\"{c}\" IS NOT NULL AND \"{c}\" NOT IN ('00000000',''))" for c in cols
    )


def _rows(wh: Warehouse, sql: str, limit: int = 5) -> list[dict]:
    cur = wh.con.execute(sql + f" LIMIT {limit}")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def validate_spec(spec: MappingSpec, target_dict: dict, insight: TableInsight,
                  warehouse: Warehouse, source_table: str) -> ValidationReport:
    wh = warehouse
    src_cols = set(wh.column_names(source_table))
    checks: list[CheckResult] = []
    # target_attr -> worst severity seen, for gate demotion
    failed: dict[str, str] = {}

    def add(name, cat, status, detail, *, sev="soft", attr=None, n=0, sample=None):
        checks.append(CheckResult(name=name, category=cat, status=status, severity=sev,
                                  detail=detail, target_attribute=attr,
                                  offending_rows=n, sample=sample or []))
        if status == "fail" and attr:
            failed[attr] = "hard" if sev == "hard" else failed.get(attr, "soft")

    # ---------------- row filter (scopes every check below) -----------
    # The reviewer's row filter decides WHICH source rows migrate, so every
    # check — compile probes, grain, keys, completeness, value loss — must run
    # on the rows in scope, exactly as the transform will see them. A filter
    # that cannot be compiled is a hard finding; the remaining checks then run
    # on the unfiltered source so their findings are not lost as well.
    filtered_from = None
    if spec.row_filter is not None:
        from ..row_filter import RowFilterError, compile_row_filter, describe_row_filter
        rf = spec.row_filter
        cond, why = None, list(rf.errors or [])
        if not why:
            try:
                cond = compile_row_filter(rf, src_cols)
            except RowFilterError as e:
                why = [str(e)]
        if why:
            add("row_filter_valid", "wellformed", "fail",
                "the row filter cannot be applied: " + "; ".join(why), sev="hard")
        else:
            desc = describe_row_filter(rf)
            wh.con.execute(f'CREATE OR REPLACE VIEW "__workset_filtered" AS '
                           f'SELECT * FROM "{source_table}" WHERE {cond}')
            total = wh.row_count(source_table)
            kept = wh.row_count("__workset_filtered")
            rf.source_rows, rf.kept_rows = total, kept
            filter_sql = (f'SELECT count(*) AS kept_rows FROM "{source_table}"\n'
                          f'WHERE {cond};')
            if kept == 0:
                add("row_filter_effect", "grain", "fail",
                    f"the row filter excludes every source row ({total}) — nothing "
                    f"would migrate: {desc}", sev="hard")
            elif kept == total:
                add("row_filter_effect", "grain", "warn",
                    f"the row filter excludes no rows (all {total} kept): {desc}")
            else:
                add("row_filter_effect", "grain", "pass",
                    f"kept {kept} of {total} source rows; {total - kept} excluded "
                    f"by the row filter: {desc}")
            checks[-1].sql, checks[-1].rows_scanned = filter_sql, total
            filtered_from, source_table = source_table, "__workset_filtered"

    # ---------------- wellformed (before materialising) ----------------
    dups = [t for t, c in Counter(m.target_attribute for m in spec.mappings).items() if c > 1]
    add("no_duplicate_targets", "wellformed", "fail" if dups else "pass",
        f"target(s) written more than once: {dups}" if dups else "each target written once.",
        sev="hard")

    # A REJECTED mapping is not accepted, so it is never materialised. Executing
    # SQL we have already rejected can only produce a spurious hard failure: a
    # coded column matched to a numeric target compiled, threw on the real rows,
    # and blocked the entire run on a mapping nobody intended to use. The
    # attribute is instead treated as having no source, so completeness reports
    # the actionable fact ("this required attribute has no accepted source")
    # rather than a cast error nobody asked about.
    #
    # Rejects are NOT ignored: they stay in the review queue, and the verdict
    # below still blocks until a human has ruled on them. Once a reviewer
    # accepts or edits one it stops being a reject, and the next validation pass
    # (review -> apply_decisions -> validation) executes it normally.
    live = [m for m in spec.mappings if m.gate != "reject"]

    # ---------------- declared grain -----------------------------------
    # One target row per group of the reviewer's group-by columns, instead of
    # one per source row. An invalid grain is a hard finding and the rest is
    # validated at source grain, so its findings are not lost as well.
    keys = None
    if spec.grain is not None:
        why = list(spec.grain.errors or [])
        unknown = [k for k in spec.grain.group_by if k not in src_cols]
        if not spec.grain.group_by:
            why.append("no group-by column was chosen")
        if unknown:
            why.append(f"group-by column(s) {unknown} are not in the source")
        add("grain_declared_valid", "wellformed", "fail" if why else "pass",
            ("the target grain cannot be applied: " + "; ".join(why)) if why else
            f"one target row per {', '.join(spec.grain.group_by)}.", sev="hard")
        if not why:
            keys = list(spec.grain.group_by)
    rel = f'"{source_table}"'
    gb = f" GROUP BY {', '.join(q(k) for k in keys)}" if keys else ""

    # Each expression is CLASSIFIED (row / window / aggregate / group_key) by
    # DuckDB's parser and checked against the grain, then bound against the
    # real columns. Probing with `count(<expr>)` used to reject every window
    # function — DuckDB forbids a window inside an aggregate — although the
    # transform ran it fine; the probes below are shaped like the statement
    # the transform actually runs.
    compilable, bad_ref, no_compile = [], [], []
    for m in live:
        if not m.transformation_sql:
            continue
        missing = [c for c in m.source_attributes if c not in src_cols]
        if missing:
            bad_ref.append((m.target_attribute, missing)); continue
        c = classify_expression(m.transformation_sql, wh.con, rel, keys,
                                attr=m.target_attribute)
        m.transform_kind, m.partition_by = c.kind, c.partition_by
        m.suggested_sql = c.suggested_sql or ""
        if c.error_code == "E-EXPR-BIND":
            no_compile.append((m.target_attribute, c.message))
            continue
        if not c.ok:
            # a column that is merely not wrapped yet (constant per group) goes
            # back for review with the ANY_VALUE suggestion; anything else is
            # a hard finding the reviewer must rewrite
            add(f"transform_kind:{m.target_attribute}", "wellformed", "fail",
                c.message, sev="soft" if c.error_code == "I-EXPR-ANY-VALUE" else "hard",
                attr=m.target_attribute)
            continue
        compilable.append(m)
    add("source_refs_exist", "wellformed", "fail" if bad_ref else "pass",
        f"accepted mappings referencing unknown columns: {bad_ref}" if bad_ref
        else "all source refs in accepted mappings are valid.", sev="hard")
    add("transforms_compile", "wellformed", "fail" if no_compile else "pass",
        f"accepted transforms failing to compile: {no_compile}" if no_compile
        else "all accepted transforms compile.", sev="hard")

    # a transform can compile yet still throw on the REAL rows (bad cast on a
    # wrongly-guessed source, etc.) — probe each one and turn failures into
    # findings against that mapping instead of letting them kill the run
    executable, no_execute = [], []
    for m in compilable:
        try:
            if keys:
                wh.con.execute(f"SELECT count(v) FROM (SELECT ({m.transformation_sql}) AS v "
                               f"FROM {rel}{gb})")
            else:
                wh.con.execute(f"SELECT count(v) FROM (SELECT ({m.transformation_sql}) AS v, * "
                               f"FROM {rel})")
            executable.append(m)
        except Exception as e:
            no_execute.append((m.target_attribute, str(e).splitlines()[0]))
    add("transforms_execute", "wellformed", "fail" if no_execute else "pass",
        (f"accepted transforms failing on the actual data (wrong source?): {no_execute}"
         if no_execute else "all accepted transforms execute on the staged data."),
        sev="hard")
    for attr, err in no_execute:
        add(f"transform_runtime:{attr}", "wellformed", "fail",
            f"transformation throws on real rows: {err}", sev="hard", attr=attr)

    # ---------------- materialise the proposed target ------------------
    # `executable` can legitimately be EMPTY — every mapping rejected, or none
    # survived compilation. An empty column list produced "SELECT , * FROM" and
    # a raw ParserException, crashing the run instead of reporting it. Excluding
    # rejects from materialisation makes that state reachable, so it must be a
    # finding rather than a stack trace.
    cols_sql = ", ".join(f'{m.transformation_sql} AS "{m.target_attribute}"'
                         for m in executable)
    if keys:
        # grouped: one row per group, plus the group keys (as __gk_i) so the
        # value-loss check below can relate each group back to its source rows
        gk = ", ".join(f"{q(k)} AS {q(f'__gk_{i}')}" for i, k in enumerate(keys))
        wh.con.execute(f"CREATE OR REPLACE TABLE {_MAT} AS SELECT "
                       f"{cols_sql + ', ' if cols_sql else ''}{gk} FROM {rel}{gb}")
    else:
        projection = f"{cols_sql}, *" if cols_sql else "*"
        wh.con.execute(f'CREATE OR REPLACE TABLE {_MAT} AS '
                       f'SELECT {projection} FROM "{source_table}"')
    if not executable:
        add("no_accepted_transforms", "wellformed", "fail",
            "no accepted mapping produced a usable transform — every mapping was "
            "rejected or failed to compile, so there is nothing to validate.",
            sev="hard")
    src_n = wh.row_count(source_table)
    tgt_n = wh.con.execute(f"SELECT count(*) FROM {_MAT}").fetchone()[0]

    # ---------------- grain --------------------------------------------
    after = ""
    if filtered_from is not None:
        after = f" after the row filter ({(spec.row_filter.source_rows or 0) - src_n} excluded)"
    if keys:
        groups = wh.con.execute(f"SELECT count(*) FROM (SELECT 1 FROM {rel}{gb})").fetchone()[0]
        add("grain_row_count", "grain", "pass" if groups == tgt_n else "fail",
            f"{src_n} source rows{after} -> {groups} distinct {', '.join(keys)} group(s) "
            f"-> {tgt_n} target rows.", sev="hard")
        # each delivered row must be identifiable by the group key: some
        # group-key attributes together must carry EVERY group-by column
        gkm = [m for m in executable if m.transform_kind == "group_key"]
        covered = {c for m in gkm for c in classify_expression(m.transformation_sql).columns}
        uncovered = [k for k in keys if k not in covered]
        if uncovered or not gkm:
            add("grain_key_mapped", "key_integrity", "fail",
                f"No target attribute carries the grain key {uncovered or keys}, so "
                f"delivered rows can't be identified.", sev="hard")
        else:
            tup = ", ".join(q(m.target_attribute) for m in gkm)
            nulls = wh.con.execute(f"SELECT count(*) FROM {_MAT} WHERE "
                                   + " OR ".join(f"{q(m.target_attribute)} IS NULL" for m in gkm)
                                   ).fetchone()[0]
            distinct = wh.con.execute(f"SELECT count(*) FROM (SELECT DISTINCT {tup} FROM {_MAT})"
                                      ).fetchone()[0]
            add("grain_key_mapped", "key_integrity", "pass",
                f"the grain key is carried by {[m.target_attribute for m in gkm]}.")
            ok = nulls == 0 and distinct == tgt_n
            add("key_unique_not_null", "key_integrity", "pass" if ok else "fail",
                f"group key {[m.target_attribute for m in gkm]}: {nulls} null(s), "
                f"{distinct}/{tgt_n} distinct.", sev="hard", n=nulls)
    else:
        add("row_count_preserved", "grain", "pass" if src_n == tgt_n else "fail",
            f"{src_n} source rows{after} -> {tgt_n} target rows.", sev="hard")

    # ---------------- key integrity ------------------------------------
    # only trusted 1:1 mappings can stand in for the key — a rejected/weak mapping
    # that happens to borrow the key column as a throwaway source must not count.
    # a window or aggregate is a per-GROUP value, so it can never stand in for
    # a row's source column (key, crossfield) even when it names one source
    one_to_one: dict[str, str] = {}
    for m in spec.mappings:
        if m.cardinality == "1:1" and len(m.source_attributes) == 1 and m.gate != "reject" \
                and m.transform_kind in ("row", "group_key"):
            one_to_one.setdefault(m.source_attributes[0], m.target_attribute)
    # The insight's candidate keys are only populated by the FULL analyst pass.
    # The mapping flow runs analyze_light(), which returns no columns and no
    # candidate keys, so this asked for a key that was never computed and then
    # reported "no target key identified from source key" — blaming the mapping
    # for a gap in the analysis. The key was mapped, 1:1 and auto-accepted, the
    # whole time.
    #
    # Derive it here instead: one aggregate over the staged source, so the check
    # no longer depends on which analysis path ran.
    candidates = list(insight.candidate_keys or [])
    if keys:
        candidates = []          # the grain's group key was checked above
    elif not candidates:
        try:
            src_n = wh.con.execute(f'SELECT count(*) FROM "{source_table}"').fetchone()[0]
            for col in wh.column_names(source_table):
                if col not in one_to_one:
                    continue          # only a column that reaches the target can be the key
                d, nulls = wh.con.execute(
                    f'SELECT count(DISTINCT "{col}"), count(*) - count("{col}") '
                    f'FROM "{source_table}"').fetchone()
                if d == src_n and not nulls:
                    candidates.append(col)
                    break             # the first unique, non-null mapped column
        except Exception:             # noqa: BLE001
            candidates = []
    key_tgt = one_to_one.get(candidates[0]) if candidates else None
    if key_tgt:
        nulls = wh.con.execute(f'SELECT count(*) FROM {_MAT} WHERE "{key_tgt}" IS NULL').fetchone()[0]
        distinct = wh.con.execute(f'SELECT count(DISTINCT "{key_tgt}") FROM {_MAT}').fetchone()[0]
        ok = (nulls == 0 and distinct == tgt_n)
        add("key_unique_not_null", "key_integrity", "pass" if ok else "fail",
            f"key '{key_tgt}': {nulls} null(s), {distinct}/{tgt_n} distinct.",
            sev="hard", attr=key_tgt, n=nulls)
    elif not keys:
        add("key_identified", "key_integrity", "warn",
            "no source column is both unique and non-null across every row, so no "
            "key could be confirmed — check whether this extract has a business key.")

    # ---------------- completeness (required targets) ------------------
    materialised = {m.target_attribute for m in executable}
    mapped = {m.target_attribute for m in spec.mappings} & materialised
    unmapped_t = {u["attribute"] for u in spec.unmapped_target}
    for attr in target_attributes(target_dict):
        name, required = attr["name"], not attr.get("nullable", False)
        if not required:
            continue
        if name in unmapped_t:
            add(f"required_present:{name}", "completeness", "warn",
                f"No source field maps to required attribute '{name}'. A default value must be set at load time.", attr=name)
        elif name in mapped:
            nulls = wh.con.execute(f'SELECT count(*) FROM {_MAT} WHERE "{name}" IS NULL').fetchone()[0]
            add(f"required_present:{name}", "completeness", "fail" if nulls else "pass",
                f"'{name}' required; {nulls} null(s) after transform." if nulls
                else f"'{name}' fully populated.", sev="hard", attr=name, n=nulls,
                sample=_rows(wh, f'SELECT "{name}" FROM {_MAT} WHERE "{name}" IS NULL') if nulls else None)

    # ---------------- crossfield (decoded business rules) --------------
    # source-ROW rules: under a declared grain a target row is a group, and the
    # rule no longer describes it
    for d in ([] if keys else insight.dependencies):
        dep_tgt = one_to_one.get(d.dependent)
        if dep_tgt and dep_tgt not in materialised:
            continue
        mobj = re.search(r"(\w+)\s+in\s+\{([^}]*)\}", d.condition or "")
        if not (dep_tgt and mobj):
            continue
        driver, vals = mobj.group(1), [v.strip() for v in mobj.group(2).split(",")]
        inlist = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
        where = f'"{dep_tgt}" IS NOT NULL AND "{driver}" NOT IN ({inlist})'
        bad = wh.con.execute(f"SELECT count(*) FROM {_MAT} WHERE {where}").fetchone()[0]
        add(f"crossfield:{dep_tgt}~{driver}", "crossfield", "pass" if not bad else "fail",
            f"'{dep_tgt}' populated only when {driver} in {{{', '.join(vals)}}}: "
            f"{bad} violation(s)." , sev="soft", attr=dep_tgt, n=bad,
            sample=_rows(wh, f'SELECT "{dep_tgt}", "{driver}" FROM {_MAT} WHERE {where}') if bad else None)

    # ---------------- reconciliation (value loss) ----------------------
    for m in executable:
        # a mapping with no source column is a constant / load-time default —
        # there is no source value that could have been lost, so reconciling it
        # is meaningless (and the sample query would have no columns to select)
        if not m.source_attributes:
            add(f"reconciliation:{m.target_attribute}", "reconciliation", "pass",
                f"'{m.target_attribute}': load-time default, no source value to reconcile.",
                attr=m.target_attribute)
            continue
        if m.transform_kind in ("window", "aggregate"):
            # A per-partition / per-group figure is legitimately NULL when the
            # partition has no qualifying rows (a SUM ... FILTER over a policy
            # with no active beneficiary). "Source populated, target NULL" is a
            # ROW property and would raise false alarms here; these columns are
            # verified by re-execution (validation workspace) and, for windows,
            # by one-value-per-partition.
            add(f"reconciliation:{m.target_attribute}", "reconciliation", "pass",
                f"'{m.target_attribute}': {m.transform_kind} column — a NULL is a valid "
                f"result for a {'partition' if m.transform_kind == 'window' else 'group'} "
                f"with no qualifying rows; values are verified by re-execution.",
                attr=m.target_attribute)
            continue
        pred = _pop_pred(m.source_attributes)
        if keys:
            # per GROUP: a group in which some source row carried a value must
            # not come out NULL. Groups are related back by their keys (__gk_i).
            popg = (f"SELECT DISTINCT {', '.join(q(k) for k in keys)} FROM {rel} "
                    f"WHERE {pred}")
            on = " AND ".join(f"t.{q(f'__gk_{i}')} IS NOT DISTINCT FROM p.{q(k)}"
                              for i, k in enumerate(keys))
            src_pop = wh.con.execute(f"SELECT count(*) FROM ({popg})").fetchone()[0]
            tgt_nn = wh.con.execute(
                f"SELECT count(*) FROM {_MAT} t JOIN ({popg}) p ON {on} "
                f"WHERE t.{q(m.target_attribute)} IS NOT NULL").fetchone()[0]
            loss = src_pop - tgt_nn
            if loss > 0:
                add(f"reconciliation:{m.target_attribute}", "reconciliation", "warn",
                    f"{loss} group(s) had a source value but '{m.target_attribute}' "
                    f"is NULL after the transform.", attr=m.target_attribute, n=loss)
            else:
                add(f"reconciliation:{m.target_attribute}", "reconciliation", "pass",
                    f"'{m.target_attribute}': no value loss ({tgt_nn}/{src_pop} groups).",
                    attr=m.target_attribute)
            continue
        src_pop = wh.con.execute(f'SELECT count(*) FROM {_MAT} WHERE {pred}').fetchone()[0]
        tgt_nn = wh.con.execute(
            f'SELECT count(*) FROM {_MAT} WHERE "{m.target_attribute}" IS NOT NULL AND ({pred})'
        ).fetchone()[0]
        loss = src_pop - tgt_nn
        if loss > 0:
            # show the distinct source values that were populated but produced NULL
            samp = _rows(wh, f'SELECT DISTINCT {", ".join(chr(34)+c+chr(34) for c in m.source_attributes)} '
                             f'FROM {_MAT} WHERE "{m.target_attribute}" IS NULL AND ({pred})')
            add(f"reconciliation:{m.target_attribute}", "reconciliation", "warn",
                f"{loss} source row(s) had a value but became NULL in '{m.target_attribute}' "
                f"after the transform — likely an unmapped code.", attr=m.target_attribute, n=loss, sample=samp)
        else:
            add(f"reconciliation:{m.target_attribute}", "reconciliation", "pass",
                f"'{m.target_attribute}': no value loss ({tgt_nn}/{src_pop}).", attr=m.target_attribute)

    # ---------------- gate demotion ------------------------------------
    adjustments: list[GateAdjustment] = []
    for m in spec.mappings:
        sev = failed.get(m.target_attribute)
        if not sev:
            continue
        to = Gate.REJECT.value if sev == "hard" else Gate.REVIEW.value
        order = {"reject": 0, "review": 1, "auto_accept": 2}
        if order[to] < order.get(m.gate, 2):
            adjustments.append(GateAdjustment(
                target_attribute=m.target_attribute, from_gate=m.gate, to_gate=to,
                reason=f"failed validation ({sev})"))
            m.gate = to  # apply to the spec in place

    # ---------------- verdict ------------------------------------------
    hard_fail = any(c.status == "fail" and c.severity == "hard" for c in checks)
    # an unresolved review always blocks; a reject blocks until a human has ruled on
    # it — once the queue has been reviewed, a confirmed reject is a deliberate
    # exclusion (handle separately / don't migrate), not an outstanding data problem.
    reviewed = "reviewed" in spec.generated_by
    # WARNINGS ARE ADVISORY, NOT BLOCKING. A warn is an observation about the
    # data (e.g. "3 rows carried an exit reason with no target enum value") —
    # it is surfaced on the report and on the item's card, but NO reviewer
    # decision can clear it, because accepting a mapping does not change the
    # underlying rows. Treating warns as blocking pinned the verdict at
    # needs_review forever: the reviewer resolved every item, certified, and
    # got the same queue back. Only real failures and undecided gates block.
    blocking = (
        any(c.status == "fail" for c in checks)
        or any(m.gate == "review" for m in spec.mappings)
        or (not reviewed and any(m.gate == "reject" for m in spec.mappings))
    )
    verdict = "blocked" if hard_fail else ("needs_review" if blocking else "certified")

    stats = {
        "checks": len(checks),
        "passed": sum(c.status == "pass" for c in checks),
        "warnings": sum(c.status == "warn" for c in checks),
        "failures": sum(c.status == "fail" for c in checks),
        "gate_demotions": len(adjustments),
    }
    stats["grain"] = keys
    stats["column_kinds"] = dict(Counter(m.transform_kind for m in executable))
    if spec.row_filter is not None:
        stats["row_filter"] = {"description": spec.row_filter.description,
                               "source_rows": spec.row_filter.source_rows,
                               "kept_rows": spec.row_filter.kept_rows}
    report = ValidationReport(
        source_table=spec.source_table, target_table=spec.target_table,
        verdict=verdict, checks=checks, gate_adjustments=adjustments, stats=stats,
    )
    return _narrate(report)


# ----------------------------------------------------- LLM summary (optional)
def _narrate(report: ValidationReport) -> ValidationReport:
    from .. import config
    client, model = config.llm_client()
    if client is None:
        report.generated_by = "deterministic+offline_stub"
        return report

    sys = ("You are a data-migration QA lead. Summarise this validation report into a "
           "short, prioritised list of what a reviewer must act on. Do NOT change any "
           "pass/fail verdicts — they are computed. Return ONLY JSON: {summary: [strings]}.")
    try:
        # Routed through _llm_json: it is the one place that speaks the
        # client, so a provider change lands here only — and these calls
        # gain the retries, truncation check and JSON repair they lacked.
        out = _llm_json(client,
                        [{"role": "system", "content": sys},
                         {"role": "user", "content": json.dumps(
                             report.model_dump(), default=str)}])
        report.stats["llm_summary"] = out.get("summary") or []
        report.generated_by = "deterministic+llm"
    except Exception as e:
        report.stats["llm_note"] = f"summary skipped: {e}"
    return report
