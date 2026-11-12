# Prompt: implement a reviewer-defined source row filter

> **How to use:** paste everything below the line into Claude Code, run from
> `C:\migration_code\datamap_v11_share`. It is written to be followed as-is.

---

## 0. Your task

Implement a **source row filter** that the reviewer defines in the
human-in-the-loop (review) step of the Mapping workspace. Only source rows that
match the filter are migrated. The filter must be honoured, and verified,
everywhere downstream: certify validation, Transformation (run and generated
script), Validation, and Reconciliation (run and generated script).

Example the user gave: *"exit date equals blank"*. In this data that is
`ENDDTE` (role end date) **is empty**, i.e. blank or `00000000`, which means
"only open roles". The user also wants several conditions combined with
**AND / OR**.

Work in this order:

1. Read the files listed in section 2.
2. Implement section 5 file by file.
3. Write the tests in section 7.
4. Run the verification in section 8.
5. Report back: what changed (with file links), test results with real output,
   and anything you could not do.

Do not commit to git; this folder is not a git repository.

---

## 1. Background you need

* The app has four workspaces: Mapping → Transformation → Validation →
  Reconciliation. Read `README.md` first. Its "Design principles" are binding,
  in particular:
  * **Structured rules, never prose**: a reviewer-authored rule is structured
    data, validated, and compiled deterministically. The reviewer never types SQL.
  * **One derivation, one artefact.**
  * **No silent skips.**
  * **Evidence**: every check records its SQL and row count.
  * **Scripts are reproducible by construction.**
* The mapping runs against a **workset**: the primary source file LEFT JOINed to
  the other files (`engine/composite.py`, view `"__workset"`). For a single file,
  the workset is the staged table itself (named after the file, e.g. `CO040`).
  Column names in the workset are the dictionary names. Clashing names from a
  joined file become `{TABLE}_{COLUMN}` (e.g. `PO010_STRDTE`).
* All source data is staged as **VARCHAR** (`all_varchar=true`). Dates are
  `YYYYMMDD` text, and `00000000` means "no date". The source dictionary's
  `inferred_type` tells you the real type: `DATE_YYYYMMDD`, `INTEGER`,
  `DECIMAL`, `CATEGORICAL_CODE`, `IDENTIFIER`, `FREE_TEXT`, `FLAG`, `CONSTANT`,
  `EMPTY`.
* The review step is **certify**: `POST /api/mapping/certify` with
  `{spec, decisions}` → graph `flow_mapping_manual_certify`
  (`manual_seed_spec → apply_decisions → validation → review`). Certify never
  re-runs the mapping agent.
* Workspaces 2-4 are **server-stateless**: the browser posts the certified spec
  back on every call (`api/server.py: _spec_from_body`). **Never trust SQL text
  from the client.** Always recompile the filter from its structured form on the server.
* A precedent for structured, reviewer-authored rules already exists: the
  reconciliation "Add a control" form (`web/static/app.js`, around lines
  3060-3118) and its validator `api/recon_rules.py: validate_rule`. Follow the same style.

---

## 2. Read these before changing anything

| File | Why |
|---|---|
| `README.md` | design principles |
| `engine/agents/contracts.py` | `MappingSpec`, `MappingEntry`, `CheckResult`, `EnrichedColumn` |
| `engine/agents/reviewer.py` | `apply_decisions`, `build_review_queue` |
| `engine/orchestration/nodes.py` | `apply_decisions_node`, `validation_node`, `manual_seed_spec_node`, `_combine_sources` |
| `engine/orchestration/graph.py` | flows used by the UI |
| `engine/agents/validator.py` | in-pipeline checks; every use of `source_table` |
| `engine/staging.py` | `Warehouse` helpers (work with views too) |
| `api/server.py` | `_run_mapping_manual`, `_run_mapping_manual_certify`, transform/validate/reconcile endpoints |
| `api/transform.py` | `build_workset_sql`, `build_full_sql`, `run_transform`, `generate_python` |
| `api/validate.py` | lines ~176-196 (row count), ~385-487 (re-execution on `_srcn`), `describe_validation_rules`, `generate_validation_script` |
| `api/reconcile.py` | `run_reconciliation` (every `FROM ({workset_sql})`, lines ~165-470), `describe_reconciliation_rules`, `generate_reconciliation_script` (uses `FROM {primary}` at ~687, 717, 733, 770) |
| `api/recon_rules.py` | `derive_candidates` (`control_total:rows` rule ~144), `rule_id` |
| `web/static/index.html` | `#review-section`, `#review-list`, `#final-section` |
| `web/static/app.js` | `run()` (~170-246), SSE `complete` handling (~587), `renderReview` (~676), `handleAct`, `decisionFor`, `renderFinalMapping` (~974), `buildSQL`/`buildMD` (~1006-1027), transform console (~1629-1646) |
| `tests/test_smoke.py`, `tests/test_frontend_scope.py` | test style; frontend scope guard |

---

## 3. Requirement

### 3.1 User story

As a reviewer, in the review step I can add a **row filter**:

* pick an input field from a dropdown of **all source fields** (every column of
  the workset, from every uploaded file);
* pick a condition (operator);
* enter or choose a value;
* add more conditions and combine them with **AND / OR**, including one level of
  bracketed groups, e.g. `ENDDTE is empty AND (ROLREF = PH OR ROLREF = PY)`;
* see **how many rows will be kept** before certifying;
* certify.

From then on only matching rows are migrated, and every later stage knows about
the filter and checks it.

### 3.2 Rules

1. The filter is **optional**. With no filter, behaviour is exactly as today,
   with byte-identical generated scripts.
2. There is **one** filter per mapping (per target). It applies to the
   workset, after joins and before any column transformation.
3. The reviewer never writes SQL. The filter is structured data compiled to SQL
   by one deterministic server-side function.
4. The filter can be changed or removed later through **Amend decisions** and
   re-certify.
5. Excluded rows are **reported**, never silently lost. Reports show:
   total source rows, rows kept, rows excluded by the filter.
6. A filter that keeps **0 rows** blocks certification. A filter that excludes
   nothing gives a warning.
7. Field, operator and value are validated on the server. An invalid filter is
   shown with its reason and blocks certification. It is never dropped silently.

---

## 4. Design decisions (fixed: do not change without asking)

### 4.1 Data contract (`engine/agents/contracts.py`)

```python
class FilterCondition(BaseModel):
    field: str                        # workset column name, e.g. "ENDDTE"
    op: str                           # see 4.2
    value: Optional[str] = None       # single value (eq, ne, gt, ge, lt, le, contains, starts_with, ends_with)
    values: list[str] = Field(default_factory=list)   # in / not_in (1..200), between (exactly 2)
    field_type: str = ""              # SERVER-FILLED from the source dictionary; client value ignored

class FilterGroup(BaseModel):
    logic: str = "AND"                # "AND" | "OR"
    conditions: list[Union["FilterCondition", "FilterGroup"]] = Field(default_factory=list)

class RowFilter(BaseModel):
    version: int = 1
    root: FilterGroup
    description: str = ""             # SERVER-GENERATED plain English (describe_row_filter)
    compiled_sql: str = ""            # SERVER-GENERATED, for DISPLAY/AUDIT ONLY — never executed from here
    declared_by: str = "reviewer"
    declared_at: str = ""             # ISO timestamp, server-set
    errors: list[str] = Field(default_factory=list)   # validation errors, kept visible
    source_rows: Optional[int] = None # filled by the validator
    kept_rows: Optional[int] = None   # filled by the validator

class MappingSpec(BaseModel):
    ...
    row_filter: Optional[RowFilter] = None   # None = no filter (today's behaviour)
```

* Limits: at most **20 conditions** in total and **nesting depth 2** in the UI
  (root group plus one level of sub-groups). The engine accepts depth ≤ 4.
* Use `model_rebuild()` for the forward reference.
* Old specs without `row_filter` must still load (default `None`).

### 4.2 Operators and their SQL

Create **`engine/row_filter.py`**. It must be pure Python with no `api` imports,
and has these functions:

```python
OPERATORS: dict[str, dict]            # op -> {label, arity, types}
def type_family(inferred_type: str) -> str          # "date" | "number" | "text"
def validate_row_filter(rf: dict | RowFilter, columns: dict[str, dict]) -> list[str]
    # columns: {workset_name: {"inferred_type", "business_name", "origin_table", "value_decode"}}
def normalise_row_filter(rf, columns) -> RowFilter  # fills field_type, description, compiled_sql, errors
def compile_row_filter(rf: dict | RowFilter, available_columns: Iterable[str]) -> str
    # returns a SQL boolean expression; raises RowFilterError on anything invalid
def describe_row_filter(rf) -> str                  # plain English
```

Type family from the condition's `field_type`:

| `inferred_type` | family |
|---|---|
| `DATE_YYYYMMDD` (or any type starting with `DATE`) | date |
| `INTEGER`, `DECIMAL`, `AMOUNT`, `NUMBER`, `NUMERIC` | number |
| anything else, or empty | text |

Value expression `V(c)` per family. `c` is the quoted identifier, quoted the
same way as `api/transform._q`.

| family | `V(c)` |
|---|---|
| text | `trim(c)` |
| number | `TRY_CAST(trim(c) AS DECIMAL(38,10))` |
| date | `TRY_STRPTIME(NULLIF(trim(c), '00000000'), '%Y%m%d')::DATE` |

Empty test `E(c)`:

* text and number: `(c IS NULL OR trim(c) = '')`
* date: `(c IS NULL OR trim(c) IN ('', '00000000'))`

Operators (label shown in the UI → SQL):

| op | UI label | types | SQL |
|---|---|---|---|
| `is_empty` | is empty | all | `E(c)` |
| `is_not_empty` | is not empty | all | `NOT E(c)` |
| `eq` | equals | all | `V(c) = L` |
| `ne` | does not equal | all | `(E(c) OR V(c) <> L)`: **empty rows count as "not equal"** (state this in the UI hint) |
| `in` | is one of | all | `V(c) IN (L1, L2, …)` |
| `not_in` | is not one of | all | `(E(c) OR V(c) NOT IN (L1, …))` |
| `gt` / `ge` / `lt` / `le` | greater than / at least / less than / at most | number, date | `V(c) > L` … |
| `between` | between (inclusive) | number, date | `V(c) BETWEEN L1 AND L2` |
| `contains` / `starts_with` / `ends_with` | contains / starts with / ends with | text | `contains(V(c), L)` / `starts_with(V(c), L)` / `ends_with(V(c), L)` |

Literal `L` per family. **Validate before quoting; reject anything else:**

* text: `'…'` with `'` doubled. The value is trimmed.
* number: must match `^-?\d+(\.\d+)?$`. Emitted as `CAST('<v>' AS DECIMAL(38,10))`.
* date: input `YYYY-MM-DD` (also accept `YYYYMMDD`). Must parse as a real date.
  Emitted as `DATE 'YYYY-MM-DD'`.

Groups compile to `(<c1> AND|OR <c2> …)`. An empty group is invalid. A root
with zero conditions means "no filter": `apply_decisions` stores `None`.

`compile_row_filter` raises `RowFilterError` when:

* a field is not in `available_columns`;
* the op is unknown or not allowed for the field's family;
* the arity is wrong;
* a value is malformed;
* the condition limit is exceeded.

**No other code path may build filter SQL.** Every consumer calls `compile_row_filter`.

### 4.3 Decision payload (certify)

```json
"decisions": {
  "__row_filter__": {"action": "set_filter", "filter": { "version": 1, "root": { "logic": "AND", "conditions": [
      {"field": "ENDDTE", "op": "is_empty"},
      {"logic": "OR", "conditions": [
          {"field": "ROLREF", "op": "eq", "value": "PH"},
          {"field": "ROLREF", "op": "eq", "value": "PY"}]}]}}},
  "...attribute decisions as today...": {}
}
```

* `{"action": "clear_filter"}` removes it.
* No `__row_filter__` key means **keep** whatever `spec.row_filter` already holds.
  Certify builds on the latest certified spec.

### 4.4 Where the filter is applied

* **Certify validator:** a view `"__workset_filtered"` =
  `SELECT * FROM "<source_table>" WHERE <compiled>`. Every check in
  `validate_spec` uses that view as its source.
* **Transform / validate / reconcile:** one new helper in `api/transform.py`:

  ```python
  def build_source_sql(spec, paths) -> tuple[str, list[str]]:
      """The workset AFTER the reviewer's row filter: the single population every
      workspace treats as 'the source'."""
      ws, params = build_workset_sql(spec, paths)
      rf = spec.get("row_filter")
      if not rf:
          return ws, params
      cond = compile_row_filter(rf, _columns_of_workset(spec, paths))
      return f"SELECT * FROM (\n{_indent(ws)}\n) AS __wf\nWHERE {cond}", params
  ```

  `build_full_sql`, `validate.py` and `reconcile.py` call `build_source_sql`
  instead of `build_workset_sql` wherever they mean "the source population".
  Keep `build_workset_sql` for the unfiltered total.
* Row order is preserved: `WHERE` over a plain scan does not reorder, so
  ordinal alignment in validation still holds.

---

## 5. Changes, file by file

### 5.1 `engine/row_filter.py` (new)

As specified in 4.2. Module docstring in the style of the other engine
modules: it explains *why* the filter is structured, not just *what* it does.

### 5.2 `engine/agents/contracts.py`

Add `FilterCondition`, `FilterGroup`, `RowFilter`, and `MappingSpec.row_filter`
(4.1).

### 5.3 `engine/agents/reviewer.py`

* `apply_decisions(spec, decisions, target_dict=None, source_columns=None)`.
  The new optional `source_columns` is `{workset_name: {...}}`; see 5.4 for how
  it is built.
* Handle `decisions.get("__row_filter__")` **before** attribute decisions:
  * `set_filter` → `normalise_row_filter(filter, source_columns)`. Set
    `declared_at` (UTC ISO) and store it in `spec.row_filter`, **even if `errors`
    is non-empty**, so the reviewer sees the problem. An empty root → `None`.
  * `clear_filter` → `spec.row_filter = None`.
  * The attribute loop must skip keys starting with `__`.
* When `source_columns` is `None` (library or CLI callers), validate field names
  only at compile time in the validator.

### 5.4 `engine/orchestration/nodes.py`

* `apply_decisions_node`: build `source_columns` from `state["enriched"].columns`
  (`name`, `inferred_type`, `business_name`, `origin_table`, `value_decode`) and
  pass it in.
* No graph changes are needed: the certify flow already runs
  `apply_decisions → validation`.

### 5.5 `engine/agents/validator.py`

At the top of `validate_spec`, when `spec.row_filter` is set:

1. Compile against `wh.column_names(source_table)`.
   * On `RowFilterError`, or if `spec.row_filter.errors` is non-empty: add a
     hard-fail check `row_filter_valid` (category `wellformed`) with the reason.
     Then **continue validating on the unfiltered source** so the other
     findings still appear.
2. Otherwise:
   * `CREATE OR REPLACE VIEW "__workset_filtered" AS SELECT * FROM "<source_table>" WHERE <sql>`.
   * Count total and kept rows, and write them into `spec.row_filter.source_rows`
     and `kept_rows`. The validator already mutates the spec, e.g. gates.
   * Add check `row_filter_effect` (category `grain`), with `sql` set:
     * kept = 0 → **fail, hard**: "the row filter excludes every source row";
     * kept = total → **warn**: "the row filter excludes no rows";
     * otherwise **pass**: "kept K of N source rows; X excluded by the row filter: `<description>`".
   * Then set `source_table = "__workset_filtered"` for the rest of the function.

   Every existing check (compile probe, execute probe, materialisation,
   `row_count_preserved`, key, completeness, crossfield, value loss) now runs on
   the kept rows.
3. Update the `row_count_preserved` detail when filtered:
   "K source rows after the row filter (X excluded) -> T target rows."
4. Put `row_filter` (`description`, `source_rows`, `kept_rows`) into `report.stats`.

### 5.6 `api/server.py`

* `_run_mapping_manual` and `_run_mapping_manual_certify`: add `source_fields`
  to the `complete` SSE event, built from `merged["enriched"].columns`:
  `[{name, business_name, origin_table, inferred_type, type_family, value_decode, description}]`.
  The UI uses it for the field dropdown.
* New read-only endpoint `POST /api/mapping/filter-preview`, body
  `{spec, filter}`:
  * opens `Warehouse(WH_DB)`;
  * the relation is `spec.source_table` (`__workset` for multi-file, else the
    staged table); if it is missing, return
    `{"error": "Run the mapping first."}`;
  * normalises the filter with the source fields (rebuild them the same way:
    reuse a small helper rather than duplicating it), then compiles;
  * on error: `{"errors": [...]}`;
  * otherwise:
    `{source_rows, kept_rows, excluded_rows, description, compiled_sql, sample_excluded: [≤5 rows of the referenced fields]}`;
  * close the warehouse in `finally`.
* New read-only endpoint `POST /api/mapping/field-values`, body
  `{spec, field}` → the top 50 distinct trimmed values with counts, so the UI can
  offer a value picker for fields without `value_decode`. Validate that `field`
  is a real column.
* The transform, validate and reconcile endpoints need no signature change:
  they read `spec.row_filter`.

### 5.7 `api/transform.py`

* Add `build_source_sql` (4.4) and a private `_columns_of_workset(spec, paths)`.
  Reuse `_csv_columns` and the same collision renaming as `build_workset_sql`,
  or derive the columns by `LIMIT 0` on the workset SQL.
* `build_full_sql`: use `build_source_sql`.
* `run_transform`: add to `stats`:
  * `source_rows_total` (unfiltered workset count);
  * `rows_excluded_by_filter`;
  * `row_filter` (the description, or `None`).
* `generate_python`:
  * For both the single-file and joined paths, when a filter is set:
    * add `WHERE <compiled>` to `transform_sql` after `FROM {src_rel}`, inside the raw literal;
    * add a header comment block:
      `# Row filter (certified by reviewer): <description>`.
  * Without a filter, output must be **byte-identical** to today.

### 5.8 `api/validate.py`

* Row count (lines ~176-196):
  * `src_n` = count of `build_source_sql`;
  * also compute `src_total` from `build_workset_sql`;
  * the detail mentions the filter when one is set;
  * the recorded `sql` shows the filtered query.
* Re-execution (`_srcn`, line ~387): build it from `build_source_sql`. The ordinal
  then aligns with the transform's output.
* New check `row_filter_excluded_absent` (category `grain`, hard), only when a
  filter is set **and** a business key is mapped 1:1 (`key_tgt` and its source
  column known):
  * no delivered key value may belong to an excluded source row;
  * SQL: excluded keys = workset rows `WHERE NOT (<cond>)`, compared on trimmed
    key against `target_out`;
  * report up to 10 offenders;
  * if no key is mapped, add a `skipped` row with the reason.
* `describe_validation_rules`: the grain rule text mentions the filter when one is set.
* `generate_validation_script`: it does not read the source today, so no
  functional change. Add the filter description to the docstring header so the
  artefact records it.

### 5.9 `api/reconcile.py`

* `run_reconciliation`:
  * every source query that means "the source population" uses
    `build_source_sql`: row count, distinct keys (~277), numeric sums (~314),
    category profiles (~403, ~468);
  * `control_total:rows`: the sample table gets rows *Source rows (all files,
    before filter)*, *Excluded by row filter*, *Source rows in scope*,
    *Delivered*;
  * pass when in-scope rows = delivered.
* New control `control_total:row_filter` (technical, soft):
  * total = in-scope + excluded;
  * the detail carries the plain-English filter;
  * it is emitted only when the filter is set **and** it is in the certified rule set.
* `generate_reconciliation_script`: today it reads only the **primary file**
  (`FROM {primary}`), which is wrong once a filter can reference joined fields.
  When `spec.row_filter` is set:
  * build a `source` view in the script from the readable workset SQL. Factor the
    "rewrite `read_csv_auto(?)` to table names" logic out of
    `transform.generate_python` into a shared helper `readable_workset_sql(...)`
    and use it in both;
  * add the `WHERE`;
  * use `source` instead of `{primary}` in every source query;
  * add `control_total:row_filter`.

  Without a filter, output stays byte-identical. Check names must match the
  runner exactly (an existing test asserts this).

### 5.10 `api/recon_rules.py`

* `derive_candidates`:
  * when `spec.row_filter` is set, title `control_total:rows` as "Delivered
    row count equals the source rows kept by the row filter";
  * add the `control_total:row_filter` candidate
    (`params: {"description": ...}`, origin `mined`).
* Add `control_total:row_filter` to `KINDS` / `TECHNICAL_KINDS` as the existing
  pattern requires.

### 5.11 `web/static/index.html` and `web/static/app.js`

All new helpers go **inside the same IIFE** as the review-screen code.
`tests/test_frontend_scope.py` fails otherwise.

**Panel.** Add `<div class="panel" id="row-filter-panel" hidden>` in
`#review-section`, above `#review-list`. Title: *"Source row filter
(optional)"*. Sub-text: *"Only rows that match are migrated. Leave empty to
migrate every row."*

**Builder.** A list of condition rows. Each row has:

* **Field** `<select>`: every entry of `state.sourceFields`, shown as
  `ENDDTE — Role End Date (CO040)`. Grouped by `origin_table` with
  `<optgroup>`. Searchable if trivial; otherwise plain.
* **Operator** `<select>`: only the operators valid for the field's
  `type_family` (mirror the table in 4.2; keep the list in one JS constant).
* **Value input**, depending on the operator:
  * none for `is_empty` / `is_not_empty`;
  * for `in` / `not_in`: a multi-select built from `value_decode`
    (`20 — In Force`) if present; otherwise tag-style input, with a
    **"pick from data"** button calling `/api/mapping/field-values`;
  * `between`: two inputs;
  * dates: `<input type="date">`;
  * numbers: `<input inputmode="decimal">`;
  * text with `value_decode`: `<select>` of codes.
* Remove (✕) button.

Between rows, an **AND / OR** toggle that applies to the group. Also:

* **"+ Add condition"**;
* **"+ Add group"**, which adds a bracketed sub-group with its own AND/OR, one
  level only;
* a live **plain-English sentence** of the whole filter;
* a **"Check filter"** button: calls `/api/mapping/filter-preview` and shows
  "Keeps 158 of 199 rows (41 excluded)" plus up to 5 excluded sample rows, or
  the errors in red;
* **"Clear filter"**.

**State.**

* `state.sourceFields` is set from the SSE `complete` event (`ev.source_fields`),
  next to where `state.finalSpec` is set (~line 587).
* `state.filterDraft` is the builder content.
* `state.filterDirty` is true when the draft differs from `state.finalSpec.row_filter`.
* On certify (`run(state.decisions, true)`, ~line 961): if `filterDirty`, add
  `decisions.__row_filter__` (`set_filter`, or `clear_filter` when empty).
  `renderReview()` resets `state.decisions = {}`, so keep the filter draft in
  its own state, not in `state.decisions`.
* After certify, re-initialise the draft from `state.finalSpec.row_filter`.
  Server errors in `row_filter.errors` are shown in the panel.
* The panel is **not** a review item: it must not change `needsDecision`, the
  "N to resolve" count, or "Accept all suggested". A filter-only change during
  **Amend** must still enable Certify.
* When the review is locked (`state.reviewLocked`), the builder is read-only.

**Final mapping view** (`renderFinalMapping`): if `spec.row_filter`, show a line
above the table:
*"Row filter: `<description>` — keeps K of N source rows."*
**Downloads:**

* `buildSQL`: add `WHERE <compiled_sql>` and a comment with the description;
* `buildMD`: add a "Row filter" section;
* JSON already carries `row_filter`.

**Transformation console** (~1645): when `s.rows_excluded_by_filter`, add a
line: *"row filter: S source rows, X excluded, K transformed"*.

Reuse existing CSS classes (`panel`, `btn-sm`, `chip`, `mono`, `muted`,
`addrule-row` style). Add minimal new CSS to `styles.css` only if needed.

### 5.12 `README.md`

Add a short subsection under the Mapping workspace, "Row filter", explaining:

* what it is;
* that it is structured, not SQL;
* where it is applied;
* how it is reported in Validation and Reconciliation.

---

## 6. Edge cases you must handle

| Case | Expected |
|---|---|
| Field from a joined file, row with no join match | the joined field is NULL → `is_empty` is true, `eq` is false |
| `ne` / `not_in` on an empty value | counts as "not equal" (documented in the UI hint) |
| Date value `00000000` or blank | treated as empty; `gt`/`lt`/`between` are false for it (`TRY_STRPTIME` → NULL) |
| Non-numeric text in a number field | `TRY_CAST` → NULL → comparisons false |
| Value containing `'` or `--` or `;` | safely quoted text literal; never breaks SQL |
| Field name with spaces, unicode, quotes (see the `tests/fixtures/CLAIMS99` hostile fixture) | correct identifier quoting everywhere, including generated scripts |
| Client sends `compiled_sql` or a tampered `field_type` | ignored: the server recomputes `field_type` from the dictionary when available, and every consumer recompiles from the structure |
| Spec posted to Transformation without dictionary context | `compile_row_filter` uses the condition's stored `field_type`; an unknown field is a hard error |
| Filter keeps 0 rows | certify blocked (`row_filter_effect` hard fail) |
| Filter keeps all rows | warning only |
| Re-running the mapping (`/api/mapping/run`) | a fresh run starts without a filter (consistent with "a fresh run supersedes anything certified earlier") |
| Old spec JSON without `row_filter` | loads; no behaviour change |
| Window/aggregate transformations (future GROUP BY work, `docs/groupby_partition_plan.md`) | the filter applies **before** them. Note this in a code comment where `build_source_sql` is defined. |

---

## 7. Tests to write

First install pytest into the venv. It is not installed, and `requirements.txt`
has it commented out:
`.venv\Scripts\python.exe -m pip install pytest`

### 7.1 Fixtures

Create `tests/fixtures/row_filter/` with copies of
`data/dummy_data/input_files/CO040.csv`, `PO010.csv`, `dict_CO040.json`,
`dict_PO010.json` and `policy_client_role.json`. Do **not** modify the originals.

### 7.2 `tests/test_row_filter.py`

**Unit tests for `engine/row_filter.py`:**

* each operator × each family compiles to the SQL in 4.2;
* invalid op for a family, wrong arity, bad number, bad date, unknown field,
  more than 20 conditions → `RowFilterError`;
* quote-injection value `O'Brien'; DROP TABLE x; --` compiles to a single safe
  literal, and executing it in DuckDB returns rows without error;
* `describe_row_filter` produces stable, readable text;
* nested group precedence: `A AND (B OR C)` ≠ `(A AND B) OR C`.

**Reviewer:**

* `set_filter` / `clear_filter` / absent key;
* `field_type` is filled from `source_columns` and overrides a client value;
* errors are stored, not dropped;
* attribute decisions still apply.

**End-to-end through the API** (FastAPI `TestClient`, like
`data/dummy_data/input_files/groupby_scenario/run_groupby_scenarios.py`; point
`TEMP`/`TMP` to a temp dir before importing `api.server`). Use the fixtures,
run the mapping, then certify with "accept all suggested" decisions plus the
filter, then transform, validate, reconcile.

Expected kept-row counts (computed on this data, workset primary = CO040,
199 rows):

| Filter | Kept |
|---|---|
| none | 199 |
| `ENDDTE is_empty` | **158** |
| `ENDDTE is_empty AND STAXRF eq 30` | **152** |
| `STAPOL in [20, 25] AND (ROLREF eq PH OR ROLREF eq PY)` (STAPOL comes from the joined PO010) | **17** |
| `EXTMOD eq 0` (policy not exited; joined field) | **86** |
| `CTBAMT is_empty` | **173** |
| `STRDTE ge 2020-01-01` (date) | **38** |
| `CTBAMT gt 100` (number) | **5** |
| `UNIQUE_ID eq "does-not-exist"` | 0 → certify **blocked** |

For each non-zero case assert:

* certify verdict is `certified`, if the baseline without a filter certifies.
  First check what the baseline verdict is with "accept all suggested", and
  compare like with like;
* transform `row_count` = kept;
* validation verdict equals the baseline verdict, with `row_count_preserved`
  passing and `row_filter_excluded_absent` passing;
* reconciliation `control_total:rows` passes and `control_total:row_filter`
  reports total = kept + excluded;
* the generated ETL script contains the `WHERE` clause and the description
  comment.

**Byte-identical regression:** with no filter, `generate_python`,
`generate_validation_script` and `generate_reconciliation_script` output equals
the output before your change. Capture it at the start of the task, before
editing, into a temp file and compare.

**Determinism:** generating each script 5 times with a filter is byte-identical.

**Script reproduces app:** run the generated reconciliation script against the
transformed CSV and the source files in a temp dir. It must report the same check
names and the same row-count result as `run_reconciliation`. Skip only if a
dependency is missing, and say so.

### 7.3 Existing suites

`.venv\Scripts\python.exe -m pytest -q` must pass (the README says 98 tests, 4
skipped without a live LLM). Report any test that was already failing **before**
your change separately. Run the suite once at the start to know the baseline.

---

## 8. Verification you must perform and report

1. Run the baseline test suite **before** editing, and record the result.
2. Implement, then run the full suite plus the new tests. Paste the summary lines.
3. Start the server
   (`.venv\Scripts\python.exe -m uvicorn api.server:app --port 8000`) and
   exercise the endpoints with the fixtures via `TestClient` or HTTP:
   * run;
   * certify with the filter `ENDDTE is_empty AND (ROLREF eq PH OR ROLREF eq PY)`;
   * transform;
   * validate;
   * reconcile.

   Report the kept count and verdicts.
4. Confirm the original files in `data/dummy_data/input_files/` are unchanged
   (`Get-FileHash` before and after).
5. You cannot click through the browser yourself. Say so, and list the manual UI
   steps the user should try:
   * build the filter;
   * Check filter;
   * Certify;
   * Amend → change the filter → re-certify;
   * the final mapping line;
   * downloads;
   * the Transformation console line.

---

## 9. Do not

* Do not let the client send executable SQL, or execute `compiled_sql` from a spec.
* Do not use the LLM anywhere in this feature.
* Do not change the mapping agent (`engine/agents/mapping_agent.py`), its scoring
  or its gates.
* Do not change behaviour or generated bytes when no filter is set.
* Do not swallow errors: unrecoverable means a hard check, otherwise a `skipped`
  or `warn` row with the reason.
* Do not modify the original dummy data files or `engine/agents/merge/`.
* Do not add new third-party dependencies (pytest for running tests is fine).

## 10. Definition of done

* A reviewer can build, preview, certify, amend and clear a multi-condition
  AND/OR row filter in the review step.
* Transformation outputs only the kept rows.
* Validation and Reconciliation compare against the kept rows and report the
  excluded count.
* Generated scripts apply the same filter and reproduce the app's results.
* All new and existing tests pass. No-filter output is byte-identical to before.
* Report back with:
  * the files changed (as links);
  * the test output;
  * the verification results from section 8;
  * the manual UI checklist;
  * any deviation from this prompt, and why.
