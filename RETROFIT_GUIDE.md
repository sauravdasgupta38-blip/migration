# Retrofit guide: `migration_backup` → `migration_code`

Compared on 2026-11-12. Both trees hold one project, `datamap_v11_share/`; every path below is relative to that folder.

- **Baseline (old):** `migration_backup/datamap_v11_share`
- **Changed (new):** `migration_code/datamap_v11_share`
- **Ignored, as requested:** everything under `data/` (the new tree adds `data/dummy_data/` and `data/warehouse.duckdb`).
- **Also ignored:** `__pycache__/`, and `.venv/` (present only in the new tree; it was built on another machine and does not run here, so do not copy it).

A machine-applicable version of everything in this guide is in `retrofit_backup_to_code.patch`, next to this file (it excludes `data/`, `.env`, `.venv/` and caches).

---

## 1. Verdict on the two stated requirements

| Requirement | Present? | Notes |
|---|---|---|
| 1. Row filtration in the mapping stage | **Yes, end to end** | Structured filter built in the review step, compiled server-side, applied in certify, transform, validation, reconciliation and all three generated scripts. |
| 2. GROUP BY / PARTITION BY transformations | **Partly** | PARTITION BY (window columns) works end to end from the UI. GROUP BY (a declared "target grain") is fully implemented in the backend, but **the UI has no control to set it** — see 1.1. |

### 1.1 Gap: GROUP BY cannot be switched on from the screen

- The backend accepts a `"__grain__": {"action": "set_grain", "group_by": [...]}` decision at certify (`engine/agents/reviewer.py`), and every workspace honours `spec.grain`.
- `web/static/app.js` never sends that decision. Its grain section is commented "read-only in the UI": it only reads the grain already on the spec.
- `web/static/index.html` has no grain panel, and nothing in the UI calls the new `/api/mapping/grain-preview` endpoint.
- `README.md` and `docs/groupby_partition_plan.md` both describe a "Target grain panel with a preview" on the review screen. That panel does not exist in the code.
- Today a grain can only be set by posting the decision directly to the certify API (this is what `tests/test_groupby.py` and `data/dummy_data/.../run_groupby_scenarios.py` do).

If GROUP BY must be usable by a reviewer, a grain panel still has to be built; it is not something to retrofit from the new code.

### 1.2 Not verified by running

The tests were not run: the system Python has no `duckdb` installed and the bundled `.venv` points at a Python on another machine. The findings here come from reading the code.

---

## 2. Changes beyond the two requirements

These are in the new code but are not "row filter" or "group by / partition by" as such. Several change behaviour even when no filter, grain or window column is used.

| # | Change | Where | Affects existing behaviour? |
|---|---|---|---|
| 1 | **One statement builder.** All workspaces now build the target query through `engine/etl_sql.build_target_sql`. At source grain it numbers source rows and adds `ORDER BY "__src_ord"`. | `engine/etl_sql.py`, `api/transform.py`, `api/validate.py`, `api/reconcile.py` | Yes: the generated ETL SQL text differs from the backup for every spec. |
| 2 | **Spec safety guard.** `assert_spec_safe` rejects sub-queries, qualified references and illegal expression kinds before anything runs. | `engine/etl_sql.py`, called from `api/transform.py`, `api/reconcile.py` | Yes: a previously certified mapping that uses a sub-query now fails with a `ValueError`. |
| 3 | **Transform runs with file access off.** Source rows are loaded into a temp table, then `SET enable_external_access = false` before reviewer SQL runs. | `api/transform.py` `run_transform` | Yes (hardening). |
| 4 | **Validation re-execution redesigned.** The expected target is built once and compared; rows align by group key, then business key, then position. Previously each column was re-run alone and aligned by position only. | `api/validate.py` | Yes: the SQL shown on `transform_rule:*` checks and the mismatch samples change. |
| 5 | **Reconciliation reads a rebuilt `_expected` table.** Numeric sums, category profiles and `aggregate_by` rules now read it; columns that cannot be recomputed are reported as skipped, not dropped; `control_total:numeric_sums` can now have status `skipped`. | `api/reconcile.py` | Yes. |
| 6 | **Generated reconciliation script restructured.** It always creates `source_all`, `source` and `expected`, and its numeric-sum and aggregate sections were rewritten. | `api/reconcile.py` `generate_reconciliation_script` | Yes: script text differs for every spec. New optional `source_paths` parameter. |
| 7 | **Certify probe reshaped.** `SELECT count(<expr>)` became `SELECT count(v) FROM (SELECT (<expr>) AS v ...)`, and each expression is classified first. | `engine/agents/validator.py` | Yes: window functions no longer fail certify. |
| 8 | **Duplicate review card fix.** An unmapped target no longer produces two cards (placeholder mapping plus unmapped item). | `engine/agents/reviewer.py` `build_review_queue` loop | Yes (bug fix). |
| 9 | **"Write SQL" on unmapped targets.** New `write_sql` action and button, plus the shared SQL editor. | `reviewer.py`, `app.js` | New capability. |
| 10 | **"Test" button in every SQL editor**, backed by `/api/mapping/preview-expression`. | `app.js`, `api/server.py` | New capability. |
| 11 | **"Override" button on auto-accepted mappings** in the final mapping table. | `app.js` `renderFinalMapping`, `overrideMapping` | New capability. |
| 12 | **"Accept suggested" no longer overwrites** decisions the reviewer already made. | `app.js` `#accept-suggested` handler | Yes (bug fix). |
| 13 | **Source columns derived from SQL.** A reviewer edit with no source named records the columns the SQL actually reads. | `reviewer.py` `_columns_of`, `apply_decisions` | Yes. |
| 14 | **`accept_suggestion` decision** and `ANY_VALUE(...)` rewrite suggestions. | `contracts.py`, `reviewer.py`, `validator.py` | New (grain-related). |
| 15 | **Codegen endpoints return `{"error": ...}`** on `ValueError` instead of a 500. | `api/server.py` transform and reconcile codegen | Yes. |
| 16 | **`.env`: `OPENAI_API_KEY` value differs.** This is the only configuration difference. | `.env` line 6 | Decide which key to keep; do not copy blindly. |
| 17 | Documentation: two README sections and two new docs. | `README.md`, `docs/` | No. |

`requirements.txt`, `.gitignore`, `cli/`, `engine/orchestration/graph.py`, `state.py`, the other `engine/` modules and the existing tests (`test_smoke.py`, `test_frontend_scope.py`) are **identical** in both trees. No new third-party dependency is needed.

---

## 3. File inventory

### 3.1 New files (copy as they are)

| File | Lines | Purpose | Requirement |
|---|---|---|---|
| `engine/row_filter.py` | 327 | Filter operators, validation, `compile_row_filter`, `describe_row_filter`, `normalise_row_filter`, `type_family`, `is_active`, `RowFilterError` | 1 |
| `engine/etl_sql.py` | 365 | `build_target_sql`, `analyse_expression`, `classify_expression`, `grain_keys`, `conservation_function`, `group_key_attributes`, `assert_spec_safe`, constants `WORKSET`, `ORD` | 2 |
| `tests/test_row_filter.py` | 513 | Unit and API tests for the filter | 1 |
| `tests/test_groupby.py` | 552 | Unit and API tests for windows and grain | 2 |
| `tests/fixtures/row_filter/` | 5 files | `CO040.csv`, `PO010.csv`, `dict_CO040.json`, `dict_PO010.json`, `policy_client_role.json` | 1 |
| `tests/fixtures/groupby/` | 8 files | Same five plus `policy_summary.json`, `expected_scenario_a.csv`, `expected_scenario_b.csv` | 2 |
| `docs/row_filter_change_prompt.md` | 683 | Design notes for the filter | 1 |
| `docs/groupby_partition_plan.md` | 332 | Design notes for grain and windows (describes a grain panel that was not built) | 2 |

### 3.2 Modified files

| File | Added | Removed | Requirement |
|---|---|---|---|
| `engine/agents/contracts.py` | 68 | 1 | 1 + 2 |
| `engine/agents/reviewer.py` | 116 | 7 | 1 + 2 + extras 8, 9, 13, 14 |
| `engine/agents/validator.py` | 185 | 16 | 1 + 2 + extra 7 |
| `engine/orchestration/nodes.py` | 21 | 1 | 1 |
| `api/transform.py` | 178 | 33 | 1 + 2 + extras 1, 2, 3 |
| `api/validate.py` | 250 | 61 | 1 + 2 + extra 4 |
| `api/recon_rules.py` | 49 | 0 | 1 + 2 |
| `api/reconcile.py` | 300 | 51 | 1 + 2 + extras 5, 6 |
| `api/server.py` | 263 | 6 | 1 + 2 + extra 15 |
| `web/static/app.js` | 404 | 14 | 1 + 2 + extras 9 to 12 |
| `web/static/index.html` | 19 | 0 | 1 |
| `web/static/styles.css` | 34 | 0 | 1 + 2 |
| `README.md` | 55 | 0 | docs |
| `.env` | 1 | 1 | config (key value only) |

---

## 4. How to retrofit

**If the target copy is the untouched backup:** every modified file can simply be replaced by the new version, and the new files copied in. Nothing in the backup would be lost, because the new code is the backup plus these changes. Leave `.env` alone unless the new key is wanted.

**If the target copy has drifted:** apply the hunks in section 5 by hand, in the order below. The order follows the import dependencies, so the project stays importable after each step.

1. Copy `engine/row_filter.py` and `engine/etl_sql.py`.
2. `engine/agents/contracts.py`
3. `engine/agents/reviewer.py`
4. `engine/agents/validator.py`
5. `engine/orchestration/nodes.py`
6. `api/transform.py`
7. `api/validate.py`, `api/recon_rules.py`, `api/reconcile.py`
8. `api/server.py`
9. `web/static/index.html`, `styles.css`, `app.js`
10. Tests, fixtures, docs, README.

The two features are **not separable file by file**. `api/transform.py`, `api/validate.py`, `api/reconcile.py`, `validator.py`, `reviewer.py`, `contracts.py`, `server.py` and `app.js` each carry both. Taking only the row filter would mean hand-picking hunks marked "(filter)" below and keeping the old statement-building code.

To apply the patch file instead, from inside the backup's parent folder:

```bash
git apply -p1 --directory=migration_backup retrofit_backup_to_code.patch
```

---

## 5. Hunk-by-hunk detail for modified files

Line numbers are "old line → new line" at the start of each hunk. "(filter)" = requirement 1, "(grain)" = requirement 2, "(extra N)" = row N of section 2.

### 5.1 `engine/agents/contracts.py`

| Lines | Change |
|---|---|
| 13 → 13 | `from typing import Optional` becomes `from typing import Optional, Union`. |
| 212 → 215 | (grain) `MappingEntry` gains `transform_kind: str = "row"`, `partition_by: list[str]`, `suggested_sql: str = ""`. |
| 215 → 226 | (filter) New models `FilterCondition`, `FilterGroup` (recursive), `RowFilter`. (grain) New model `GrainSpec`. All inserted before `class MappingSpec`. |
| 229 → 287 | `MappingSpec` gains `row_filter: Optional[RowFilter] = None` and `grain: Optional[GrainSpec] = None`. Directly after the class: `FilterGroup.model_rebuild()`, `RowFilter.model_rebuild()`, `MappingSpec.model_rebuild()`. |
| 304 → 371 | (grain) `ReviewItem` gains `transform_kind: str = "row"` and `partition_by: list[str]`. |

### 5.2 `engine/agents/reviewer.py`

| Lines | Function | Change |
|---|---|---|
| 135 → 138 | `build_review_queue` | (extra 8) Build `unmapped_names`; at the top of the `for m in spec.mappings` loop, `continue` for a placeholder that is in `unmapped_names` and has no source and no SQL. |
| 161 → 172 | same | (grain) If no code-fix suggestion exists and `m.suggested_sql` is set, use it as `sugg_sql` and set the "wrap it" text. |
| 183 → 199 | same | (grain) Pass `transform_kind=m.transform_kind, partition_by=m.partition_by` into `ReviewItem(...)`. |
| 192 → 209 | same | (extra 9) Unmapped item `actions` becomes `["write_sql", "set_default", "leave_null"]`. |
| 206 → 223 | module level | (filter) New `ROW_FILTER_KEY = "__row_filter__"` and `_apply_row_filter`. (grain) New `GRAIN_KEY = "__grain__"` and `_apply_grain`. (extra 13) New `_columns_of`. All inserted before `apply_decisions`. |
| 209 → 296 | `apply_decisions` | New fourth parameter `source_columns: dict[str, dict] | None = None`. At the top of the body: apply `__row_filter__`, then `__grain__`, then strip every key starting with `__` from `decisions`. Docstring extended. |
| 231 → 333 | same | (extra 14) New branch `elif action == "accept_suggestion" and m.suggested_sql:` swaps in the suggested SQL. |
| 234 → 339 | same, `edit` branch | (extra 13) SQL is set first; if no `source_attributes` were sent, they are taken from `_columns_of(sql)`; `m.suggested_sql = ""`. |
| 289 → 398 | same, unmapped edit | `srcs = list(d.get("source_attributes") or []) or _columns_of(sql)`. |

### 5.3 `engine/agents/validator.py`

| Lines | Change |
|---|---|
| 24 → 27 | New import `from ..etl_sql import classify_expression, q`. |
| 70 → 74 | (filter) New "row filter" block before the wellformed checks: compiles the filter, creates view `__workset_filtered`, sets `rf.source_rows` / `rf.kept_rows`, adds check `row_filter_valid` (hard fail) or `row_filter_effect` (fail when nothing kept, warn when nothing excluded), then repoints `source_table` at the view and remembers the original in `filtered_from`. |
| 90 → 136 | (grain) New "declared grain" block after `live = [...]`: adds check `grain_declared_valid`, sets `keys`, `rel`, `gb`. |
| 97 → 166 | (extra 7) The `LIMIT 0` compile probe is replaced by `classify_expression(...)`; it stores `transform_kind`, `partition_by`, `suggested_sql` on the mapping, sends bind errors to `no_compile`, and adds a `transform_kind:<attr>` finding for illegal kinds (soft for `I-EXPR-ANY-VALUE`, hard otherwise). |
| 115 → 195 | (extra 7) Execute probe reshaped to `SELECT count(v) FROM (SELECT (<expr>) AS v ...)`, with the `GROUP BY` variant when a grain is set. |
| 136 → 220 | (grain) Materialisation: grouped variant projects the group keys as `__gk_<i>`; the ungrouped branch is the old code. |
| 148 → 239 | Grain check: (filter) detail text gains "after the row filter (N excluded)"; (grain) new checks `grain_row_count`, `grain_key_mapped`, and a group-key form of `key_unique_not_null`; the old `row_count_preserved` moves into the `else`. |
| 161 → 278 | (grain) `one_to_one` only accepts mappings whose `transform_kind` is `row` or `group_key`. |
| 171 → 296 | (grain) `if not candidates:` becomes `if keys: candidates = []` / `elif not candidates:`. |
| 193 → 320 | (grain) Final `else:` of the key block becomes `elif not keys:`. |
| 217 → 344 | (grain) Crossfield loop iterates over `[] if keys else insight.dependencies`. |
| 240 → 369 | (grain) Value-loss check: window and aggregate columns pass with an explanatory note; under a grain the check is done per group through the `__gk_<i>` keys. |
| 298 → 461 | Stats gain `grain`, `column_kinds`, and (filter) `row_filter` with description and row counts. |

### 5.4 `engine/orchestration/nodes.py`

| Lines | Change |
|---|---|
| 88 → 88 | (filter) `apply_decisions(...)` call gains a fourth argument `source_columns(state.get("enriched"))`. |
| after 94 → 95 | (filter) New function `source_columns(enriched)` returning `{column: {name, business_name, description, inferred_type, origin_table, value_decode}}`. |

### 5.5 `api/transform.py`

| Lines | Function | Change |
|---|---|---|
| 122 → 125 | new | (filter) `row_filter_active`, `_relation_columns`, `compile_spec_filter`, `build_source_sql` inserted after `build_workset_sql`. |
| 169 → 213 | `build_full_sql` | (extra 1, 2) Rewritten: new `extra=()` parameter; calls `assert_spec_safe`, `build_source_sql`, and returns `build_target_sql(...)`. |
| after it → 232 | new | (grain) `column_kinds(spec)`. |
| 211 → 275 | `run_transform` | (extra 1, 2, 3) Loads the in-scope rows into temp table `__source`, counts them, disables external access, then builds and runs the target statement over `__source`. |
| 245 → 319 | same | Stats gain `row_filter`, `grain`, `source_rows`, `target_rows`, `window_columns`, `aggregate_columns`; (filter) plus `source_rows_total` and `rows_excluded_by_filter` when a filter is active. |
| 252 → 345 | new | `readable_workset_sql` (code moved out of `generate_python`) and `_script_columns`. |
| 262 → 400 | `generate_python` | Calls `assert_spec_safe(spec)` first. |
| 301 → 442 | same | The inline workset-rewrite block is replaced by a call to `readable_workset_sql`. |
| 329 → 453 | same | (filter) Emits the "Row filter" comment lines and builds `where`. (grain) Emits "Target grain" and "Column kinds" comment lines. |
| 336 → 480 | same | `body` is now `build_target_sql(select, f"SELECT * FROM {src_rel}{where}", keys) + "\n"`. |

### 5.6 `api/validate.py`

| Lines | Function | Change |
|---|---|---|
| 42 → 42 | imports | Adds `from engine.etl_sql import ORD, analyse_expression, classify_expression, grain_keys`; the `.transform` import now also brings `build_full_sql`, `build_source_sql`, `compile_spec_filter`, `row_filter_active`. |
| 74 → 77 | `_one_to_one_map` | (grain) Extra condition: kind must be `row` or `group_key`. New helpers `_kind_of` and `_group_key_attrs` after it. |
| 174 → 195 | `run_validation`, grain | (filter) Source count comes from `build_source_sql`; `src_total` is the unfiltered count; detail and SQL text mention the filter. (grain) New `grain_row_count` check when a grain is set. |
| 199 → 248 | same, key integrity | (grain) New group-key branch of `key_unique_not_null`; new `key_ok` flag set by both branches. |
| 219 → 289 | same | (filter) New check `row_filter_excluded_absent`: no record the filter excluded may appear in the delivered file, matched on the business key; skipped under a grain or when no key is mapped 1:1. |
| 374 → 491 | same, transform conformance | (extra 4) `_srcn` is replaced by `_expected`, built from `build_full_sql(..., extra=[(ORD, ...)])`. New alignment choice (`group key` / `business key` / `position`), new `missing_group` and `unexpected_group` checks, new comparison SQL, new mismatch sample query. |
| 485 → 621 | same | (grain) New `partition_consistency:<attr>` check for each window column. |
| 561 → 735 | rule descriptions | (filter) Adds the filter sentence and a `row_filter_excluded_absent` rule entry. |
| 632 → 816 | validation script generator | (filter) Emits two "Row filter" comment lines after the docstring. |

### 5.7 `api/recon_rules.py`

| Lines | Change |
|---|---|
| 60 → 60 | `KINDS` gains `"control_total:row_filter"` (filter) and `"control_total:groups"`, `"control_total:keys"`, `"control_total:conservation"` (grain). |
| 149 → 157 | After the base `rules = [...]` list: (filter) retitle `rules[0]`, record the filter description, insert a `control_total:row_filter` rule; (grain) replace `rules[0]` with `control_total:groups`, insert `control_total:keys`, and append one `control_total:conservation` rule per SUM/COUNT aggregate column. |

### 5.8 `api/reconcile.py`

| Lines | Function | Change |
|---|---|---|
| 61 → 61 | imports | Adds `build_target_sql`, `conservation_function`, `grain_keys`, `group_key_attributes` from `engine.etl_sql`, and `build_full_sql`, `build_select`, `build_source_sql`, `readable_workset_sql`, `row_filter_active`, `_script_columns`, `compile_spec_filter` from `.transform`. |
| 162 → 167 | `run_reconciliation` | (filter) Workset comes from `build_source_sql`; `src_total` computed. (extra 5) Builds table `_expected` once from `build_full_sql`; sets `expected_ok`, `expected_err`, `exp_cols`. |
| 185 → 216 | same, `control_total:rows` | (filter) Three-line sample (all / excluded / in scope) and filter wording. New `control_total:row_filter` check. (grain) New `control_total:groups`, `control_total:keys`, and `control_total:conservation:<attr>` checks. |
| 302 → 436 | same, numeric sums | (extra 5) Expected totals read from `_expected`; uncomputable columns collected in `skipped` and reported; status can be `skipped`. |
| 397 → 540 | same, category profiles | (extra 5) Reads `_expected`; skips with a reason when it could not be built. |
| 462 → 609 | same, `aggregate_by` | (extra 5) Source side reads `_expected` when the columns exist there, otherwise the old workset query. |
| 598 → 753 | rule descriptions | (filter) Adds the filter sentence to the `control_total` family. |
| 621 → 782 | `generate_reconciliation_script` | (extra 6) New parameter `source_paths`. |
| 674 → 836 | same | (extra 6) Emits `source_all`, `source` (with the filter `WHERE`) and `expected`; `primary` becomes `"source"`. |
| 685 → 891 | same | Emits group, key, conservation and row-filter checks according to the enabled rule kinds. |
| 710 → 953 | same | (extra 6) Numeric-sum section rewritten to read `expected`. |
| 756 → 1008 | same | (extra 6) Aggregate section: `sql_of`, `agg_s`, `ks` removed; source side reads `expected`. |

### 5.9 `api/server.py`

| Lines | Change |
|---|---|
| 307 → 307 | (filter) The mapping run's `complete` event gains `"source_fields": _remember_source_fields(merged.get("enriched"))`. |
| 313 → 315 | (filter) New module-level cache `_SOURCE_FIELDS` and helpers `_inputs_signature`, `_field_list`, `_remember_source_fields`, `_current_source_columns`. |
| 364 → 425 | (filter) The certify run's `complete` event gains the same `source_fields` entry. |
| 392 → 454 | New helpers and endpoints, inserted before the "Tab 4 · Transformation workspace" banner: `_staged_relation`; (filter) `POST /api/mapping/filter-preview`; `_scoped_relation`; (grain) `POST /api/mapping/preview-expression`; (grain) `POST /api/mapping/grain-preview` (no UI caller); (filter) `POST /api/mapping/field-values`. |
| 437 → 687 | (extra 15) `T.generate_python(...)` wrapped in `try / except ValueError` returning `{"error": ...}`. |
| 707 → 960 | (extra 15) `R.generate_reconciliation_script(...)` wrapped the same way and now passed `source_paths=...`. |

### 5.10 `web/static/index.html`

| Lines | Change |
|---|---|
| 141 → 144 | (filter) New `<div class="rowfilter" id="row-filter-panel" hidden>` block, inserted immediately before `<div id="review-list" class="cards">`. Element ids used by the script: `row-filter-panel`, `rf-check`, `rf-clear`, `rf-builder`, `rf-sentence`, `rf-result`. |

### 5.11 `web/static/styles.css`

| Lines | Change |
|---|---|
| 830 → 833 | Appended at end of file: (filter) `.rowfilter`, `.rowfilter-head`, `.rf-*` rules; (grain / extras 10, 11) `.badge.kind`, `.sql-hint`, `.test-out`, `.test-tbl`, `.btn-sm.ovr`. |

### 5.12 `web/static/app.js`

| Lines | Location | Change |
|---|---|---|
| 585 → 588 | run-complete handler | (filter) Store `ev.source_fields`; call `rfInitFromSpec()` and `gInitFromSpec()`. |
| 699 → 706 | review lock function | (filter) Call `rfRender()` at the end. |
| 800 → 808 | `handleAct` | (extra 10) New branch `else if(act==="test") previewExpression(cardEl, attr);`. |
| 867 → 876 | review card renderer | The inline editor markup is replaced by `${sqlEditor(...)}`; the card title gains `${kindBadge(it.transform_kind, it.partition_by)}`. |
| 889 → 893 | new | `sqlEditor(prefill)`: shared editor with hint, Save, Test, Cancel and a `.test-out` area. |
| 913 → 935 | unmapped default editor | (extra 9) Adds the "✎ Write SQL" button and `${sqlEditor("")}`. |
| 949 → 973 | `#accept-suggested` | (extra 12) Only fills attributes that have no decision yet. |
| 958 → 984 | `#certify` | (filter) Sends `Object.assign({}, state.decisions, rfDecision())`. |
| 986 → 1014 | `renderFinalMapping` | (extra 11) Override button on auto-accepted rows; kind badge in the target cell. |
| 998 → 1030 | same | Summary line gains the target grain and the row filter text; Override buttons are wired. |
| 1001 → 1037 | new block | (filter) Whole row-filter builder: `RF_OPS`, `RF_OP`, `rfEmpty`, `rfBlankCond`, `rfField`, `rfFamily`, `rfNode`, `rfParent`, `rfCanon`, `rfCertifiedRoot`, `rfDirty`, `rfDecision`, `rfDescribe`, `rfFieldOptions`, `rfValueHTML`, `rfCondHTML`, `rfGroupHTML`, `rfSentence`, `rfShowCertified`, `rfRender`, `rfInitFromSpec`, `rfTouched`, `rfPost`, `rfPickValues`, `rfCheck`, `rfWire` and the `rfWire()` call. (grain) `gCertified`, `gLabel`, `gInitFromSpec`, `kindBadge`, `previewExpression`. (extra 11) `overrideMapping`. |
| 1009 → 1375 | `buildSQL` | Download SQL gains the row-filter comment, `WHERE <compiled_sql>` and `GROUP BY <keys>`. New helper `grainOf(spec)` after it. |
| 1023 → 1398 | `buildMD` | Download markdown gains "Row filter" and "Target grain" sections. |
| 1644 → 2026 | transform console | Extra console lines for the row filter, the grain and the window / aggregate column counts. |

`RF_OPS` in `app.js` mirrors `OPERATORS` in `engine/row_filter.py`; keep the two in step if either is edited.

### 5.13 `README.md`

| Lines | Change |
|---|---|
| 39 → 42 | Two new sections inserted before "### 2 · Transformation workspace": "#### Row filter" and "#### Group totals and target grain (PARTITION BY / GROUP BY)". The last paragraph of the second section mentions a Target grain panel that is not in the UI (see 1.1). |

### 5.14 `.env`

| Line | Change |
|---|---|
| 6 | `OPENAI_API_KEY` has a different value in each tree. No other setting differs. |

---

## 6. New API surface (for anyone integrating)

| Item | Detail |
|---|---|
| Decision key `__row_filter__` | `{"action": "set_filter", "filter": {"version": 1, "root": {...}}}` or `{"action": "clear_filter"}`; omitted = keep the current filter. |
| Decision key `__grain__` | `{"action": "set_grain", "group_by": [...], "rationale": "..."}` or `{"action": "clear_grain"}`. Not sent by the UI. |
| Decision action `accept_suggestion` | Applies a mapping's `suggested_sql`. |
| Spec fields | `MappingSpec.row_filter`, `MappingSpec.grain`; `MappingEntry.transform_kind`, `.partition_by`, `.suggested_sql`. |
| SSE `complete` event | New `source_fields` list. |
| Endpoints | `POST /api/mapping/filter-preview`, `/api/mapping/field-values`, `/api/mapping/preview-expression`, `/api/mapping/grain-preview`. |
| New check names | `row_filter_valid`, `row_filter_effect`, `row_filter_excluded_absent`, `grain_declared_valid`, `grain_row_count`, `grain_key_mapped`, `transform_kind:<attr>`, `missing_group`, `unexpected_group`, `partition_consistency:<attr>`, `control_total:row_filter`, `control_total:groups`, `control_total:keys`, `control_total:conservation:<attr>`. |

---

## 7. After retrofitting

1. Install the requirements and run `pytest tests` in the retrofitted tree; `test_row_filter.py` and `test_groupby.py` run offline (they blank the LLM keys).
2. Re-run `tests/test_smoke.py` as well: the changes in section 2 (rows 1 to 7) touch paths it covers.
3. Regenerate any ETL, validation or reconciliation scripts that were produced by the old code; their text is no longer identical.
