# GROUP BY and PARTITION BY transformations

## Part 1: Requirement

### 1.1 What we need

Some target fields cannot be filled from one source row. They need values from
**several rows added up or counted together**. The mapping agent will not work
these out by itself, so the **reviewer must be able to write them by hand** in
the review step of the Mapping workspace. After the mapping is certified, they
must work correctly in Transformation, Validation and Reconciliation.

There are two kinds of need.

**Type 1: a group total shown on every row (PARTITION BY).**
The target keeps one row per source row, but some fields show a total for the
group the row belongs to.

> Example: the target has one row per client role. Each row should show
> "number of active beneficiaries on this policy". All roles on policy 3104365
> show `2`.

**Type 2: one row per group (GROUP BY).**
The target has fewer rows than the source: one row for each group.

> Example: a `policy_summary` target with one row per policy, built from 199
> role rows, which gives 30 rows.

### 1.2 What must be true

1. **Nothing changes for normal mappings.** If no one uses these features, the
   application behaves exactly as it does today.
2. The reviewer can write a group total (Type 1) for **any** target field. That
   includes fields the agent left unmapped and fields the agent auto-accepted.
3. The reviewer can choose to make the target **one row per group** (Type 2) by
   picking the group column(s), for example `POLREF`. They can also undo it.
4. The system checks the reviewer's SQL **before** certifying it. If it is
   wrong, the message says what to do.
5. **What is certified is exactly what runs.** SQL that passes certification
   must not fail later in Transformation.
6. Output rows come out in a **fixed, predictable order**.
7. Validation must correctly check these fields and must not report false errors.
8. Reconciliation must include these fields in its totals. It must never
   silently skip a field.
9. The downloadable scripts (ETL, validation, reconciliation) must give the same
   results as the application.
10. The reviewer can **test** their SQL and preview the group counts before
    certifying.
11. Reviewer SQL can only use source columns. It cannot read files or other
    tables.

### 1.3 Why it doesn't work today

We tested this with the data in `data/dummy_data/input_files/groupby_scenario/`:

* **The certify check rejects correct group-total SQL.** It wraps the SQL in a
  `count(...)`, which DuckDB doesn't allow around a window function.
* **The SQL is built in four different places.** Each place names the source
  data differently, so SQL that passes in one place can fail in another.
* **Rows come out in a different order** when a group total is used.
  Validation matches rows by position, so every column then looks wrong.
* **Every check assumes one target row per source row**, so one row per group
  is impossible.
* **Reconciliation quietly drops** these fields from its totals.
* **The review screen has gaps.** It can't attach SQL to unmapped fields and
  can't change auto-accepted ones.

---

## Part 2: Technical solution

### 2.1 The idea in one line

Build the target SQL in **one place only**, and let two simple settings decide
how it is built.

### 2.2 Two settings

| Setting | Where it lives | Values | Who sets it | Default |
|---|---|---|---|---|
| **Grain** | the whole target table (`spec.grain`) | "one row per source row", or "one row per group of `<columns>`" | the reviewer, on purpose | one row per source row |
| **Column kind** | each target field (`transform_kind`) | `row`, `window`, `aggregate`, `group_key` | **the system**, by reading the SQL | `row` |

There is **no global switch**. If the grain is left alone and no one writes
aggregate SQL, the mapping stays exactly as it is today.

**Column kinds explained:**

| Kind | What it means | Example SQL |
|---|---|---|
| `row` | normal: value comes from the same row | `trim("CLIREF")` |
| `window` | group total shown on every row (Type 1) | `COUNT(*) FILTER (WHERE "ROLREF" = 'BN') OVER (PARTITION BY "POLREF")` |
| `aggregate` | a total for a group (Type 2) | `SUM(TRY_CAST("CTBAMT" AS DECIMAL(12,2)))` |
| `group_key` | the column that identifies the group (Type 2) | `trim("POLREF")` |

**Which kind is allowed with which grain:**

| Column kind | Grain = one row per source row | Grain = one row per group |
|---|---|---|
| `row` | ✅ | ❌ unless the value is the same for the whole group. The system then suggests `ANY_VALUE(...)`. |
| `window` | ✅ | ❌ |
| `aggregate` | ❌ "add `OVER (PARTITION BY ...)` or set a grain" | ✅ |
| `group_key` | – | ✅ |

### 2.3 Changes to the data model

File: `engine/agents/contracts.py`. All new fields are optional, so old mappings
still load.

```python
class GrainSpec(BaseModel):
    group_by: list[str]          # e.g. ["POLREF"]
    pre_filter: str = ""         # optional: which rows to include before grouping
    declared_by: str = "reviewer"
    rationale: str = ""

class MappingEntry(BaseModel):   # existing
    ...
    transform_kind: str = "row"  # filled in by the system, not by the browser
    partition_by: list[str] = [] # for window columns, e.g. ["POLREF"]

class MappingSpec(BaseModel):    # existing
    ...
    grain: Optional[GrainSpec] = None   # None = one row per source row (today)
```

### 2.4 New shared module: `engine/etl_sql.py`

Every stage uses this module instead of building SQL itself.

**a) `classify_expression(expr, grain)`: works out the column kind.**

It uses DuckDB's own SQL parser (`json_serialize_sql`), which turns the SQL
into a tree we can inspect. It does not use the LLM or guesswork.

1. If the SQL can't be parsed (for example someone typed `GROUP BY` or `;`),
   reject it.
2. If it contains a sub-query (`SELECT ... FROM ...`), reject it. This also
   blocks reading files.
3. If it contains `OVER (...)`, it is a `window`.
4. If it contains `SUM`, `COUNT`, `MAX` and similar outside an `OVER`, it is an
   `aggregate`.
5. Otherwise it is a `row`. With a grain, it is a `group_key` if it only uses the
   group columns.
6. Check the result against the "which kind is allowed" table above.
7. If a window uses an order-based function (`row_number`, `lag`,
   `first_value`, …) without `ORDER BY`, reject it, because the result would
   change between runs.
8. Run the SQL once with `LIMIT 0` against the real data to catch unknown
   columns.

**b) `build_target_sql(spec)`: builds the one SQL statement that creates the target.**

When grain = one row per source row (today, plus group totals):

```sql
SELECT <field SQL> AS "field1", <field SQL> AS "field2", ...
FROM (SELECT *, row_number() OVER () AS "__src_ord" FROM (<source data>)) AS "__workset"
ORDER BY "__src_ord"
```

* Each source row is numbered first (`__src_ord`), and the output is sorted by
  that number. Rows therefore always come out in source order, even with group
  totals.
* The source data is always called `"__workset"`, the same name the certify
  check uses.

When grain = one row per group:

```sql
SELECT <field SQL> AS "field1", ...
FROM (<source data>) AS "__workset"
[WHERE <pre_filter>]
GROUP BY "POLREF"
ORDER BY "POLREF"
```

**c) `check_same_within_group(expr, grain)`: finds the `ANY_VALUE` cases.**

It counts groups where the field has more than one value. If there are none,
it suggests `ANY_VALUE(<field SQL>)`. If there are some, it shows example
groups so the reviewer can choose an aggregate instead.

**d) Safety.** Connections that run reviewer SQL load the source files first,
then switch off file access (`SET enable_external_access = false`).

### 2.5 What changes in each stage

#### Mapping, certify check (`engine/agents/validator.py`)

* Use `classify_expression` instead of the current `count(...)` test.
* Build the test target with `build_target_sql`, exactly like Transformation will.
* Row count check:
  * one row per source row: target rows = source rows (as today).
  * one row per group: target rows = number of distinct groups.
* With a grain, at least one target field must carry the group column, so each
  output row can be identified.
* The "value lost" check is done per group for window and aggregate fields,
  instead of per row.

#### Mapping, review step (`engine/agents/reviewer.py` and `web/static/app.js`)

* **Grain panel** at the top of the review screen.
  * The default reads "One row per source row (199)".
  * The reviewer can pick group column(s). The panel shows a live preview, for
    example "199 rows → 30 groups".
  * The choice is sent as `decisions["__grain__"] = {"action": "set_grain", "group_by": ["POLREF"]}`.
* **"Write SQL"** button on every card, including unmapped fields.
* **One card per field.** Today an unmapped field can show two cards that
  overwrite each other.
* **"Override"** on auto-accepted fields in the final mapping table.
* **"Test"** button in the SQL editor. It shows the column kind, any error, and
  10 sample values.
* After the grain changes, fields that no longer fit are sent back for review,
  with an `ANY_VALUE` suggestion where possible.
* The final mapping table shows a small kind badge per field (`window ⟂ POLREF`,
  `aggregate`, …) and the grain above the table.

New read-only API endpoints (`api/server.py`):

| Endpoint | Input | Output |
|---|---|---|
| `POST /api/mapping/preview-expression` | the SQL and the grain | kind, error message, 10 sample values |
| `POST /api/mapping/grain-preview` | group columns, pre-filter | source rows, number of groups, largest group, examples |

#### Transformation (`api/transform.py`)

* Run the SQL from `build_target_sql`, and nothing else.
* The downloadable ETL script contains the same SQL, with a header such as
  `# Target grain: one row per POLREF · 4 window columns`.

#### Validation (`api/validate.py`)

* Recalculate the expected target once, using `build_target_sql`, then compare
  it with the delivered file.
* Match expected and delivered rows by:
  * the group column, when there is a grain;
  * else the business key (e.g. `role_record_id`);
  * else the row position.

  This fixes the false errors seen today.
* New checks:
  * for window fields: the value is the same on every row of a group;
  * for one row per group: report groups that are missing or unexpected.

#### Reconciliation (`api/reconcile.py`, `api/recon_rules.py`)

* Calculate totals from the target SQL as a whole. Today the field's SQL is put
  inside `sum(...)`, which fails for window and aggregate fields.
* If a total can't be calculated, show it as **skipped, with the reason**,
  instead of dropping it silently.
* New controls when there is a grain:
  * number of groups delivered = number of groups in the source;
  * the group keys match.
* **Totals must add up.** For simple `SUM` and `COUNT` fields, the delivered
  totals must equal the same total calculated directly from the source. Example:
  the sum of all `total_monthly_premium` values must equal the sum of `CTBAMT`
  in the source for active roles.
* The downloadable reconciliation script gets the same changes.

### 2.6 Error messages shown to the reviewer

| Situation | Message |
|---|---|
| Typed `GROUP BY` or more than one statement | "Write one column expression only. To group rows, set a target grain." |
| Used a sub-query | "Sub-queries aren't supported. Use `AGG(...) OVER (PARTITION BY ...)`, or set a target grain." |
| Aggregate without a grain | "`SUM` adds up rows. Add `OVER (PARTITION BY ...)` to keep one row per source row, or set a target grain." |
| Window function with a grain | "With a target grain, use the aggregate without `OVER`." |
| Normal field varies within a group | "`<field>` has different values within a `POLREF` group. Use an aggregate, or make it a group column." |
| Normal field is the same within each group | "`<field>` is the same within each group. Suggested: `ANY_VALUE(...)`." |
| Order-based window without `ORDER BY` | "`row_number` depends on row order. Add `ORDER BY` inside `OVER (...)`." |
| Unknown column | DuckDB's message, e.g. "column X not found" |
| Grain set but no field carries the group column | "No target field contains `POLREF`, so output rows can't be identified." |

### 2.7 Examples from the test data

**Type 1**: target `policy_client_role` (199 rows), grain left as default:

| Field | SQL the reviewer writes | Kind |
|---|---|---|
| `policy_beneficiary_count` | `COUNT(*) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30') OVER (PARTITION BY "POLREF")` | window |
| `policy_total_monthly_premium` | `SUM(TRY_CAST(NULLIF(trim("CTBAMT"), '') AS DECIMAL(12,2))) FILTER (WHERE "STAXRF" = '30') OVER (PARTITION BY "POLREF")` | window |
| all other fields | unchanged | row |

**Type 2**: target `policy_summary` (30 rows), grain = `POLREF`:

| Field | SQL | Kind |
|---|---|---|
| `policy_reference` | `trim("POLREF")` | group_key |
| `product_type` | `ANY_VALUE(CASE "PRDREF" WHEN 'RS-11' THEN 'Personal Pension' … END)` (suggested by the system) | aggregate |
| `beneficiary_count` | `COUNT(*) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30')` | aggregate |
| `total_monthly_premium` | `SUM(TRY_CAST(NULLIF(trim("CTBAMT"), '') AS DECIMAL(12,2))) FILTER (WHERE "STAXRF" = '30')` | aggregate |

### 2.8 Compatibility

* Old mappings have no grain and no column kind. They load as "one row per
  source row" with all `row` fields, which is today's behaviour.
* The only visible change for old mappings is the extra `ORDER BY "__src_ord"`
  in the generated SQL.
* No LLM is used in any of this, so generated scripts stay identical every time
  they are generated.

---

## Part 3: As implemented (2026-10-24)

Implemented as specified above, with these differences.

| Specification | As built | Why |
|---|---|---|
| `GrainSpec.pre_filter` — free-text SQL | Removed. The structured **row filter** (`MappingSpec.row_filter`, built earlier) is the grain's pre-filter. Grouping and windows only ever see rows the filter kept. | A free-text `WHERE` would reintroduce the reviewer-written SQL the row filter was designed to avoid, and two filters would be confusing. `GrainSpec` gains `errors`, so an invalid grain is kept and shown, not dropped. |
| File access switched off on every connection that runs reviewer SQL | Switched off in the **transform**, which loads the source rows first and then disables file access. Everywhere else relies on the classifier, which runs before anything executes and rejects sub-queries, the only place a file-reading function can appear. | Validation and reconciliation connections must keep reading the source CSVs between checks. The staging warehouse is a shared file database. |
| "Value lost" check per partition / per group | For **window and aggregate** columns the row-level heuristic is replaced by a pass that states the reason. These columns are verified by full re-execution in Validation and, for windows, by `partition_consistency`. | A per-partition figure is legitimately NULL when the partition has no qualifying rows (e.g. a SUM … FILTER for a policy with no active beneficiary). The heuristic raised false alarms. |
| Reconciliation `partition_constant` control | Not added. Validation's `partition_consistency` already checks it on the delivered values. | It would duplicate a validation check, which the reconciliation design deliberately avoids. |
| Totals add up: SUM and COUNT | SUM and COUNT, **excluding `DISTINCT`**. | One value can belong to several groups (e.g. one assessor handles several claim types), so distinct counts do not add up. The test on the claims dataset found this. |
| "Same within group" check | NULL counts as a value of its own. | A column set on one row of a group and blank on the others is not constant. Without this, `ANY_VALUE` was wrongly offered for a per-role premium. |

Other changes made along the way:
* **Reconciliation** rebuilds the certified target once (`_expected`) and takes
  every source-side figure from it: totals, category counts and business
  aggregates. The generated reconciliation script does the same. It now always
  rebuilds the joined source data, which also fixes totals for columns that come
  from a joined file.
* **"Accept all suggested"** now fills in only undecided items. Previously it
  overwrote decisions already made, which discarded a hand-written aggregate.
* **Edits without `source_attributes`** record the columns the SQL actually
  reads, taken from the parser.

Tests: `tests/test_groupby.py`. It covers the legality matrix, both scenarios
end to end, every rejection, row filter combined with grain, the preview
endpoints, script determinism and reproduction, and the same features on the
unrelated `CLAIMS99` dataset (column names with spaces and unicode).
