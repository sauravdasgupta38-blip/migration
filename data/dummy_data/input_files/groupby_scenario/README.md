# GROUP BY test scenarios

These files test transformations that need **GROUP BY**, meaning several source
rows added up or counted together. The mapping agent is not expected to find
these; the reviewer adds them by hand in the review step of the Mapping
workspace.

**Status (2026-10-24): both scenarios now work end to end.** The feature is
described in `docs/groupby_partition_plan.md`. The results before the change are
kept at the end of this file.

The original files in `../` are not changed. This folder holds copies, with
fields added.

## Files

| File | Upload to | Notes |
|---|---|---|
| `CO040.csv` | C · Source data | copy of the original, plus a new column `BENPCT` (beneficiary's share of the benefit, %, filled only on BN rows) |
| `PO010.csv` | C · Source data | unchanged copy |
| `dict_CO040.json` | A · Source dictionaries | copy, plus a `BENPCT` entry |
| `dict_PO010.json` | A · Source dictionaries | unchanged copy |
| `policy_client_role.json` | B · Target dictionary | **Scenario A** target (copy, plus 4 new fields) |
| `scenario_b_target/policy_summary.json` | B · Target dictionary | **Scenario B** target (new) |
| `expected_scenario_a.csv`, `scenario_b_target/expected_scenario_b.csv` | – | correct answers, computed in plain Python |
| `run_groupby_scenarios.py` | – | runs both scenarios automatically through the API |

---

## Scenario A: a group total on every row (PARTITION BY)

The target `policy_client_role` keeps **one row per client role (199 rows)**.
Four new fields hold a total for the whole policy, repeated on every role row of
that policy.

| Target field | Meaning | Example: policy 3104365 |
|---|---|---|
| `policy_active_role_count` | number of active roles on the policy | 8 |
| `policy_beneficiary_count` | number of active beneficiaries | 2 |
| `policy_beneficiary_share_total` | total of `BENPCT` for active beneficiaries | 100.00 |
| `policy_total_monthly_premium` | total of `CTBAMT` for active roles | 789.96 |

**How to do it in the UI:**

1. Upload the files with `policy_client_role.json` as the target, and run the mapping.
2. Leave **Target grain** as *one row per source row*.
3. On each of the four fields' cards, click **✎ Write mapping**, or **✎ Write SQL**
   on an unmapped field. Paste the SQL below and press **Test**: it should say
   `window ⟂ POLREF` and show sample values. Then press **Save mapping**.
4. If the agent auto-accepted a field you want to change, certify first and then
   use **Override** on its row in the final mapping table.
5. Click **Accept all suggested** for the rest (it no longer overwrites your own
   decisions), then **Certify mapping**.

| Field | SQL |
|---|---|
| `policy_active_role_count` | `COUNT(*) FILTER (WHERE "STAXRF" = '30') OVER (PARTITION BY "POLREF")` |
| `policy_beneficiary_count` | `COUNT(*) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30') OVER (PARTITION BY "POLREF")` |
| `policy_beneficiary_share_total` | `SUM(TRY_CAST(NULLIF(trim("BENPCT"), '') AS DECIMAL(9,2))) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30') OVER (PARTITION BY "POLREF")` |
| `policy_total_monthly_premium` | `SUM(TRY_CAST(NULLIF(trim("CTBAMT"), '') AS DECIMAL(12,2))) FILTER (WHERE "STAXRF" = '30') OVER (PARTITION BY "POLREF")` |

**Result:**
* Certified.
* Transformation: 199 rows in source order, all values equal to `expected_scenario_a.csv`.
* Validation: certified, including `partition_consistency` for each window column.
* Reconciliation: certified; the four columns are included in the numeric totals.

---

## Scenario B: one row per policy (GROUP BY)

The target `policy_summary` has **one row per policy (30 rows)**, built from the
199 role rows.

**How to do it in the UI:**

1. Upload the files with `scenario_b_target/policy_summary.json` as the target,
   and run the mapping.
2. In **Target grain**, choose `POLREF` and press **Preview grain**. It shows
   *199 source rows → 30 target row(s)*.
3. Write the fields:

   | Field | SQL |
   |---|---|
   | `policy_reference` | `trim("POLREF")` (becomes the *group key*) |
   | `active_role_count` | `COUNT(*) FILTER (WHERE "STAXRF" = '30')` |
   | `beneficiary_count` | `COUNT(*) FILTER (WHERE "ROLREF" = 'BN' AND "STAXRF" = '30')` |
   | `total_monthly_premium` | `SUM(TRY_CAST(NULLIF(trim("CTBAMT"), '') AS DECIMAL(12,2))) FILTER (WHERE "STAXRF" = '30')` |

4. **Certify.** The grain is applied, and any field that doesn't fit it comes
   back in the queue:
   * `product_type` is the same within every policy, so it comes back with
     **Accept suggested fix** → `ANY_VALUE(CASE "PRDREF" … END)`.
   * A field that varies within a policy, such as the agent's per-row
     `TRY_CAST("CTBAMT")`, comes back as *Map or exclude*, with the reason.
5. Resolve those and **Certify** again.

**Result:**
* Certified.
* Transformation: 30 rows ordered by policy, all values equal to `expected_scenario_b.csv`.
* Validation: certified, with `grain_row_count`, `missing_group`, `unexpected_group` and group-key uniqueness all passing.
* Reconciliation: certified, with group count, group keys, and the totals adding up for the SUM / COUNT fields.

---

## What is rejected, and why

| Reviewer SQL | Message at certify |
|---|---|
| a sub-query, e.g. `(SELECT COUNT(*) FROM "__workset" …)` or one reading a file | Sub-queries aren't supported. Use `AGG(...) OVER (PARTITION BY ...)`, or set a target grain. |
| `… GROUP BY "POLREF"` typed into the editor | Write one column expression only. To group rows, set a target grain. |
| `SUM(…)` / `COUNT(…)` with no grain | Adds up rows. Add `OVER (PARTITION BY ...)` to keep one row per source row, or set a target grain. |
| `row_number() OVER (PARTITION BY …)` without `ORDER BY` | Depends on row order. Add `ORDER BY` inside `OVER (...)`. |
| a window function under a grain | With a target grain, use the aggregate without `OVER (...)`. |
| a field that varies within a group | Has different values within a group. Use an aggregate, or make it a group column. |

---

## Run the automated test

```
.venv\Scripts\python.exe data\dummy_data\input_files\groupby_scenario\run_groupby_scenarios.py all
```

The script uses its own temporary upload folder, so files uploaded in the running
UI are not affected. The permanent tests are in `tests/test_groupby.py`.

---

## Before the change (recorded 2026-10-24)

| Case | Certify | Transformation | Validation | Reconciliation |
|---|---|---|---|---|
| A: window SQL | ❌ the certify check wrapped it in `count(...)` | values correct, row order changed | ❌ false failures (rows matched by position) | ⚠️ fields silently skipped |
| A: sub-query SQL | ✅ (wrongly) | ❌ `__workset` does not exist | – | – |
| A: `GROUP BY` typed in | ❌ | – | – | – |
| B: one row per policy | ❌ | ❌ | – | – |
