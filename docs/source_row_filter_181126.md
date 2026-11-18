# Prompt: add a "Source row filter" to the Data Mapping review step

> **How to use:** open Claude Code at the root of the target application and
> paste everything below the line.

---

## Context

This application is a larger product with several menu items. One of them,
**Data mapping**, opens two tabs: **Mapping** and **Transformation**. The
Mapping tab takes source data files, a source dictionary per file and a target
dictionary, proposes a source-to-target mapping, and ends in a **review step**
where a person resolves exceptions and **certifies** the mapping. The
Transformation tab turns the certified mapping into an ETL (SQL run on the
source data, plus a downloadable generated script) and executes it.

Add a feature to the review step: a **Source row filter** that lets the reviewer
choose **which source rows migrate**. Only rows that match the filter are
transformed and delivered. The filter is optional: with no filter, everything
must behave exactly as it does today.

**This is a generic tool.** The data files, column names, codes and values on
disk are examples only. Do **not** hard-code any field name, file name, code,
value or row count anywhere: every field, its type and its allowed codes come
from the dictionaries the user uploads at runtime.

## Step 1: understand before changing

The Data mapping feature lives inside a bigger application, so first find, and
list in your report:

* where the Data mapping menu item's code lives (backend routes, frontend
  module, styles), and what is shared with the rest of the app;
* the mapping data model (the mapping spec object the review step certifies);
* how review decisions reach the server and are applied at certify;
* the validation that runs at certify, if any;
* how the Transformation tab builds and runs its SQL, and how it generates its
  script;
* how source files are staged and how several files are joined into one source
  (if they are);
* where the source dictionary keeps each column's type and code list (decode
  values);
* the existing tests, and run them to record a baseline.

Work **only inside the Data mapping feature**. Do not change shared or global
code, other menu items, or the app shell. If a change outside the feature seems
unavoidable, stop and explain why before making it.

## Step 2: what the reviewer sees

A panel titled **Source row filter (optional)** appears in the review step,
above the review items, once a mapping run has produced the list of source
fields. Sub-text: *Only source rows that match are migrated. Leave it empty to
migrate every row. "does not equal" and "is not one of" also keep rows where the
field is blank.*

* A top line: **Migrate rows that match [ALL (AND) | ANY (OR)] of these
  conditions.**
* Each condition is a row: **[Field] [Condition] [Value] [✕]**.
  * **Field:** every column of the (joined) source, from every uploaded file,
    grouped by file, labelled `<column name> — <business name>`.
  * **Condition:** only the conditions that make sense for the field's type
    (see Step 3).
  * **Value:** the control changes with the field's type:
    * a field with a code list in its dictionary → a dropdown of
      `<code> — <meaning>`;
    * a date field → a date picker;
    * a numeric/amount field → a number input;
    * any other text field → a text box, plus a **Pick from data** button that
      offers the most frequent values actually present in that column;
    * "is one of" / "is not one of" → multi-select (or comma-separated input);
      "is between" → two inputs; "is empty" / "is not empty" → no value.
  * Changing the field resets the condition and value; changing the condition
    resets the value.
* Buttons: **+ Add condition**, **+ Add bracket (group)** (one level of brackets,
  each bracket with its own AND/OR, default OR), **Check filter**, **Clear filter**.
* A live sentence under the builder: **Migrate only rows where:** followed by
  the filter in plain English, with brackets shown.
* **Check filter** asks the server how many rows the draft keeps and shows
  *Keeps K of N source rows (X excluded)* with a few example excluded rows. It
  warns if nothing is excluded, shows an error if nothing is kept (certify will
  be blocked), and lists the reasons if the filter is invalid.
* After certify, the panel shows *Applied filter keeps K of N source rows* and
  becomes read-only until the reviewer amends the decisions; it can then be
  edited or cleared and re-certified.
* The panel is **not a review item**: it does not change the "to resolve" count
  or "accept all"; a filter-only change must still allow re-certifying.

## Step 3: principles to follow

1. **Structured data, never SQL from the user.** The filter is stored on the
   mapping spec as a tree: a group `{logic: AND|OR, conditions: [...]}` whose
   items are conditions `{field, op, value, values}` or nested groups. The
   reviewer never types SQL, and the browser never builds SQL.
2. **One compiler, on the server.** A single function turns the tree into a SQL
   boolean expression. Certify, the row-count preview, the Transformation run
   and the generated script all call it. A compiled SQL string may be stored on
   the spec for display, but because the spec round-trips through the browser it
   is **never executed**: every consumer recompiles from the tree.
3. **The dictionary decides the type, not the client.** Each condition's type
   family comes from the source dictionary on the server: `date`, `number` or
   `text`. Map the dictionary's own type names to these three families; anything
   unrecognised is `text`.
4. **Type-aware comparisons.** Use whatever the app's SQL engine needs. If source
   data is staged as text, compare: text trimmed; numbers through a safe cast
   that yields NULL for non-numbers; dates parsed from the dictionary's stored
   date format, with the dataset's "no date" sentinel (for example an all-zero
   date) treated as empty.
5. **Conditions:** `equals`, `does not equal`, `is one of`, `is not one of`,
   `is empty`, `is not empty` (all types); `greater than`, `at least`,
   `less than`, `at most`, `between (inclusive)` (number and date); `contains`,
   `starts with`, `ends with` (text). "Empty" means NULL, blank, or the "no date"
   sentinel. "does not equal" and "is not one of" also keep empty rows (say so in
   the UI); every other comparison on an empty value is false.
6. **Validate values before quoting them.** Numbers must look like numbers,
   dates must be real dates, text literals have quotes escaped, control
   characters are rejected, and the field must be a real column of the source.
   Anything else is an error with a reviewer-friendly reason. Set sensible
   limits (e.g. 20 conditions, a cap on list size) and report every problem, not
   just the first.
7. **Where it applies:** on the joined source rows, **after** joins and
   **before** any column transformation (and before any grouping or window
   functions, if the app has them).
8. **Errors are visible, never silent.** An invalid filter is stored with its
   errors and shown to the reviewer; it blocks certification. A filter that
   keeps zero rows blocks certification; one that keeps every row gives a
   warning. The Transformation tab must refuse to run an invalid filter rather
   than migrate every row.
9. **No filter = no change.** With no filter, results and generated scripts
   must be identical to today's.
10. **No LLM** anywhere in this feature.

## Step 4: what to build

**Backend**

* A new, self-contained filter module (compile, validate, describe in plain
  English, normalise). It has no dependency on the web layer.
* The filter on the mapping spec, optional and defaulting to "none", so specs
  saved before this change still load.
* At certify, a reserved decision key carries `set_filter` (with the tree) or
  `clear_filter`. If the key is absent, the current filter is kept. The server
  normalises the tree (types filled from the dictionary, description, errors)
  and stores it, even when invalid.
* Certify-time validation runs on the **filtered** rows, records total and kept
  row counts on the spec, and adds checks for "filter valid" and "filter effect".
* The mapping run's result sends the UI the list of source fields:
  name, business name, origin file, type family and code list.
* Two read-only endpoints: **filter preview** (total / kept / excluded counts, a
  few excluded rows, or the errors) and **field values** (the most frequent
  values of one column, after checking it is a real column).
* Transformation: one helper returns "the source rows in scope" (joined source +
  `WHERE <compiled filter>`), and the run uses it. The run's stats report total
  rows, rows excluded by the filter, and the filter description. The generated
  script includes the same `WHERE` and a comment with the plain-English filter.

**Frontend:** the panel from Step 2, built inside the Data mapping module, with
the filter draft kept in its own state (separate from review decisions). Also:
show the filter on the final mapping summary and in any mapping downloads, and
add a line to the Transformation output such as *row filter: N source rows, X
excluded, K transformed*.

## Step 5: avoid conflicts with the existing app

* Before adding any name (file, module, class, function, route, CSS class, DOM
  id, state key, decision key, spec field), **search the whole application** to
  make sure it is not already used. Prefer a clear, feature-specific prefix such
  as `row_filter` / `rf-` / `rowFilter`.
* Put new routes under the Data mapping feature's existing route prefix, and
  follow its auth and request conventions.
* Scope all new CSS under the panel's own root class so it cannot restyle
  anything else; reuse the app's existing button and text styles.
* Keep new JavaScript inside the Data mapping module's scope: no new globals,
  and no edits to shared state or the app shell.
* Add only optional fields to shared data models; don't rename or change the
  meaning of existing ones.
* Add no new third-party dependencies.

## Step 6: tests and verification

* **Compiler tests** on a small in-memory table you create in the test (your own
  invented columns and values): every condition for every type family; empty and
  "no date" handling; bad values; unknown fields; limits; a value containing
  quotes and SQL keywords stays one safe literal; `A AND (B OR C)` differs from
  `(A AND B) OR C`; a client-supplied type or SQL string is ignored.
* **End-to-end test** through the API with whatever sample inputs the project
  has: run the mapping, certify with a filter, then transform. Compute the
  expected kept count **independently in the test** (for example with Python's
  `csv` module) instead of hard-coding a number. Also cover: a zero-row filter
  and an invalid filter both block certify; clear and keep work; the generated
  script contains the filter.
* **No-filter regression:** before editing, capture the transform output and
  generated script for a no-filter run. Afterwards, they must be identical.
* Run the full existing suite: everything that passed before must still pass.
* You cannot click through the UI, so give the user a short manual checklist:
  each value-control type, brackets, Check filter, certify, amend, clear, and
  the Transformation output line.

## Report back

The files changed (as links); the test results before and after; what you found
in Step 1 and how this app differs from what this prompt assumed; and any
deviation from this prompt, with the reason.
