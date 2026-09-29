# Accuracy review: `docs/python/getting-started/crash-course/parallelization.md` (DOC-857 snapshot, f2ef483084)

**Category:** Tutorial. It's a Crash Course page, so no property names or thresholds should appear in the narrative. The configuration-placement rule applies.
**Scope:** Python file only. You asked me to leave out the Groovy sibling, so I didn't check the findings below against it. Before merging, sweep the Groovy page for the same claims (findings 2, 3 and 4).
**Source of truth:** the `/Users/margaretkennedy/dhc-skills-chip` working tree (HEAD 373cece093). I resolved links against the PR tree at f2ef483084.

Most of the page is accurate. There are four real findings, three minor ones, and one link note.

---

## Findings

### 1. Configuration property named in the narrative (line 158)
> "…larger than the default `QueryTable.minimumParallelSelectRows` (about 4.2 million rows)…"

- **The fact is correct.** `QueryTable.java:336-337` has `getLongWithDefault("QueryTable.minimumParallelSelectRows", 1L << 22)`, which is 4,194,304 rows.
- **The placement is wrong.** A Tutorial shouldn't carry an inline property name. Lines 57 and 171 already say the same thing at the right level ("a few million rows" and "about 4.2 million rows").
- **Fix:** drop the property name, for example "…with a table larger than the parallelization threshold (about 4.2 million rows)…". The exact property is already on the linked concept page.

### 2. The notes suggest table size is the only gate, but Python formulas never run in parallel on standard Python (lines 171 and 198)
- **Line 171:** "With 100 rows, the formula is evaluated serially by default; the race … would appear only on a table above the default parallelization threshold … or with a lowered threshold."
- **Line 198:** "This example uses only 100 rows, well below the threshold … so it wouldn't show the race…"
- **What the source says:** `DhFormulaColumn.isParallelizable()` (around line 899) returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`. The vectorized path agrees: `FormulaColumnPython.isParallelizable()` returns `isPythonFreeThreaded()`. `SelectColumnLayer.java:115-117` only splits a column when `sc.isStateless() && sc.isParallelizable()`.
- **The gap:** on a standard (GIL) Python build, `get_next_id()` is never parallelized at any table size. Line 158 says this correctly, but both notes present table size as the only condition. A reader on normal CPython who runs 5M rows will not see the race.
- **Fix:** add "and a free-threaded Python build" to the condition in both notes.

### 3. "Deephaven runs formulas in parallel by default" is too absolute (line 204, Key takeaways)
- **What the source says:**
  - Splitting one column's rows across threads needs `totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS` (`SelectColumnLayer.java:203-205`).
  - Python-backed formulas also need free-threaded Python (see finding 2).
  - Splitting work across columns has no size gate. `anyParallelColumns()` / `allowCrossColumnParallelization()` return `selectColumn.isStateless()`.
- **Fix:** frame it as eligibility, for example "Deephaven treats formulas as safe to run in parallel by default, so they must be stateless". That is backed by `QueryTable.statelessSelectByDefault` defaulting to `true` (`QueryTable.java:387-388`).

### 4. The "three ways" list leaves out filters and sorts (lines 12-16 and 42-57)
The page says Deephaven distributes work "in three ways" and describes the across-rows case only for formula columns. The engine also splits these across threads:
- **`where` filters:** `AbstractFilterExecution.shouldParallelizeFilter` needs `numberOfRows / 2 > QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT`, which defaults to `1 << 16`. Filters therefore split at about 131K rows, not "a few million."
- **Sorts:** `SortHelpers.java:1121-1122` uses `MINIMUM_PARALLEL_SORT_ROWS`, which defaults to `1L << 20`.

The linked concept page (PR tree) lists both under "What gets parallelized."

Line 57's "only … once a table is large enough (at least a few million rows)" is correct for formula columns but wrong for filters. If the chapter keeps only formulas, fix it at the tutorial's level of detail: either say "formulas in `update`/`select`" explicitly, or name filters and sorts as also split across threads. Don't add any thresholds.

---

## Minor findings

- **Missing import in the broken-counter snippet (lines 144-156).** It calls `empty_table(100)` without `from deephaven import empty_table`. It's a `syntax` block, so it isn't run, but a reader who copies it gets a `NameError`. The fixed version (line 180) has the import.
- **"`with_serial` is the only thing that guarantees…" (line 198).** The `ConcurrencyControl.withSerial` javadoc does promise "never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order". But `SelectColumn.withSerial()` only wraps the column in `StatefulSelectColumn` (`isStateless() == false`). Setting `statelessSelectByDefault=false` also makes a Python-param formula stateful (`DhFormulaColumn.isStateless`), so it takes the same serial path. Suggest "`with_serial` is how you guarantee…" instead of "the only thing." The supporting clause is verified: the `SelectColumn.isParallelizable` javadoc says the engine "may choose to evaluate it out-of-order" even when a column isn't parallelizable.
- **Line 57, "(at least a few million rows)", has an exception.** Formulas that return `Table` or `RowSet` skip the minimum size and use `divisionSize = 1` (`SelectColumnLayer.java:204-207`). It's low priority for a tutorial, but it's an exception to an "only" claim.

---

## Verified accurate (with source)

- **Across tables (line 40):** `PeriodicUpdateGraph.updateThreads` defaults to `-1`, which becomes `Runtime.getRuntime().availableProcessors()` (`PeriodicUpdateGraph.java:55, 141`). `ConcurrentNotificationProcessor` is used when `updateThreads > 1` (line 154-157).
- **The 20M rows / 4 cores note (line 55):**
  - Chunk size is `divisionSize = max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(total / threadCount))`, which gives `max(4.19M, 5M) = 5M`, so 4 chunks.
  - The operation-initialization pool is one thread per core: `OperationInitializationThreadPool.threads` defaults to `-1`, and `ThreadHelpers.getOrComputeThreadCountProperty` maps values `<= 0` to `availableProcessors()`.
- **Across columns (lines 61-69, 57):** `SelectAndViewAnalyzer.UpdateScheduler.doKickOffWork` fires every layer whose `getLayerDependencySet()` doesn't intersect `remainingLayers`. This has no size gate. Formulas using `i` aren't treated as constant, so they still form real layers.
- **`with_serial` wording (line 177):** matches the `ConcurrencyControl.withSerial` javadoc quoted above.
- **Python API:**
  - `Selectable.parse` and `Selectable.with_serial` exist (`py/server/deephaven/table.py:157, 222`).
  - `Table.update` accepts `Selectable` (line 1364-1383).
  - `agg_by(aggs, by)`, `tail`, `where` and `agg.sum_` are present.
  - `time_table` and `empty_table` are exported from `deephaven`.
- **Query-language built-ins:**
  - `randomGaussian(double, double)` and `randomInt(int, int)` are in `engine/function/.../Random.java`.
  - `hourOfDay(Instant, ZoneId, boolean)` and `dayOfMonth(Instant, ZoneId)` are in `DateTimeUtils.java:3278, 3509`.
  - `Duration * i` resolves to `DateTimeUtils.multiply(Duration, long)`.
  - `Instant + Period` resolves to `plus(Instant, Period)`.
  - `' '` is kept as a char literal: `TimeLiteralReplacedExpression` skips values of length `<= 1`.
- **Race explanation (line 173):** plausible for the unsynchronized `counter += 1` on free-threaded Python.

---

## Links

| Link | Result |
|---|---|
| `../../reference/table-operations/filter/where.md` | exists |
| `../../reference/table-operations/group-and-aggregate/aggBy.md` | exists |
| `../../reference/table-operations/filter/tail.md` | exists |
| `../../conceptual/query-engine/parallelization.md` | exists; covers barriers (PR tree `### Barriers`), so the "barriers and other concurrency-control tools" pointer holds |
| `../../reference/query-language/types/Selectable.md#with_serial` | exists in the PR tree (f2ef483084) with a `### with_serial` heading. **Not in the current checkout (HEAD 373cece), which isn't based on the PR.** It resolves only if the PR's new `Selectable.md` lands with it. |

**Link suggestion:** `update` (lines 44, 61) and `empty_table` are used without links. `docs/python/reference/table-operations/select/update.md` exists (the concept page links it).

---

## Completeness sweep
- The threshold figure appears at lines 55, 57, 158, 171 and 198. Findings 1 and 2 cover all of them.
- The size-only-gate problem appears at lines 171 and 198. Finding 2 covers both.
- "Parallel by default" appears at line 8 (the Tip, which is fine as a benefit claim) and line 204 (finding 3).
- There are no other code comments that repeat a wrong claim. The comment at line 34, "can update at the same time", is correctly hedged.
- Groovy sibling: not checked, per your scope.