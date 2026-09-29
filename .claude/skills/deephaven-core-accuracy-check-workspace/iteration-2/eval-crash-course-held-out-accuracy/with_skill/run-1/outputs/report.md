# Accuracy review: `docs/python/getting-started/crash-course/parallelization.md` (DOC-857 snapshot, commit f2ef483084)

**Category:** Tutorial. It sits under `crash-course/`, so it is written for a first-time reader. That affects where configuration detail belongs (finding 4).

**Scope:** I checked technical accuracy and internal links in the Python file only. I made no edits. I checked the snapshot against `git show f2ef483084:docs/python/getting-started/crash-course/parallelization.md` and they are identical. I checked claims against source in the `dhc-skills-chip` checkout (HEAD 373cece093).

**Verdict:** Every code example is valid, and the core mechanisms, thresholds and thread-pool defaults are correct. There are three medium-severity accuracy problems (findings 1–3) and one placement fix (finding 4). The three medium ones all do the same thing: a later summary line drops a condition that the body states correctly.

---

## Findings

### 1. The note under the broken counter drops the free-threaded Python condition (medium, line 171)
Line 158 gets this right: Python-backed formulas are parallelized only on free-threaded builds. The note at line 171 then says the race "would appear only on a table above the default parallelization threshold (about 4.2 million rows) or with a lowered threshold." On a standard CPython build with the GIL, the race never shows up within the column, whatever the table size.

- `FormulaColumnPython.isParallelizable()`: `// If we are not free-threaded, then we cannot be parallelized for performance reasons` / `return PythonFreeThreadUtil.isPythonFreeThreaded();`
- `DhFormulaColumn.isParallelizable()`: `return !usesPython || PythonFreeThreadUtil.isPythonFreeThreaded();`
- `SelectColumnLayer` (line 115): `canParallelizeThisColumn = ... && sc.isStateless() && sc.isParallelizable();`

**Fix:** "...would appear only on a free-threaded Python build, with a table above the parallelization threshold (roughly 4 million rows)."

### 2. The advice to use `with_serial` implies it is always enough (medium, line 198)
Line 198 reads: "Use `with_serial` any time your formula depends on shared state or row order... `with_serial` is the only thing that guarantees rows are processed one at a time, in order."

That holds for a single formula. If two columns in the same `update` touch the same state, `with_serial` alone does not stop them running at the same time under the defaults:

- `ConcurrencyControl.withSerial()` javadoc promises only: "The expression will never be invoked concurrently with itself." It also says: "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed... To impose further ordering constraints, use barriers."
- `QueryTable.java`: `SERIAL_SELECT_IMPLICIT_BARRIERS = ...getBooleanWithDefault("QueryTable.serialSelectImplicitBarriers", !STATELESS_SELECT_BY_DEFAULT)`, and `STATELESS_SELECT_BY_DEFAULT` defaults to `true`. So the barrier setting is `false` by default.

"The only thing" is also too strong. Setting `QueryTable.statelessSelectByDefault=false` makes every formula stateful: no within-column parallelism, and implicit barriers turn on.

**Fix:** Limit the sentence to one formula and name barriers for the multi-column case. For example: "...`with_serial` guarantees that a single formula is never run concurrently with itself and processes rows in order. If more than one column touches the same state, you also need barriers — see [query parallelization](../../conceptual/query-engine/parallelization.md)." The link on line 210 already points readers to barriers.

**Sweep:** The Key takeaways bullet on line 206 ("Use `with_serial` to force sequential execution when your formula needs it") is fine, because it is about one formula.

### 3. "Deephaven runs formulas in parallel by default" overstates it (low–medium, line 204)
The page itself says otherwise in three places:
- Line 57: a column is split across cores only above a size threshold.
- Line 158: Python formulas are split only on free-threaded builds.
- Line 171: a 100-row table is evaluated serially.

In source, a column is split only if all of these hold: `canParallelizeThisColumn && jobScheduler.threadCount() > 1 && !hasShifts && ... totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS` (`SelectColumnLayer`, around line 203). What is true by default is that formulas are *treated as safe to parallelize*: `STATELESS_SELECT_BY_DEFAULT = true`.

**Fix:** "Deephaven assumes formulas are safe to run in parallel by default, and parallelizes them when it's worthwhile — this is fast but requires stateless code."

### 4. A configuration property name in the tutorial narrative (placement, line 158)
The text "larger than the default `QueryTable.minimumParallelSelectRows` (about 4.2 million rows)" puts a property name in the middle of a Crash Course explanation. The value itself is correct: `getLongWithDefault("QueryTable.minimumParallelSelectRows", 1L << 22)` = 4,194,304. The check is `>=`, so "at least" is more precise than "larger than."

**Fix:** Keep the plain-language threshold ("a table of roughly 4 million rows or more"). Drop the property name, or move it to a link to `../../conceptual/query-table-configuration.md` (that file exists at f2ef483084).

### 5. A code block is missing an import (low, lines 144–156)
The `syntax` block calls `empty_table(100)` without `from deephaven import empty_table`. Anyone who copies and runs it gets a `NameError`. The corrected block at line 180 has the import.

### 6. The 20-million-row note implies `Total` runs alongside the other columns (low, line 55)
The note says each column is split "independently for `Price`, `Quantity`, and `Total`." `Total = Price * Quantity` depends on both, so it waits for them to finish. In `SelectAndViewAnalyzer.UpdateScheduler.doKickOffWork` a layer runs only when `!layers[nextLayer].getLayerDependencySet().intersects(remainingLayers)`.

The chunk arithmetic is correct: `divisionSize = Math.max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(totalSize / threadCount))` = max(4,194,304, 5,000,000) = 5M, which gives four chunks.

**Fix:** "...`Price` and `Quantity` are each split into four chunks and computed in parallel, then `Total` (which depends on both) is split the same way."

### 7. The prose and the sample output table disagree (low, lines 160–173)
The prose says two cores "both return 6." The table has no 6: it shows `5, 5, 7`, which fits both cores reading 4. Either change the prose to "read `counter = 4`... both return 5," or make the table match.

---

## Verified correct (with source)
- **Three ways (tables, rows, columns):** matches how the engine works for `update`/`select`.
  - **Across tables:** `PeriodicUpdateGraph` uses a `ConcurrentNotificationProcessor` when `updateThreads > 1`.
  - **Across rows:** the `SelectColumnLayer` split described in finding 6.
  - **Across columns:** each independent layer is submitted separately to the job scheduler. A column can be scheduled alongside others only if `allowCrossColumnParallelization()` (= `selectColumn.isStateless()`) is true. Initialization uses `OperationInitializerJobScheduler` when `canParallelize()` (`numThreads > 1 && !isInitializationThread.get()`), with no size gate. So line 69 ("can compute them on different cores") is accurate.
  - **Completeness note (optional):** `where` filters also run in parallel (`QueryTable.disableParallelWhere` / `forceParallelWhere` and related settings), and the chapter doesn't mention them. For a tutorial that's acceptable, since the concept guide at f2ef483084 uses the same three-way framing.
- **Line 40, "thread pool (sized to your CPU cores by default)":** `PeriodicUpdateGraph.updateThreads` defaults to -1, which becomes `Runtime.getRuntime().availableProcessors()`.
- **Line 55, "operation-initialization thread pool, which uses one thread per core":** `OperationInitializationThreadPool.threads` defaults to -1, and `ThreadHelpers.getOrComputeThreadCountProperty` turns that into `availableProcessors()`.
- **Line 57, "at least a few million rows":** matches 1<<22.
- **Line 177, the `with_serial` description:** the javadoc says "never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order." The paraphrase adds nothing beyond that.
- **Line 198, "parallelization isn't the only way execution order can vary":** `SelectColumn.isParallelizable` javadoc says "Even if a column is not parallelizable, the engine may choose to evaluate it out-of-order."
- **APIs used in the examples:**
  - `Selectable.parse`, `Selectable.with_serial`, `from deephaven.table import Selectable` (`py/server/deephaven/table.py`).
  - `Table.update` accepts a `Selectable`; `to_sequence` unwraps it.
  - `time_table`, `empty_table` and `agg` import from `deephaven`; `agg.sum_`, `agg_by`, `where` and `tail` exist.
- **Query-language built-ins:**
  - `randomGaussian(double, double)` and `randomInt(int, int)` come from `io.deephaven.function.Random`, which is a default import and uses `ThreadLocalRandom`, so it is safe in parallel.
  - `hourOfDay(Instant, ZoneId, boolean)` and `dayOfMonth(Instant, ZoneId)` exist in `DateTimeUtils`.
  - The `'ET'`, `'PT1m'` and `'P1D'` literals parse to `ZoneId`, `Duration` and `Period` (`TimeLiteralReplacedExpression`).
  - `'PT1m' * i` maps to `DateTimeUtils.multiply(Duration, long)`, and adding a `Period` to an `Instant` uses `plus(Instant, Period)`.
- **Using `i` in `update` on a `time_table`:** allowed. `AbstractFormulaColumn` throws only when `(usesI || usesII) && !sourceTable.isAppendOnly() && !sourceTable.isBlink()`, and a time table is append-only.
- **Enterprise-only features:** none are mentioned.

## Links
- **All five existing links resolve at f2ef483084:** `where.md`, `aggBy.md`, `tail.md`, `Selectable.md#with_serial` (the file has a `### \`with_serial\`` heading), and `conceptual/query-engine/parallelization.md`.
  - `Selectable.md` is **not** in the current checkout (HEAD 373cece093). It is added by the DOC-857 PR, so check that it lands in the same PR as this page.
- **Suggested new links (all confirmed to exist at f2ef483084):**
  - `time_table` → `../../reference/table-operations/create/timeTable.md`
  - `empty_table` → `../../reference/table-operations/create/emptyTable.md`
  - `update` → `../../reference/table-operations/select/update.md`
  - `agg.sum_` → `../../reference/table-operations/group-and-aggregate/AggSum.md`
  - "update graph" → `../../conceptual/dag.md`

## Author queries
- **AQ1 [The fix, note]:** Should the chapter say that turning off `statelessSelectByDefault` globally also stops within-column parallelism? Or is limiting the "only thing" claim to per-formula control (finding 2) enough for a tutorial?

## Not covered
- **Groovy sibling:** out of scope as instructed, so I did not check it for the same claims. Findings 1–4 describe shared text and should be checked in `docs/groovy/getting-started/crash-course/parallelization.md`. Finding 1 needs rewording rather than a copy there, since Groovy has no free-threaded-Python condition.