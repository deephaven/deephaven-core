# Accuracy review: Crash Course, "Query Parallelization" (Python)

**File:** `docs/python/getting-started/crash-course/parallelization.md`, reviewed from the snapshot at `.claude/skills/deephaven-core-accuracy-check/evals/files/crash-course-parallelization-pre-review.md` (DOC-857, commit f2ef483084). I checked the claims against source in `/Users/margaretkennedy/dhc-skills-chip` (HEAD 373cece093).

**Category:** Tutorial. It is under `getting-started/crash-course/`, the Crash Course is the only Tutorial category, and the Concept-guide/Tutorial rule of keeping configuration detail out of the narrative applies.

**Scope:** Accuracy and links only. You put the Groovy sibling out of scope, so I skipped the cross-language check. No edits were made.

---

## Summary

Most of the page checks out: the code examples, APIs, the threshold, the thread-pool defaults, the free-threaded Python gate, and the `with_serial` contract. There are **3 medium findings** and **6 low findings**, all below:

1. The "Key takeaways" bullet overstates the default ("runs formulas in parallel by default") and contradicts the page's own threshold and GIL caveats.
2. A configuration property name is embedded in the tutorial narrative.
3. The "across rows" coverage is incomplete: `where` filters and `sort` also parallelize, at much lower thresholds, and the page doesn't say so.

---

## Findings

### Medium

**M1. "Deephaven runs formulas in parallel by default" (Key takeaways, line 204) is overstated and contradicts the page.**
Row-splitting within a column only happens when all of these hold:
- the column's work reaches the threshold;
- the job scheduler has more than one thread;
- the update has no shifts;
- the column is stateless and parallelizable.

Python-backed formulas also fail the "parallelizable" test unless Python is free-threaded. Source:
- `SelectColumnLayer.java:115-117`: `canParallelizeThisColumn = !isRedirected && ...supportsParallelPopulation(writableSource) && sc.isStateless() && sc.isParallelizable();`
- `SelectColumnLayer.java:203-205`: `if (canParallelizeThisColumn && jobScheduler.threadCount() > 1 && !hasShifts && ((resultTypeIsTableOrRowSet && totalSize > 0) || totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS))`
- `DhFormulaColumn.java:899-905` and `FormulaColumnPython.java:63-65`: `return !usesPython || PythonFreeThreadUtil.isPythonFreeThreaded();`

The page itself says this elsewhere: line 57 ("only splits ... once a table is large enough"), and the notes at lines 55, 171 and 198 ("100 rows ... evaluated serially by default").

Suggested fix, at tutorial resolution: "Deephaven parallelizes formulas automatically when it's worth it, which assumes your code is stateless." No property names are needed.

**M2. Configuration property named in the tutorial narrative (line 158).**
"...larger than the default `QueryTable.minimumParallelSelectRows` (about 4.2 million rows)..."

The value itself is correct: `QueryTable.java:336-337` has `getLongWithDefault("QueryTable.minimumParallelSelectRows", 1L << 22)`, which is 4,194,304. The gate is actually `>=` rather than "larger than", which makes no practical difference.

The problem is placement. A property name mid-paragraph in a Tutorial is configuration injection. The rest of the page already works at "about 4.2 million rows / a few million rows" resolution.

Suggested fix: drop the property name from line 158 and keep "about 4.2 million rows by default". If readers need the knob, link to [query table configuration](../../conceptual/query-table-configuration.md). That file exists at f2ef483084, and its line 123 documents `QueryTable.minimumParallelSelectRows | 1L << 22`. The conceptual parallelization guide, already linked at line 210, is another option.

Sweep result: this is the only property name in the file. Lines 57, 171 and 198 already use plain-language thresholds.

**M3. "Across rows" covers only formula columns, but filters and sorts split rows too, at much lower thresholds.**

Line 12 frames the page as "Deephaven distributes work across cores in three ways". Lines 15 and 44 describe "across rows" only as "computing values". Line 57 gives the only row-split threshold: "at least a few million rows". But source shows other operations split rows as well:
- **`where` splits into segments of 65,536 rows.** `QueryTable.java:275-276` sets `PARALLEL_WHERE_ROWS_PER_SEGMENT = ...("QueryTable.parallelWhereRowsPerSegment", 1 << 16)`, and parallel filtering is gated in `InitialFilterExecution.java:42-48` and `WhereListener.java:90`.
- **`sort` parallelizes from 1,048,576 rows.** `QueryTable.java`: `MINIMUM_PARALLEL_SORT_ROWS = ...("QueryTable.minimumParallelSortRows", 1L << 20)`.

The linked concept guide (at f2ef483084) lists both, under "What gets parallelized": "Filters in `where` clauses" and "`sort`, once the table is large enough".

This matters for correctness, not just completeness. A reader with a stateful filter learns only about the few-million-row formula threshold and `with_serial` on `Selectable`. They get no hint that `where` filters are parallelized, or that `Filter` objects have their own serial control. The page even uses `where` itself in the across-tables example (line 35).

Suggested fix: add one sentence noting that filters (`where`) and sorts also split rows across cores, and point to the concept guide's "Serial filters" section. Don't add thresholds or property names.

### Low

**L1. "Once a table is large enough" (line 57, and the note at line 55) describes the gate as table size. For ticking tables it is the size of each cycle's update.**
`SelectColumnLayer.java:200` computes `final long totalSize = upstream.added().size() + upstream.modified().size();`, and line 203 turns parallelism off when there are shifts (`!hasShifts`).

The example here is static, so the text is right for that example. But the page just showed a ticking table. A huge ticking table that receives small updates each cycle won't split rows during those cycles.

Possible fix: "once there are enough rows to process (at least a few million)". Or leave it, since this nuance may be too fine for a tutorial.

**L2. The 20-million-row note (line 55) says columns are chunked "independently for `Price`, `Quantity`, and `Total`", but `Total` depends on the other two.**
The chunk math is correct: `SelectColumnLayer.java:207-208` gives `divisionSize = max(4,194,304, ceil(20M/4)) = 5M`, so four chunks. The thread pool claim is also correct: `OperationInitializationThreadPool.java:29-30` passes `-1` to `ThreadHelpers.getOrComputeThreadCountProperty`, which uses `availableProcessors()` when the value is `<= 0`.

The issue is the dependency: `SelectAndViewAnalyzer.java:1006` fires a layer only when `!layers[nextLayer].getLayerDependencySet().intersects(remainingLayers)`. So `Total`'s chunks can't start until `Price` and `Quantity` have finished. "All four cores would compute their chunks simultaneously" holds within each column, not across all three columns at once.

A smaller point: the chunk count also depends on the row count, not only the pool size (because of the `max(threshold, ...)` floor). For example, 8 million rows on 4 cores gives 2 chunks. The 20M/4 example happens to avoid this.

**L3. The counter prose doesn't match the illustrative output table (lines 160-173).**
The prose says two cores both read `counter = 5` and "both return 6". The table shows duplicate `2`s and `5`s, and no `6` at all.

Also, `SelectColumnLayer` splits rows into *contiguous* ranges, one per task (`getNextRowSequenceWithLength(divisionSize)`), and each task walks its range in order. The first seven rows of the result would therefore all come from a single task's range, so a race-damaged sequence wouldn't look like adjacent rows swapping values.

The table is framed as "values like", so this isn't strictly wrong. Still, making the prose and the table agree would help, for example by having the prose say both return 5.

**L4. "Row 2 might be processed before row 1" (line 138) can't happen under the engine's actual split.**
Tasks get contiguous ranges of at least about 4.2 million rows (`divisionSize >= MINIMUM_PARALLEL_SELECT_ROWS`), so rows 1 and 2 are always in the same task and are evaluated in order. Out-of-order evaluation happens across task boundaries.

This is presented as an e.g., so it is illustrative. A phrasing that stays true would be "rows in one part of the table might be processed before rows in an earlier part".

**L5. The `with_serial` paraphrase adds "one at a time" (line 177), and the note at line 198 says `with_serial` "is the only thing that guarantees rows are processed one at a time, in order".**
The contract in `table-api/src/main/java/io/deephaven/api/ConcurrencyControl.java:32-33` reads: "The expression will never be invoked concurrently with itself." and "Rows are evaluated sequentially in row set order."
- "never running concurrently with itself" and "row-set order" match the contract.
- "one at a time" is a granularity the javadoc doesn't promise. Evaluation is chunked, so it reads as one call per row. "Sequentially" is the safer word.
- "The only thing that guarantees" is an absolute claim.
- The rationale at line 198, "parallelization isn't the only way execution order can vary", has no source cited on the page.

AQ1 [The fix, note at line 198]: What besides parallelization can change the order in which a column's rows are evaluated? It should be named or cited, or the clause should be dropped.

**L6. The "broken counter" snippet (lines 144-156) calls `empty_table` without importing it.**
The block is tagged `syntax`, so it isn't executed. A reader who copies it will still get a `NameError`. Add `from deephaven import empty_table` to match the fixed version at line 180.

---

## Verified as accurate (with source)

**Update-graph and initialization thread pools default to one thread per core (lines 40 and 55).**
- `PeriodicUpdateGraph.java:54-55`: `"PeriodicUpdateGraph.updateThreads", -1`.
- `PeriodicUpdateGraph.java:140-141`: `if (numUpdateThreads <= 0) this.updateThreads = Runtime.getRuntime().availableProcessors();`.
- `PeriodicUpdateGraph.java:154-157`: `ConcurrentNotificationProcessor` is used when `updateThreads > 1`.
- `server/.../UpdateGraphModule.java:29` wires `NUM_THREADS_DEFAULT_UPDATE_GRAPH`.
- The initialization pool is covered under L2.

**Independent columns can be computed at the same time, even on small tables (lines 16, 61-69).**
- `QueryTable.java:1825-1830`: when `...getOperationInitializer().canParallelize() && analyzer.anyParallelColumns()`, initialization uses `OperationInitializerJobScheduler`. This path has no row-count gate.
- `SelectColumnLayer.java:657-658`: `allowCrossColumnParallelization()` returns `selectColumn.isStateless()`.
- `SelectOrUpdateListener.java:68-72`: the same applies to update cycles.
- The 10-row "across columns" example is therefore valid.

**Below the threshold, a column runs single-threaded while other columns and tables can still run concurrently (line 57).** This matches the `else` branch `doSerialApplyUpdate` in `SelectColumnLayer.java` together with the cross-column scheduling above.

**Free-threaded Python gate (line 158):** `FormulaColumnPython.isParallelizable()` returns `PythonFreeThreadUtil.isPythonFreeThreaded()`. `DhFormulaColumn.isParallelizable()` returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`.

**Stateless by default:** `QueryTable.java:387-388` has `"QueryTable.statelessSelectByDefault", true`. Both `DhFormulaColumn.isStateless()` and `FormulaColumnPython.isStateless()` return that default.

**Python APIs, all in `py/server/deephaven/`:**
- `time_table`, `empty_table`: `table_factory.py:69,87`, exported from `deephaven/__init__.py`.
- `agg.sum_(cols)`: `agg.py:75`.
- `Table.agg_by(aggs, by=...)`: `table.py:2541`.
- `Table.where`: `table.py:1508`.
- `Table.tail(num_rows)`: `table.py:1632`.
- `Selectable.parse` (classmethod): `table.py:157`.
- `Selectable.with_serial()`: `table.py:222`, calls Java `withSerial()`.
- `Table.update` accepts `Selectable`: `table.py:1364-1385`.

**Query-language functions and literals:**
- `hourOfDay(Instant, ZoneId, boolean)`: `DateTimeUtils.java:3278`.
- `dayOfMonth(Instant, ZoneId)`: `DateTimeUtils.java:3509`.
- `Instant + Period`: `plus(Instant, Period)` at `DateTimeUtils.java:1790`.
- `'PT1m' * i`: `multiply(Duration, long)` at `DateTimeUtils.java:2202`.
- `'ET'` becomes a `ZoneId` via `DateTimeUtils.parseTimeZoneQuiet`, and single-character literals like `' '` stay chars: `TimeLiteralReplacedExpression.java:86,131-136`.
- `randomGaussian(double, double)` and `randomInt(int, int)`: `engine/function/.../Random.java:57,173`.
- `sqrt(primitive)`: `Numeric.ftl:2782`.

**Static `empty_table(...).update(...)` isn't redirected, so row-splitting applies:** `SelectAndViewAnalyzer.java` `SELECT_STATIC` builds `new SelectColumnLayer(..., false /*isRedirected*/, ...)`.

---

## Links

All targets exist at f2ef483084, checked with `git ls-tree`:
- `../../reference/table-operations/filter/where.md`
- `../../reference/table-operations/group-and-aggregate/aggBy.md`
- `../../reference/table-operations/filter/tail.md`
- `../../reference/query-language/types/Selectable.md#with_serial`: the file exists at f2ef483084 and has a `` ### `with_serial` `` heading (line 23), so the anchor resolves. The file is not in the current working tree at 373cece093, so it is added by this PR's branch.
- `../../conceptual/query-engine/parallelization.md`

There are no external links in the file.

---

## Completeness sweep

- **M1:** "Runs formulas in parallel by default" appears only at line 204. The same idea appears at lines 8 and 208 ("Most queries benefit ... automatically", "parallelization handles everything automatically"), but those don't claim unconditional parallelism, so they are fine.
- **M2:** No other property names in the file.
- **M3:** No mention of `where` or filter parallelism anywhere in the file apart from its use in the across-tables example.
- **L5:** "One at a time" appears at lines 177 and 198.
- Everything else was checked at every occurrence.
- Groovy sibling: not checked, per your scope. The same findings (M1, M2, M3, L5, L6) are likely present there.