# Accuracy review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857 snapshot, commit f2ef483084)

**Category:** Concept guide. It lives under `conceptual/`. The sidebar places it under Performance so readers can find it, but that doesn't change its category. Because it's a concept guide, any fix involving configuration detail is sent to a Configuration section or to `../query-table-configuration.md`. That page exists at f2ef483084 and already lists every property this page names. Fixes are not written inline.

**Scope:** Python file only, as you asked. The skill requires a check of the Groovy sibling (`docs/groovy/conceptual/query-engine/parallelization.md`, which exists), but I skipped it because you ruled it out of scope. Most findings below are claims about engine behavior, so expect the same defects in the Groovy page when it gets reviewed.

The snapshot is byte-identical to `docs/python/conceptual/query-engine/parallelization.md` at f2ef483084. I checked links against that commit's tree.

---

## A. Incorrect or overstated claims (fix these)

**A1. GIL caution: "on a standard (GIL-enabled) build, they're never run concurrently" is wrong (line 75)**
- The GIL gate only switches off splitting *one column's rows* across threads:
  - `FormulaColumnPython.isParallelizable()` and `DhFormulaColumn.isParallelizable()` both return `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`.
  - `SelectColumnLayer` uses it only in `canParallelizeThisColumn = ... && sc.isStateless() && sc.isParallelizable()`.
- Running different columns at the same time is gated on statelessness alone: `SelectColumnLayer.allowCrossColumnParallelization()` is `return selectColumn.isStateless();`. With the default `statelessSelectByDefault=true`, `DhFormulaColumn.isStateless()` returns `true`.
- So two Python-backed columns in one `update` can run on different threads at once, taking turns on the GIL. Python formulas in different tables can also run concurrently on update-graph threads. This is exactly the race the page's own A/B counter example relies on.
- The true statement is narrower: on a GIL build, a Python-backed formula or filter is never split across rows, so it never runs concurrently with itself.
- Suggested fix: "On a standard (GIL-enabled) build, Deephaven doesn't split a Python-backed formula or filter across threads, though different Python-backed columns or tables can still run concurrently."

**A2. `with_serial` doesn't serialize access to a shared resource (lines 20, 22, 148, 346)**
- The contract is scoped to a single expression. `ConcurrencyControl.java` says: "The expression will never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order."
- By default, serial selectables aren't ordered against each other. The same javadoc says: "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed."
- So two serial columns that call the same non-thread-safe library or write the same log file can still run concurrently. Line 141 already says so ("Other columns can still run at the same time").
- These places claim more than the contract gives:
  - Quick-reference row "Non-thread-safe library → `with_serial` → Forces single-threaded access" (line 22)
  - Quick-reference row "File I/O or logging → Serialize access to shared resource" (line 20)
  - Line 148 ("calls external functions that aren't safe to call from multiple threads simultaneously")
  - Key takeaway line 346 ("calls functions that aren't safe to run from multiple threads")
- Fix: say that `with_serial` protects one expression. When several expressions share a resource, they also need a barrier.

**A3. "on a single thread" overstates the contract (line 148)**
The javadoc promises only "never invoked concurrently with itself" plus row-set order. It never names a single thread. Replace with "never concurrently with itself, in row order". Line 213 ("only one thread processes the column at a time") is already correct.

**A4. Barriers only work within one `select`/`update`/`where` call (lines 21, 238, 240, 339, 347)**
- The page says barriers order "operations", and the quick-reference row reads "Multiple operations sharing state → Barriers". A reader will take "operation" to mean a table operation.
- In fact, barriers are resolved per call, and the declaring expression must come earlier in the same list:
  - `SelectAndViewAnalyzer` throws `"Respected barrier, " + barrier + ", is not defined for " + sc.getName()`.
  - `QueryTable.where` throws `"... respects barrier ... that is not declared by any filter so far."`
- A barrier can't order two separate `update` calls.
- Fix: say "column" or "filter" within a single call, and say the declaring expression must come first in the list.
- Related, and verified correct: "Each barrier can only be declared by one operation" (line 240). Selectables throw `"Duplicate barrier, ..."`; filters throw `"Filter Barriers must be unique!"`.

**A5. "Filter: Concurrency control works the same way for `Filter` as it does for `Selectable`" (line 135)**
- The contract differs. `ConcurrencyControl.java` says: "For a filter, serial acts as an absolute reordering barrier." Selectables get extra ordering only through implicit barriers.
- The javadoc also says a serial filter "will not evaluate additional rows or skip rows that future filters may eliminate."
- The page itself contradicts line 135 at line 234 ("the filter cannot be reordered with respect to other filters").
- Fix: say that the API is the same, but a serial filter also acts as an ordering barrier among the filters.

**A6. Breaking-change callout (line 9)**
- **Version:** the change is commit 8ea55b6a1d (DH-20714, #7331). `gradle.properties` at that commit has `deephavenBaseVersion=0.41.0`. So the versions are 0.41 and 0.40, not "41" and "40".
- **"Deephaven 40 and earlier assumed all formulas required sequential processing by default" is false.** Before the change, `DhFormulaColumn.isStateless()` still treated a formula as stateless when `Arrays.stream(params).allMatch(DhFormulaColumn::isImmutableType) && usedColumns...allMatch(isUsedColumnStateless)`. Many formulas were already parallelized.
  - Filters were a different case. `ConditionFilter.permitParallelization()` returned `QueryTable.STATELESS_FILTERS_BY_DEFAULT`, whose old default was false, so condition filters really were serial by default.
  - Suggested wording: "0.40 and earlier treated formulas as stateful unless the engine could prove otherwise, and treated condition filters as stateful."
- **"will now produce incorrect results"** is an absolute. It should be "can". Below the size thresholds, or on a GIL build, the result is often still correct.
- **The same "all formulas" claim appears at line 345.** Line 345 also sits right next to its own exception, the Python/GIL gate at line 75.

**A7. Implicit barriers section (lines 320–325)**
- `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is a Java field, not something a user sets. The property is `QueryTable.serialSelectImplicitBarriers`.
- The "Stateless mode" and "Stateful mode" labels don't exist in source. The property is a boolean whose default comes from another setting: `getBooleanWithDefault("QueryTable.serialSelectImplicitBarriers", !STATELESS_SELECT_BY_DEFAULT)`. It is false by default, and becomes true if a user sets `statelessSelectByDefault=false`. The page doesn't mention that second route.
- Implicit barriers apply to non-stateless columns (`if (QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS && !sc.isStateless())`). That matches "serial operations". Verified.
- Placement: this is configuration detail in a concept guide. Keep one sentence ("by default, serial columns aren't ordered relative to each other; you can configure the engine to add implicit barriers") and move the property and its default to `query-table-configuration.md`. That page already lists `serialSelectImplicitBarriers | false`.
- Source bug worth a separate follow-up: `ConcurrencyControl.java` javadoc says the property defaults "to the value of `QueryTable.statelessSelectByDefault`". The code uses its negation. Don't copy the javadoc wording into the doc.

**A8. `view`/`update_view`/`lazy_update` listed under "What does NOT get parallelized" (line 70)**
- Their formulas are deferred. They run later, on whichever thread reads the column, including a parallel `update`, `select`, or `where` downstream. A `view`/`update_view` formula can run again on every read.
- The engine refuses stateful columns in these operations precisely because it can't order that later evaluation:
  - `QueryTable.viewOrUpdateView` throws `"view and updateView cannot respect barriers"`.
  - With the default `STATELESS_SELECT_BY_DEFAULT`, it also throws `"A stateful column cannot safely be used in a view or updateView."`
- So "no parallel work when the table is created" is true, but "not parallelized" is the wrong mental model. Reframe as: evaluation is deferred, and may happen concurrently on whatever thread reads the value.
- "upfront" is also ambiguous. It doesn't say whether it means static creation, initialization, or update cycles.

**A9. `with_serial` restriction box (line 156)**
- Accurate under the default configuration: the exception above fires only `if (STATELESS_SELECT_BY_DEFAULT)`. That qualifier belongs in the configuration page, not this box.
- The box leaves out `lazy_update`. `QueryTable.lazyUpdate` has no guard, so it silently accepts a serial column without giving it any ordering guarantee. Either mention `lazy_update` here or drop it from line 70 for consistency.

**A10. Thresholds note (line 124)**
- The numbers check out:
  - `minimumParallelSelectRows` default is `1L << 22` = 4,194,304.
  - `parallelWhereRowsPerSegment` default is `1 << 16`.
  - The `where` test is `numberOfRows / 2 > PARALLEL_WHERE_ROWS_PER_SEGMENT`.
- "once a table crosses" is wrong for update cycles. `SelectColumnLayer` compares `totalSize = upstream.added().size() + upstream.modified().size()`, the size of the change, not the table. It also skips row-splitting when there are shifts (`!hasShifts`).
- There's one more exception: Table- or RowSet-typed results are always split (`resultTypeIsTableOrRowSet && totalSize > 0`).
- Placement: all of these are configuration values in a concept guide. Say "once there are enough rows to be worth splitting" and link to `query-table-configuration.md`. At f2ef483084 that page already explains the initial-versus-per-update distinction for `where`.

---

## B. Examples whose claimed runtime behavior doesn't match the execution path

**B1. Unserialized counter sample output (lines 180–190)**
- On a standard GIL build, a Python formula isn't split across rows, so each column runs start to finish in order (`doSerialApplyUpdate` → `doApplyUpdate`).
- Column A can't come out non-monotonic, but the sample shows A = 0, 1, 5, 4, 9, and the text points to "row 4 has A=4 after row 3 has A=5".
- Gaps and B ≠ A+1 are plausible, because A and B are stateless, so the columns can run concurrently (see A1).
- The pattern is only reproducible on a free-threaded build. Either say so, or change the sample so each column increases but the two columns interleave.
- The "fix" block also swaps the A/B example for a single `ID` column. It doesn't show the fix for the code above it: putting `with_serial` on both A and B would still not order A against B by default (A2 and A7).

**B2. Barrier example prose: "Without a barrier, both columns would start simultaneously, both read `counter = 0`" (line 244); "both columns would race" (line 280)**
- In this exact example, both columns are serial, so `allowCrossColumnParallelization()` is false for both.
- That makes `anyParallelColumns()` false, and `QueryTable.selectOrUpdate` picks `new ImmediateJobScheduler()`. The layers then run one after another on the calling thread, in list order.
- So they would *not* start at the same time, and wouldn't both read 0.
- The barrier is still needed for the *guarantee*: without it, nothing in the contract orders A before B.
- Fix: replace the claim that it would fail with "without the barrier, nothing guarantees A finishes before B starts."
- "Without `with_serial`, rows within each column would also race" has the same problem. It's a 10-row table far below the thresholds, on a GIL build, so it wouldn't race. The real point is "no row-order guarantee."

**B3. Multiple-barriers example (line 314)**
"A and B run in parallel" should be "can run in parallel". It depends on thread count and on `OperationInitializer.canParallelize()`, which is `numThreads > 1 && !isInitializationThread.get()`. The D and C ordering is correct: execution dependencies come from the layers of respected barriers.

**B4. Serial filters example (lines 219–232)**
The API is valid:
- `is_null` and `not_` exist in `deephaven.filters`.
- `Filter.with_serial` wraps `withSerial()`.
- `where` accepts `Sequence[Filter]`.

But `is_null` has no side effects, so `with_serial` does nothing useful in this example. Either say it only shows the syntax, or use a filter that actually has side effects.

---

## C. Classifications and completeness

- **"What gets parallelized" (lines 62–66) is incomplete.** `JobScheduler.iterateParallel` or `OperationInitializerJobScheduler` is also used by:
  - `UpdateBy` (`update_by`)
  - `RangeJoinOperation` (`range_join`)
  - `ConstructSnapshot` (parallel snapshot, `QueryTable.enableParallelSnapshot`)
  - `UnionSourceManager` (merge)
  - `RegionedColumnSourceManager` (source-table locations)

  At least `update_by` is user-facing and should be listed, or the list should say it isn't exhaustive.
- **"Operations waiting for dependencies" (line 72)** is not a kind of operation that goes unparallelized. It describes how the update graph schedules work, so it doesn't belong in the classification.
- **Sort (line 66):** verified.
  - `SortHelpers` checks `!QueryTable.PARALLEL_SORT || minimumSize <= 0 || sortSize < minimumSize`.
  - `minimumParallelSortRows` defaults to `1L << 20`, and `parallelSort` defaults to `true`.

  The property names are configuration detail in narrative prose; move them (see D).

---

## D. Configuration detail placed in concept-guide narrative

The property names and defaults are all correct, but a concept guide shouldn't carry them inline. They appear at:
- line 50: `PeriodicUpdateGraph.updateThreads` "greater than 1, which is the default". The default is `-1`, which becomes `availableProcessors()`. That is only greater than 1 on a machine with more than one core.
- line 66: sort properties
- lines 83 and 87: thread-pool properties. Both defaults verified as `-1` → `Runtime.getRuntime().availableProcessors()` (`ThreadHelpers.getOrComputeThreadCountProperty` and the `PeriodicUpdateGraph` constructor).
- line 124: thresholds
- line 126: stateless defaults, both `true`, verified
- lines 320–323: implicit barriers

**Proposed destination:** a single Configuration section at the end of the page that links to `../query-table-configuration.md`, which already lists every one of these. Rewrite the narrative sentences in plain terms, for example "once the table is large enough", "uses all cores by default", "assumes formulas are stateless by default".

---

## E. Verified accurate (with source)

- `Selectable.parse`, `.with_serial()`, `.with_declared_barriers()`, `.with_respected_barriers()` (single Barrier or sequence) are in `py/server/deephaven/table.py`. `Barrier` is in `deephaven/concurrency_control.py`. `Table.update` accepts `Selectable` or `Sequence[Selectable]`.
- Barrier semantics match the `Barrier` docstring: "the respecting filter/selectable will be executed entirely after the filter/selectable declaring the barrier."
- `with_serial` row order and filter reordering (lines 136, 213, 234) match the `ConcurrencyControl.java` contract.
- The "may still evaluate a non-parallelizable column out of order" part of the caution (line 77) matches the `SelectColumn.isParallelizable` javadoc: "Even if a column is not parallelizable, the engine may choose to evaluate it out-of-order."
- Running columns concurrently within one table (line 60) is verified. `UpdateScheduler.doKickOffWork` fires a layer when `!getLayerDependencySet().intersects(remainingLayers)`. It's conditional on `anyParallelColumns()` and the thread count, so "can" is correct.
- Running tables concurrently during updates (lines 50 and 87) is verified: `ConcurrentNotificationProcessor` is used when `updateThreads > 1`.
- Stateful partition filters (lines 329–331) are mostly accurate. In `PartitionAwareSourceTable.whereImpl`, serial filters go to `otherFilters` and get coalesced and filtered row by row. Non-serial partitioning filters are prioritized without any statelessness check.
  - Refinements: once *any* serial filter appears, every later partition filter is demoted too. Refreshing filters, and filters using `i`/`ii`, are never prioritized.
  - "Treats it as stateless" is loose. The engine doesn't mark it stateless; it just prioritizes it anyway.
- `randomGaussian(double, double)` and `randomInt(int, int)` exist in `io.deephaven.function.Random`.

---

## F. Links

- **Internal links:** every one resolves at f2ef483084, including the anchors:
  - `Selectable.md#with_serial`
  - `update.md#serial-execution`
  - `where.md#serial-execution`
  - `../../getting-started/crash-course/parallelization.md`
  - `Filter.md`
  - `Selectable.md`
- **Heads-up:** the current checkout of dhc-skills-chip (c68a9a9be8) lacks the crash-course page, `Selectable.md`, and `Filter.md`. Those links break if this page lands without the rest of the PR.
- **Suggested internal links:** `Barrier` (lines 137 and 240) and `ConcurrencyControl` (line 153) point to external pydoc. At f2ef483084, `docs/python/reference/query-language/types/Barrier.md` and `ConcurrencyControl.md` both exist; consider linking to those instead.
- **External links to check by hand:**
  - docs.deephaven.io pydoc (lines 137, 153, 240, 356)
  - python.org free-threading (line 75)
  - Oracle `Runtime.availableProcessors` (line 89)

---

## Author queries

- **AQ1 [Stateful partition filters, para 2]:** is `Date=today()` still the recommended example, given that the engine prioritizes non-serial partition filters regardless of the stateless setting? I couldn't find a source for that example.

## Follow-ups outside this file

- Fix the `ConcurrencyControl.java` javadoc's stated default for `serialSelectImplicitBarriers`. Source is `!STATELESS_SELECT_BY_DEFAULT`; the javadoc says it equals `statelessSelectByDefault`.
- Run the same review on the Groovy sibling. Items A1–A8 and B2 describe engine behavior and very likely appear there too.
