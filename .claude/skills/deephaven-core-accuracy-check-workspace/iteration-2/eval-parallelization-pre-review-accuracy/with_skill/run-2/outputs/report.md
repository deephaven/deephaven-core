# Accuracy review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857 snapshot, f2ef483084)

**Category:** Concept guide. It lives under `conceptual/`, and `ref-deephaven-doc-categories` calls out this exact page: its sidebar placement under Performance is for discoverability and doesn't change the category. So configuration detail belongs in a Configuration section or the configuration reference, not in the narrative.

**Scope:** Python file only, as you asked. The Groovy sibling `docs/groovy/conceptual/query-engine/parallelization.md` exists at f2ef483084 and was not reviewed. Most findings below are about engine behavior, so they probably apply there too. Treat it as a follow-up. No edits were made.

---

## High severity: wrong or misleading claims

### 1. The Quick reference rows prescribe `with_serial` alone where it isn't enough (lines 18, 20, 22)
The `withSerial` contract (`table-api/.../ConcurrencyControl.java`) says: *"The expression will never be invoked concurrently with itself"*. For selectables, it also says that when `SERIAL_SELECT_IMPLICIT_BARRIERS` is false (the default), *"no additional ordering between selectable expressions is imposed."*
- **Global counter → `with_serial`** is wrong whenever more than one column touches the counter. The page's own barrier example (lines 263–277) and the IMPORTANT box at line 283 say you need **both**.
- **File I/O or logging → `with_serial`** and **Non-thread-safe library → `with_serial`** have the same problem. `with_serial` only stops one expression from running concurrently with itself. Another column, another filter, or another table's update notification running on a different update thread can still call the same resource at the same time.
- The "Why" text "Forces single-threaded access" overstates the contract. The contract promises no self-concurrency, not single-threaded access.
- **The same claim appears in:**
  - "Choosing an approach" (line 338: "file I/O or logging")
  - Key takeaways (line 346: "calls functions that aren't safe to run from multiple threads")
  - The breaking-change callout (line 9: "unless you mark it with `with_serial`")

### 2. The breaking-change callout misstates what version 40 did and what users need to change (line 9)
- *"Deephaven 40 and earlier assumed all formulas required sequential processing"* is false. At `8ea55b6a1d~1` (the commit just before DH-20714 / #7331, first tagged in v41.0), `DhFormulaColumn.isStateless()` still inferred statelessness when the default was false: `Arrays.stream(params).allMatch(DhFormulaColumn::isImmutableType) && usedColumns...`. Pure column formulas were already stateless and parallelized. Also, "stateful" never meant "in order". The `isParallelizable` javadoc says *"the engine may choose to evaluate it out-of-order."*
- The callout leaves out the second half of the change. `SERIAL_SELECT_IMPLICIT_BARRIERS` defaults to `!STATELESS_SELECT_BY_DEFAULT`, so it also went from true to false in 41. Under 40, stateful columns implicitly ordered themselves against each other. Under 41, `with_serial` alone does not restore that. Users also need barriers, or `QueryTable.serialSelectImplicitBarriers=true`, or `statelessSelectByDefault=false`.
- "will now produce incorrect results" is too absolute. It should be "can".

### 3. The GIL caution contradicts the engine and the page's own counter example (line 75)
The text says Python-backed selectables are *"never run concurrently"* on a GIL build. That holds for splitting one column across rows: `FormulaColumnPython.isParallelizable()` / `DhFormulaColumn.isParallelizable()` return `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`, and `SelectColumnLayer` requires `sc.isStateless() && sc.isParallelizable()`.

It does not hold across columns. `SelectColumnLayer.allowCrossColumnParallelization()` returns only `selectColumn.isStateless()`, and Python formulas are stateless by default. So two Python columns in one `update` can run on different threads, with the GIL interleaving them. That interleaving is exactly what makes the unserialized A/B counter example (lines 175–190) race on a standard build. Without it, the example wouldn't race at all.

Python filters are handled correctly: `ConditionFilter.permitParallelization()` returns false on a GIL build. The same Python function can still be called concurrently from different tables' notifications.

### 4. "What does NOT get parallelized" misclassifies `view` / `update_view` / `lazy_update` (line 70)
It's true that no work happens up front. But the formulas run later, on whatever thread reads the column. That can be a parallel `update`/`select`/`where` or a chunked downstream read, so the formulas can run concurrently, and `view`/`update_view` can re-run them on every read.

The engine's own guard shows it treats these columns as the least safe place for stateful formulas, not a safe one. In `QueryTable.viewOrUpdateView` it throws `"view and updateView cannot respect barriers"`, and under the default `statelessSelectByDefault=true` it throws `"A stateful column cannot safely be used in a view or updateView."`

Listing these operations next to `with_serial` as "not parallelized" gives readers the wrong mental model. Reword it as "deferred: evaluated on the reading thread, which may be parallel". Also say when "upfront" applies, since static creation, refreshing initialization and update cycles differ.

### 5. The counter-example output and its explanation can't be reproduced (lines 180–190)
- *"row 4 has `A=4` after row 3 has `A=5`"*: out-of-order values in *adjacent rows of one column* can't come from this code.
  - On a GIL build, column A is never split across rows (see finding 3), so one thread fills A in row order and its values only increase.
  - On a free-threaded build, the split size is `Math.max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(total/threads))`, which is at least 4,194,304. Rows 3 and 4 always land in the same segment.
  - Out-of-order values can only show up at segment boundaries, or on a free-threaded build.
- *"`B` not following `A + 1`"* is presented as a symptom of the race, but no execution mode produces B = A+1. Columns are evaluated one layer at a time over all rows, not row by row across columns. Even fully serial execution gives B = A + N.
- The "fix" (lines 192–211) quietly turns the two-column example into a single `ID` column. It doesn't show how to fix the two-column case, which needs `with_serial` plus a barrier (see finding 1).

### 6. The barrier example's "without X" explanation doesn't match the engine (lines 244, 280)
- *"both read `counter = 0`, and produce overlapping, incorrect results"* is wrong on two counts:
  - The counter is shared, so concurrent columns would interleave their values; they wouldn't both start at 0.
  - In this exact example both columns are serial (not stateless), so `anyParallelColumns()` is false. `QueryTable` then uses `ImmediateJobScheduler`, which runs the two layers one after the other on the calling thread.
- By the javadoc contract the barrier is still what *guarantees* the order, so keeping it is correct. The prose should say "without the barrier, nothing guarantees A finishes before B" rather than describe a race that won't happen as written.
- *"Without `with_serial`, rows within each column would also race"* is false here. It's a Python UDF (not split across rows on a GIL build) and only 10 rows (below the 4.2M threshold). Per the contract it should say "the engine would be free to evaluate rows concurrently or out of order".

---

## Medium severity

7. **"Concurrency control works the same way for `Filter` as it does for `Selectable`" (line 135) is wrong.** The javadoc separates them. For a filter, *"serial acts as an absolute reordering barrier"*. For selectables, ordering between expressions depends on `SERIAL_SELECT_IMPLICIT_BARRIERS`. A serial filter also stops every later filter from being moved ahead as a partition filter (`PartitionAwareSourceTable.whereImpl`, `serialFilterFound`). Barriers themselves do behave the same way, so line 316 is fine.

8. **"When parallelization is safe by default" (line 93) suggests Deephaven detects stateless formulas.** It doesn't. With the default, `DhFormulaColumn.isStateless()` returns `true` for every formula, and `FormulaColumnPython.isStateless()` returns the default. Line 345 gets this right ("assumes"); line 93 should match it.

9. **"Implicit barriers" section (lines 320–323):**
   - "Stateless mode" and "Stateful mode" are not terms the engine uses.
   - The default is not fixed to false. It is `!QueryTable.statelessSelectByDefault`, so turning off stateless-by-default also turns implicit barriers on.
   - The logic applies to any non-stateless *selectable* (`SelectAndViewAnalyzer`: `SERIAL_SELECT_IMPLICIT_BARRIERS && !sc.isStateless()`), not to filters.
   - Separate follow-up for source: the `ConcurrencyControl.withSerial` javadoc says the property defaults "to the value of `QueryTable.statelessSelectByDefault`". The code uses the *negation*, so the javadoc is wrong.

10. **"What gets parallelized" is incomplete (lines 62–66).** Derived from the code that creates `OperationInitializerJobScheduler` / `UpdateGraphJobScheduler`:
    - `UpdateBy.java` and `RangeJoinOperation.java` also parallelize.
    - Table snapshots do too (`ConstructSnapshot`, `QueryTable.enableParallelSnapshot`, which the configuration page already documents).
    - `update_by` is the one users will care about most. **AQ1:** should the list name it (and `range_join`)?

11. **"Choosing an approach" names "cumulative calculations" as a `with_serial` use case (line 338).** On a ticking table, `SelectColumnLayer` only re-evaluates added and modified rows. A cumulative value kept in a global drifts with modifications, removals and shifts, even with `with_serial`. `update_by` (for example `cum_sum`) is the right tool. Either drop this case or qualify it as static-only.

12. **"Serialization processes rows one at a time, in order, on a single thread" (line 148).** "On a single thread" goes beyond the contract, which only promises no self-concurrency; different chunks or cycles can run on different threads. "Without it, parallel execution produces incorrect results" should be "can produce".

---

## Low severity

13. **Line 50:** "`PeriodicUpdateGraph.updateThreads` being greater than 1, which is the default" is imprecise. The default is `-1`, which resolves to `Runtime.availableProcessors()` (`PeriodicUpdateGraph` constructor). That is 1 on a single-core host. (See the placement note below.)
14. **Line 124 NOTE:**
    - The select/update threshold uses `>=` and applies to *added + modified rows per update*, not table size. Shifts turn off splitting across rows in updates (`!hasShifts`). Formulas that return a Table or RowSet ignore the threshold entirely.
    - The `where` figure matches `numberOfRows / 2 > PARALLEL_WHERE_ROWS_PER_SEGMENT`.
15. **Line 87:** updates parallelize "just like during initialization", except that shifted updates aren't split across rows. "Update Graph Processor Thread Pool" isn't a name in source; the thread group is `PeriodicUpdateGraph-updateExecutors`. **AQ2:** is this informal name intended?
16. **Line 36 comment:** "adds a row every second" is the same wording commit f2ef483084 fixed in the crash course. `TimeTable.refresh` adds however many rows elapsed time calls for.
17. **Serial filters example (lines 219–232):** the syntax is valid (`deephaven.filters.is_null`/`not_` exist, and `Filter.withSerial` has a default implementation). But the prose asks for explicit `Filter` objects "when a filter has stateful side effects" and then uses side-effect-free built-ins, so `with_serial` does nothing here. With 1000 rows it's also below the parallel-`where` threshold.
18. **Stateful partition filters (lines 329–331):** broadly supported by `PartitionAwareSourceTable.isPrioritizablePartitioningFilter`, which has no statefulness check.
    - A serial filter also blocks prioritization of every *later* filter.
    - Refreshing filters, filters using `i`/`ii`/`k`, and reindexing filters are never prioritized.
    - "The engine treats it as stateless" is loose. It's reordered no matter how stateful it is, but its evaluation still follows the default.
19. **Line 156:** accurate at the default configuration. For Python, the plainer reason is that `view`, `update_view` and `lazy_update` only accept formula strings (`table.py` signatures), so a `Selectable` can't be passed at all. `lazy_update` is missing from the list; on the Java side it has no guard.

---

## Configuration detail placement (Concept guide)
Property names and defaults sit inline in the narrative at lines 50, 66, 83, 87, 124 and 320–323. That includes the Java field name `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` in a Python page. I'd move them to one Configuration section at the end, or point to `../query-table-configuration.md`, which already documents all of them at f2ef483084 with matching defaults. Then rewrite the sentences in plain terms, for example "once the table is large enough to be worth splitting". Line 126 already follows this pattern.

---

## Links
- **Internal links:** all resolve at f2ef483084, and the anchors exist (`Selectable.md#with_serial`, `update.md#serial-execution`, `where.md#serial-execution`). `Selectable.md`, `Filter.md` and `crash-course/parallelization.md` are missing in the current checkout but present at the PR commit.
- **Suggested internal replacements:** `Barrier` and `ConcurrencyControl` link to external pydoc (lines 137, 153, 240, 356). At f2ef483084, `reference/query-language/types/Barrier.md` and `ConcurrencyControl.md` exist; I confirmed with `git ls-tree`.
- **Optional new links, confirmed to exist:** `reference/table-operations/create/timeTable.md`, `create/emptyTable.md`, `filter/tail.md`, and `update-by-operations/updateBy.md` (the last is relevant if finding 10 or 11 is taken up).
- **External links to check by hand:** the pydoc anchors, the Python free-threading how-to, and the Java 11 `Runtime.availableProcessors` page.

---

## Verified accurate (with source)
- **Property names and defaults, `QueryTable.java`:**
  - `minimumParallelSelectRows` = `1L << 22` (4,194,304)
  - `minimumParallelSortRows` = `1L << 20`
  - `parallelSort` = true
  - `parallelWhereRowsPerSegment` = `1 << 16`
  - `statelessSelectByDefault` = true
  - `statelessFiltersByDefault` = true
  - `serialSelectImplicitBarriers` exists
- **Thread pools:**
  - `OperationInitializationThreadPool.threads` = -1 and `PeriodicUpdateGraph.updateThreads` = -1, both resolving to `availableProcessors`.
  - `ThreadHelpers.getOrComputeThreadCountProperty` and the `PeriodicUpdateGraph` constructor implement that fallback.
  - The server wires both defaults in `UpdateGraphModule`.
- **Update threads:** independent notifications run concurrently via `ConcurrentNotificationProcessor` when `updateThreads > 1`.
- **Barriers** (`SelectAndViewAnalyzer`, which throws on a duplicate barrier; javadoc says "declared by at most one"):
  - A barrier can be declared once and respected by many.
  - The multiple-barriers execution order is correct.
  - Line 285: reading after a barrier doesn't need `with_serial`.
- **Line 77:** "may still evaluate a non-parallelizable column out of order" matches the `SelectColumn.isParallelizable` javadoc.
- **Serial filter semantics (line 234)** match the javadoc.
- **Python APIs:** `Selectable.parse`, `.with_serial()`, `.with_declared_barriers`, `.with_respected_barriers` (which accepts a list), `deephaven.concurrency_control.Barrier`, and `update`/`select` accepting `Selectable` lists.
- **Query strings:** a `' '` char literal is left alone by `TimeLiteralReplacedExpression` (length ≤ 1). `i` in `update` on an append-only `time_table` passes `validateSafeForRefresh`.
- **Version:** stateless-by-default arrived in DH-20714 (#7331), first tagged `v41.0`. The "41+" boundary is correct; only the description of 40 is wrong (finding 2).

## Author queries
- **AQ1** [What gets parallelized]: should `update_by`, `range_join` and snapshots be listed?
- **AQ2** [Query phases and thread pools, para 3]: is "Update Graph Processor Thread Pool" the intended user-facing name?

## Completeness sweep
I re-grepped the file for each finding:
- **`with_serial`-alone sufficiency:** lines 9, 18, 20, 22, 338, 346.
- **"not parallelized" deferred evaluation:** line 70 only. Line 156's reason is correct.
- **"never concurrently":** line 75 only.
- **Race description:** lines 180, 190, 244, 280.
- **Filter vs. Selectable equivalence:** line 135 only.

No further occurrences turned up in the Python file. The Groovy sibling was not swept because you scoped it out.