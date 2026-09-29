# Accuracy review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857 PR snapshot, commit f2ef483084)

I checked it against source in `/Users/margaretkennedy/dhc-skills-chip`, which is on HEAD `373cece093`. The snapshot file is byte-identical to that path at f2ef483084. This was a review only; I edited nothing.

**Category:** Concept guide, because it lives under `conceptual/`. The sidebar puts it under Performance for discoverability, but that doesn't change the category. So the placement rule applies: configuration detail belongs in one configuration section or behind a link, not scattered through the narrative.

**Scope note:** You put the Groovy sibling out of scope. It does exist (`docs/groovy/conceptual/query-engine/parallelization.md` at f2ef483084), and I didn't review it. Most findings below are claims the two pages probably share, especially 1, 2, 5, 6 and 9. Normally the skill would fix both pages in the same pass, so run the same sweep on the Groovy page before merging.

---

## A. Wrong or overstated claims (fix before merge)

### 1. Quick reference and "use `with_serial` when…" rows promise more than `with_serial` gives
Source is `table-api/.../ConcurrencyControl.java`, `withSerial()`:
- "The expression will never be invoked concurrently with itself."
- "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed."

`QueryTable.java` sets that flag to `!STATELESS_SELECT_BY_DEFAULT`, which is false by default.

`with_serial` only protects a resource when a single column touches it. Several rows prescribe it on its own for resources that more than one column can touch:
- **Line 18, "Global counter → `with_serial`".** The page's own first counter example has two columns (A and B) calling the same counter. Putting `with_serial` on both would still let A and B interleave.
- **Line 20, "File I/O or logging → `with_serial`".**
- **Line 22, "Non-thread-safe library → `with_serial` / Forces single-threaded access".** "Single-threaded access" is wrong. Two serial columns can call the library at the same time.
- **Line 21, "Multiple operations sharing state → Barriers or implicit barriers".** Barriers alone don't serialize rows *within* each column. Implicit barriers only exist between columns that are already `with_serial` and only with a non-default property (`SelectAndViewAnalyzer.java`: `if (QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS && !sc.isStateless())`). So this remedy is really "`with_serial` plus barriers" or "`with_serial` plus implicit barriers."

The same shortened claim repeats in:
- line 213 ("global state updates happen sequentially without race conditions")
- line 338 ("Choosing an approach": "file I/O or logging")
- line 346 (Key takeaways: "calls functions that aren't safe to run from multiple threads")

**Fix:** qualify each as "when only this column uses it," or pair it with barriers. The body already says it correctly on line 144 and in the IMPORTANT box on line 283; the summaries dropped that qualification.

### 2. The GIL caution is wrong: "on a standard (GIL-enabled) build, they're never run concurrently" (line 75)
On a GIL build, Python-backed columns are blocked only from being split *across rows*:
- `DhFormulaColumn.isParallelizable()` returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`.
- `FormulaColumnPython.isParallelizable()` returns `isPythonFreeThreaded()`.

Parallelism *across columns* is gated only on statelessness:
- `SelectColumnLayer.allowCrossColumnParallelization()` returns `selectColumn.isStateless()`.
- For Python columns, `isStateless()` returns `QueryTable.STATELESS_SELECT_BY_DEFAULT`, which is true.

So `anyParallelColumns()` is true, and two Python columns in one `update` get submitted to the parallel job scheduler (`QueryTable.java` ~1825, `SelectOrUpdateListener.java` line 68–72). They run on separate threads and interleave at GIL granularity. Separate tables that use Python formulas or filters can also update at the same time on the update-graph threads.

For filters, the part about row segmentation is correct: `ConditionFilter.permitParallelization()` returns false when Python is involved and the build isn't free-threaded.

**Fix:** "On a GIL build, Deephaven doesn't split a Python-backed formula or filter's rows across threads. Different Python columns, and different tables, can still be evaluated concurrently." Line 77 restates "not running concurrently" and needs the same correction.

### 3. `view` / `update_view` / `lazy_update` listed as "What does NOT get parallelized" (line 70)
All three build deferred column sources (the `VIEW_EAGER` / `VIEW_LAZY` branches in `SelectAndViewAnalyzer`). Their formulas run on whichever thread reads the column. That reader can be a parallel `update`/`select`, a segmented `where`, or a concurrent downstream table, so the formula can run concurrently, and repeatedly for the same row.

The engine's own guard confirms this. In `QueryTable.viewOrUpdateView` it throws "view and updateView cannot respect barriers", and, under the default, "A stateful column cannot safely be used in a view or updateView."

"No work computed up front" is true. Filing these as "not parallelized" gives readers the wrong model and suggests they're safe for stateful formulas. "Upfront" is also ambiguous: does it mean static creation, initialization of a dynamic table, or each update cycle?

**Fix:** "These don't compute values when the table is created or updated. Their formulas run later, on whatever thread reads the column, possibly in parallel. Don't use stateful formulas in them."

### 4. "What gets parallelized" is incomplete, and one entry is only true at initialization (lines 62–66)
- **Sort is parallel only at initialization.** `SortHelpers.parallelizableOperationInitializer()` says: "Update graph refresh threads have the poisoned ExecutionContext … a sort listener running there sorts serially."
- **The list omits other operations that also parallelize:**
  - `update_by` (`UpdateBy.java` ~308)
  - `range_join` (`RangeJoinOperation.java` ~258)
  - snapshotting (`ConstructSnapshot.java`, `QueryTable.enableParallelSnapshot`)

**Fix:** for a concept guide, present these as examples ("for example…") rather than try to list everything, and note that sort only parallelizes its initial sort.

### 5. The small-table NOTE contradicts the page's multiple-barriers example (line 124)
Line 124 says that below the thresholds "Deephaven evaluates the formula on a single core." That is true only *within* one column:
- `SelectColumnLayer` applies `MINIMUM_PARALLEL_SELECT_ROWS` only to row splitting.
- Cross-column scheduling has no size threshold.
- Columns that return a Table or RowSet ignore the minimum entirely (`resultTypeIsTableOrRowSet`).

The page itself relies on this at line 314 ("A and B run in parallel" on a 10-row table).

The unconditional wording at lines 58, 83 and 87 has the same tension: "dividing the rows among CPU cores" and "just like during initialization." Row splitting during updates depends on that cycle's added plus modified rows (`totalSize = upstream.added().size() + upstream.modified().size()`) and on there being no shifts.

The threshold values themselves are correct:
- 1L<<22 = 4,194,304, so "about 4.2M" is right.
- `where` uses `numberOfRows / 2 > PARALLEL_WHERE_ROWS_PER_SEGMENT` (1<<16), so "about 131,072" is right.

### 6. "Concurrency control works the same way for `Filter` as it does for `Selectable`" (line 135)
It doesn't. From the `ConcurrencyControl.withSerial()` javadoc:
- For a filter, "serial acts as an absolute reordering barrier."
- For selectables, no ordering between expressions is imposed unless implicit barriers are on.

Implicit barriers apply only to selectables. The page's own partition-filter section (lines 327–331) is another difference.

**Fix:** "Filters use the same methods; a serial filter additionally can't be reordered relative to other filters." Line 234 already says this correctly.

### 7. Barriers are described as ordering "operations"; they only work within one call (lines 19, 21, 240, 339, 347)
Barriers are resolved per `select`/`update`/`where` call. `SelectAndViewAnalyzer` builds `barrierToLayerIndex` for that analyzer only, and throws "Respected barrier, … is not defined" otherwise. The javadoc adds: "It is an error to respect a barrier that has not already been defined per the natural left to right ordering."

"Use barriers when one operation must complete before another starts" (line 347), and the Quick-reference row "Multiple operations sharing state," can read as ordering two separate table operations. Barriers can't do that.

**Fix:** say "column or filter in the same `update`/`select`/`where`," and mention that the declaring column must come first in the list.

### 8. "Serialization processes rows one at a time, in order, on a single thread" (line 148)
The contract says "never invoked concurrently with itself" and "rows are evaluated sequentially in row set order." It never mentions a single thread; across update cycles the work can land on different threads. This is the skill's own named example of an overstated guarantee. The Quick-reference "single-threaded access" (line 22) repeats it.

**Fix:** "…one at a time, in order, never concurrently with itself." Line 213 already words it correctly.

### 9. The implicit-barriers default is described incompletely (lines 320–323)
`QueryTable.java` sets `serialSelectImplicitBarriers` with a default of `!STATELESS_SELECT_BY_DEFAULT`. So it isn't a fixed "stateless mode (default)": it turns on automatically when a user sets `statelessSelectByDefault=false`, which restores the pre-41 behavior. It also applies only to serial *selectables*, not filters. Both lines say "serial operations"; they should say serial columns.

Separately, the `ConcurrencyControl.withSerial()` javadoc says the property defaults to "the value of `QueryTable.statelessSelectByDefault`." That contradicts the code, which uses the negation. This is a source-javadoc bug to file as a follow-up, not a doc change.

### 10. The counter race example doesn't match what the engine does (lines 175–190)
The engine computes columns layer by layer: all of A, then B, or both at once on separate threads. It never runs row by row across columns. So even fully serial execution gives `B = A + 5,000,000`, not `A + 1`. "B not following A + 1" isn't a symptom of parallelism.

"Gaps (no 10-19 visible)" contradicts the sample table, which contains exactly the values 0–9 once each. And on the default GIL build, A's rows aren't split across threads (finding 2). So A running out of order within itself (5 then 4) would need a rare lost-update race. The realistic symptom is A and B interleaving.

**Fix:** redo the explanation below the table so it describes interleaved, non-contiguous values in A and B. Drop the `A + 1` expectation.

### 11. Barrier example: the "without X" claims overstate the example (lines 244, 280)
Both columns are `with_serial`, so neither is stateless and `anyParallelColumns()` is false. For this static table the engine then uses `ImmediateJobScheduler` and computes A before B on one thread even without the barrier. The 10 rows are also below every threshold.

By contract the barrier *is* still required: without implicit barriers, `with_serial` promises no ordering between columns. So the example is valid, but three sentences go too far:
- "both columns would start simultaneously, both read `counter = 0`"
- "Without the barrier, both columns would race"
- "rows within each column would also race"

**Fix:** reword along the lines of "without the barrier, nothing guarantees A finishes before B starts; without `with_serial`, nothing guarantees row order."

### 12. Partition filters (lines 329–331) are mostly right, with two omissions
`PartitionAwareSourceTable.whereImpl` confirms that a non-serial partition filter is prioritized and evaluated per location regardless of `statelessFiltersByDefault`, because the check is `isSerial()` and not statelessness. Two gaps:
- **One serial filter disables all later prioritization.** From the source: "once we've found a serial filter, then we cannot prioritize any filter". So a serial filter *anywhere earlier* in the list also stops later partition filters from being evaluated per location.
- **"Evaluate it on all rows of the table" is too broad.** A serial filter evaluates the rows that survive earlier filters (javadoc: "exactly the set of rows as if any prior filters were applied").

### 13. Code comment "Create a live table that adds a row every second" (line 35)
`TimeTable.refresh` works out the row count from elapsed time, so one refresh can add zero rows or several. Commit f2ef483084 fixed this same comment in the Crash Course but left it here. Use the same wording: "rows arriving at one-second intervals."

---

## B. Verified correct

| Claim | Source |
|---|---|
| Deephaven 41 changed the default to parallel | Commit `8ea55b6a1d` (DH-20714). The first tag containing it is `v41.0`; the `v0.40.x` tags don't contain it. |
| `statelessSelectByDefault` / `statelessFiltersByDefault` default to true | `QueryTable.java` 379, 388 |
| `minimumParallelSortRows` default 1<<20 ("about 1 million"); `parallelSort=false` disables | `QueryTable.java` 344, 353 |
| `OperationInitializationThreadPool.threads` default -1 means all cores | `OperationInitializationThreadPool.java` 30; `ThreadHelpers.getOrComputeThreadCountProperty` |
| `PeriodicUpdateGraph.updateThreads` default -1 means `availableProcessors()`; >1 uses `ConcurrentNotificationProcessor` | `PeriodicUpdateGraph.java` 55, 140–157 |
| Two columns with no dependency between them can run concurrently (line 60) | `SelectAndViewAnalyzer`, `SelectColumnLayer` |
| `with_serial` can't be used with `view`/`update_view` (line 156) | Engine rejects it under the default config. In Python, `view`/`update_view` (and `lazy_update`) accept only strings anyway (`table.py` 1389, 1408, 1427). |
| Each barrier is declared at most once; many columns can respect it | "Duplicate barrier" exception; javadoc "declared by at most one filter" |
| Respecting columns run "entirely after" the declaring one | `ConcurrencyControl` javadoc |
| A serial filter can't be reordered; rows are evaluated in order | `withSerial()` javadoc |
| APIs exist as used | `Selectable.parse`, `with_serial`, `with_declared_barriers`, `with_respected_barriers` (these take a single barrier or a sequence), `deephaven.concurrency_control.Barrier`, `deephaven.filters.is_null`/`not_`, `Filter.with_serial`, `empty_table`/`time_table` exported from `deephaven`, `randomGaussian`, `randomInt`, `today()` |
| Free-threaded build needs no other configuration | Detection is `PyLib.getPythonVersion().contains("free-threading")`; stateless is already the default |

---

## C. Links

- **Internal links:** all resolve at f2ef483084. This includes `crash-course/parallelization.md`, `types/Selectable.md#with_serial`, `types/Filter.md`, `update.md#serial-execution`, `where.md#serial-execution`, `../dag.md`, `../query-table-configuration.md`, `./engine-locking.md`, and the six in-page anchors. Three of these targets don't exist on the current HEAD, so they're new in the PR; keep them in the same merge.
- **Suggested internal links (targets confirmed at f2ef483084):**
  - `Barrier` and `ConcurrencyControl` currently link to external pydoc. Internal pages exist: `reference/query-language/types/Barrier.md` and `ConcurrencyControl.md`.
  - `empty_table` → `reference/table-operations/create/emptyTable.md`
  - `time_table` → `create/timeTable.md`
  - `tail` → `filter/tail.md`
  - If finding 4 adds `update_by`/`range_join`: `update-by-operations/updateBy.md`, `join/range-join.md`
- **External links to check by hand:** the pydoc `deephaven.concurrency_control` anchors, the docs.python.org free-threading how-to, and the Oracle `Runtime.availableProcessors` javadoc.

---

## D. Where the configuration detail should go
Property names and defaults are injected into the narrative at lines 50, 66, 83, 87, 124, 126 and 320–323. All the `QueryTable.*` values are already in `query-table-configuration.md`: where/select/sort thresholds, `stateless*ByDefault`, `serialSelectImplicitBarriers`. The two thread-pool properties are not on that page.

When you fix findings 4, 5 and 9, write the narrative at the concept level ("once a table is large enough to be worth splitting") and put the exact properties and values in one Configuration section at the end, linked to that reference. Don't add more inline parentheticals.

---

## E. Questions for the author
- **AQ1 [What does NOT get parallelized, bullet 3]:** "Operations waiting for dependencies" doesn't correspond to any class of operation I can find in source. Should it be removed, or reworded to describe notification scheduling?
- **AQ2 [Serial filters example]:** `is_null(...).with_serial()` has no side effects, and 1000 rows never parallelize anyway. Is this meant as a syntax demo only? If so, should the lead-in drop "when a filter has stateful side effects"?

## F. Duplicate-claim sweep (Python file)
I re-checked every finding across the prose, the Quick reference, "Choosing an approach," Key takeaways and code comments. All repeats found are listed inside each finding above. Nothing new turned up. The Groovy sibling still needs the same sweep.

Two follow-ups outside the doc:
- Fix the `ConcurrencyControl.withSerial()` javadoc default (finding 9).
- Check whether other pages copy the "never run concurrently on GIL builds" claim.