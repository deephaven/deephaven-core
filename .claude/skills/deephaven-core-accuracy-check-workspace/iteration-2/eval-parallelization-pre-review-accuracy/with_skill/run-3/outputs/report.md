# Accuracy review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857, commit f2ef483084)

**Category:** Concept guide (`conceptual/`). Its sidebar spot under Best practices → Performance is there to help readers find it and doesn't change the category. So the configuration-placement rules apply, and any fix that adds detail should keep property names out of the narrative.

**Scope:** Python file only, as you asked. The Groovy sibling `docs/groovy/conceptual/query-engine/parallelization.md` exists. The skill normally requires checking it too, and most shared-claim findings below probably apply there, but I didn't review it. I verified the snapshot against source at `c68a9a9be8`. It is byte-identical to the file at `f2ef483084`, and I checked links against that commit.

---

## 1. Wrong or overstated claims

### 1.1 Quick reference rows prescribe `with_serial` alone for shared resources (lines 18, 20, 22)
The rows for "Global counter", "File I/O or logging" and "Non-thread-safe library" say `with_serial` alone is enough. The contract doesn't support that:

- `ConcurrencyControl.withSerial` javadoc (`table-api/.../ConcurrencyControl.java`): "The expression will never be invoked concurrently with itself." It adds: "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed."
- `QueryTable.java`: `SERIAL_SELECT_IMPLICIT_BARRIERS = ...getBooleanWithDefault("QueryTable.serialSelectImplicitBarriers", !STATELESS_SELECT_BY_DEFAULT)`, which is **false** by default.

If more than one column touches the counter, file or library, `with_serial` doesn't stop those columns running at the same time. That takes a barrier (or implicit barriers). The page says this itself at lines 144 and 283 ("you typically need **both**"), and the table drops it.

- **Line 22, "Forces single-threaded access":** this goes beyond the contract, which only says the expression is never invoked concurrently with itself.
- **Line 21, "Multiple operations sharing state → Barriers or implicit barriers":** also wrong. Barriers "do not affect concurrency" (javadoc for `withDeclaredBarriers`), so rows inside a column that mutates shared state still need `with_serial`. Implicit barriers only exist between serial selectables (`SelectAndViewAnalyzer.java`: `if (QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS && !sc.isStateless())`). "Implicit barriers" therefore also means `with_serial`, and they're off by default.
- **Fix:** qualify each row as single-column only, or prescribe `with_serial` plus barriers when more than one column shares the resource.

The same "`with_serial` is enough" claim appears in several other places:
- Line 148 ("calls external functions that aren't safe to call from multiple threads").
- Line 213 ("global state updates happen sequentially without race conditions"). This only holds if no other column touches that state.
- Line 338 ("file I/O or logging").
- Line 346 (Key takeaways: "calls functions that aren't safe to run from multiple threads").

### 1.2 "What does NOT get parallelized" lists deferred operations (line 70)
`view`, `update_view` and `lazy_update` all produce a `ViewColumnSource` (`AbstractFormulaColumn.getViewColumnSource`). Their formulas run later, on whichever thread reads the column. When the reader is a parallel `update`, `select` or `where`, or a parallel snapshot, the formula runs concurrently. A `view`/`update_view` formula can also run again on every read of the same row.

The engine's own guard in `QueryTable.viewOrUpdateView` confirms this: "An updateView can fetch things in any order; therefore we cannot allow it to be stateful", followed by `throw new IllegalArgumentException("A stateful column cannot safely be used in a view or updateView.")`. A few lines earlier it also throws `"view and updateView cannot respect barriers"`.

"Not computed upfront" is true. Putting these operations under "not parallelized" gives readers the wrong picture, and it suggests they're safe for stateful formulas. "Upfront" is also ambiguous: it could mean static tables, initialization, or update cycles.
- **Fix:** move them out of this list. Say they compute nothing when the table is created or updated, and that their formulas run on the reader's thread, possibly concurrently and repeatedly.

### 1.3 The GIL caution claims Python code is "never run concurrently" (line 75)
On a GIL build, `DhFormulaColumn.isParallelizable()` and `FormulaColumnPython.isParallelizable()` return false, so a Python column's rows aren't split across threads. Cross-column scheduling is controlled by a different flag, though:
- `SelectColumnLayer.allowCrossColumnParallelization()` returns `selectColumn.isStateless()`.
- `FormulaColumnPython.isStateless()` returns `QueryTable.STATELESS_SELECT_BY_DEFAULT`, which is true.

So two Python-backed columns in one `update` can run on different threads at the same time, interleaving at GIL switch points. The same Python function can also run concurrently in different tables through across-table parallelism.

The accurate statement: on GIL builds, a Python-backed column or filter isn't split across rows. It can still run concurrently with *other* columns or tables. That's also why the two-column counter example (line 175) actually races.

### 1.4 The breaking-change callout overstates Deephaven 40's behavior (line 9)
Line 9 says Deephaven 40 "assumed all formulas required sequential processing by default". At `v0.40.9`, `DhFormulaColumn.isStateless()` still returned true for any formula whose params are immutable and whose input columns are stateless:

```java
return Arrays.stream(params).allMatch(DhFormulaColumn::isImmutableType)
        && usedColumns.stream().allMatch(this::isUsedColumnStateless) ...
```

So plain column math like `Price * Quantity` was already parallelizable in 40. What changed in 41 (`8ea55b6a1d`, DH-20714; first tag containing it is v41.0.0) was two things:
- The `statelessSelectByDefault` and `statelessFiltersByDefault` defaults went from `false` to `true`.
- `serialSelectImplicitBarriers` therefore went from `true` to `false`.

The second change also breaks code: in 41+, code that put `with_serial` on several columns and relied on implicit barriers needs explicit barriers. The callout only tells readers to add `with_serial`.

### 1.5 The counter example output can't be reproduced, and its expectation is wrong (lines 180–190)
- **Out-of-order `A` values** ("row 4 has `A=4` after row 3 has `A=5`") need column `A` to be split across rows. On the default GIL build, Python columns are never split (1.3), so `A` runs top to bottom on one thread and only increases. Gaps are possible, reordering isn't.
- **"`B` not following `A + 1`"** is the wrong expectation. The engine runs one column at a time, not one row at a time. Even fully serial and ordered, you'd get `A` = 0…N-1 and `B` = N…2N-1, never `B = A + 1`.
- **"gaps (no 10-19 visible)"** doesn't match the table, which shows every value from 0 to 9 across `A` and `B`, and there are 5 million rows.
- **The "fix" (lines 194–211) drops a column.** It swaps the two-column `A`/`B` query for a one-column `ID` query, so it doesn't fix the code shown. Fixing the two-column version takes `with_serial` plus a barrier, which the next section shows.

### 1.6 The barrier example describes behavior that doesn't happen (lines 244, 280)
- **Line 244, "Without a barrier, both columns would start simultaneously, both read `counter = 0`":** with `with_serial` on both columns, neither is stateless. `analyzer.anyParallelColumns()` is then false, and `QueryTable` picks `ImmediateJobScheduler`, so the columns run one after the other in practice.
- **Why the barrier is still needed:** the contract doesn't *guarantee* that order when implicit barriers are off. Say "without the barrier there is no ordering guarantee between the columns". Don't describe a specific race.
- **Line 280, "Without `with_serial`, rows within each column would also race":** with 10 rows (below `minimumParallelSelectRows`) and a Python UDF on a GIL build, rows wouldn't be split. `with_serial` is what guarantees row order; the `SelectColumn.isParallelizable` javadoc says the engine "may choose to evaluate it out-of-order". It isn't preventing a race that actually happens here. Say "no row-order guarantee" instead.

### 1.7 "Concurrency control works the same way for `Filter` as it does for `Selectable`" (line 135)
It doesn't. According to the `withSerial` javadoc:
- For a filter, "serial acts as an absolute reordering barrier".
- For selectables, ordering against other expressions depends on `SERIAL_SELECT_IMPLICIT_BARRIERS`, which is off by default.

Implicit barriers only apply to selectables. The API methods are the same; the ordering semantics are different.

### 1.8 Barriers only work within a single operation (lines 137, 240, 316, 339, 347, 19)
The page says "one operation declares… another respects" and "one operation must complete before another starts". Barriers order the column or filter expressions *inside one* `update`, `select` or `where` call. They don't order separate table operations. The `withRespectedBarriers` javadoc says: "It is an error to respect a barrier that has not already been defined per the natural left to right ordering of filters/selectables at the operation level."

`SelectAndViewAnalyzer` enforces this with `throw new IllegalArgumentException("Respected barrier, " + barrier + ", is not defined for " + sc.getName())`. The page never states the left-to-right rule. The examples happen to follow it.

### 1.9 Update-cycle parallelism isn't "just like during initialization" (line 87; also lines 66 and 124)
- **Row splitting in `select`/`update`** during updates is based on the size of the change, not the table. `SelectColumnLayer` uses `totalSize = upstream.added().size() + upstream.modified().size()` and requires `!hasShifts`.
- **`where`** has the same per-update behavior. `query-table-configuration.md` already says a large table getting small ticks may not parallelize.
- **`sort`** is parallelized only at initialization. The `SortHelpers.parallelizableOperationInitializer` javadoc says: "Update graph refresh threads have the poisoned ExecutionContext… a sort listener running there sorts serially." Line 66 doesn't say this.

### 1.10 The initialization description is overstated (line 83)
Line 83 says each operation's initial result is computed "dividing the rows among CPU cores" and is "handled by the Operation Initialization Thread Pool". Only operations that support parallel work and are above their thresholds split rows, and the main work runs on the calling thread while the pool takes the parallel sub-tasks. "Operation Initialization Thread Pool" and "Update Graph Processor Thread Pool" (lines 83, 87) aren't names in source:
- The classes are `OperationInitializationThreadPool` and `PeriodicUpdateGraph` (thread group `PeriodicUpdateGraph-updateExecutors`).
- "Update Graph Processor" is the old name; no `UpdateGraphProcessor` class exists in the tree.

### 1.11 The page implies the engine detects statelessness (line 93)
"Deephaven parallelizes operations that are **stateless**" reads as if the engine checks for statelessness. It doesn't: with the default config, `DhFormulaColumn.isStateless()` returns `true` without looking at the formula. Line 345 says it correctly ("assumes"). Line 93 should say the same, so readers know that telling the engine is their job.

### 1.12 The implicit-barriers section mixes up names and misses a dependency (lines 320–323)
- It mixes the Java field `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` with the property `QueryTable.serialSelectImplicitBarriers`. Users only set the property.
- "Stateless mode" and "Stateful mode" aren't source terms. The default of `serialSelectImplicitBarriers` depends on `statelessSelectByDefault` (`!STATELESS_SELECT_BY_DEFAULT`). If a reader sets `statelessSelectByDefault=false`, which line 126 invites, implicit barriers turn **on**. The page doesn't say so.
- **Source-side follow-up (not a doc defect):** the `ConcurrencyControl.withSerial` javadoc says the property defaults "to the value of `QueryTable.statelessSelectByDefault`". That's the inverse of the code. The `QueryTable` field javadoc and the code agree on `!`.

### 1.13 The "What gets parallelized" list is incomplete (lines 62–66)
Read as a classification, the list misses user-facing operations that use `OperationInitializerJobScheduler` or `UpdateGraphJobScheduler`:
- `update_by` (`UpdateBy.java`)
- `range_join` (`RangeJoinOperation.java`)
- partitioned-table `transform` (`PartitionedTableImpl.java`)
- snapshots (`ConstructSnapshot.java`, `QueryTable.enableParallelSnapshot`)

Either add the user-relevant ones or present the list as examples.

### 1.14 The partition filter section is incomplete (line 331)
`PartitionAwareSourceTable.getWithWhere`: once *any* serial filter appears in the list (`serialFilterFound`), every later filter, partition filters included, goes into `otherFilters`. So marking a non-partition filter serial also stops later partition filters from being moved ahead. A partition filter that respects a barrier declared by a non-partition filter isn't moved ahead either (`missingBarrier`).

The "treated as stateless" claim itself checks out: `isPrioritizablePartitioningFilter` never looks at statelessness, only at the filter being non-refreshing, having no `i`/`ii`/`k`, and not being a `ReindexingFilter`.

### 1.15 Minor claims to tighten
- **Line 50:** "`updateThreads` greater than 1, which is the default". The default is `-1`, which resolves to `availableProcessors()` (`PeriodicUpdateGraph` constructor), so it's only above 1 on multi-core hosts.
- **Line 120:** the column `Squared = sqrt(X)` computes a square root, so the name contradicts the formula.
- **Line 148:** "parallel execution produces incorrect results" should be "can produce". On GIL builds a Python column isn't split across rows at all.
- **Line 219 (serial filter example):** `is_null` has no side effects, so `with_serial` does nothing useful there even though the prose introduces it "when a filter has stateful side effects". The syntax is valid.
- **Line 314:** "A and B run in parallel" should be "can run in parallel". It depends on thread count, and the barriers don't require it.

---

## 2. Verified accurate (source checked)
- **Property names and defaults:**
  - `minimumParallelSelectRows` = `1L << 22` (4,194,304)
  - `parallelWhereRowsPerSegment` = `1 << 16`, with gate `numberOfRows / 2 > PARALLEL_WHERE_ROWS_PER_SEGMENT` (`AbstractFilterExecution`)
  - `minimumParallelSortRows` = `1L << 20`
  - `parallelSort` = true
  - `statelessSelectByDefault` = true, `statelessFiltersByDefault` = true
  - `OperationInitializationThreadPool.threads` = -1 and `PeriodicUpdateGraph.updateThreads` = -1, both resolving to `availableProcessors()` (`ThreadHelpers`, `PeriodicUpdateGraph`)
- **Python API:**
  - `Selectable.parse`, `with_serial`, `with_declared_barriers`, `with_respected_barriers` (`py/server/deephaven/table.py`)
  - `Filter.with_serial` and friends; `is_null` and `not_` (`filters.py`)
  - `Barrier()` (`concurrency_control.py`)
  - `update` and `where` accept `Selectable`/`Filter` or sequences of them
  - `time_table` and `empty_table` exported from `deephaven`; `randomGaussian`, `randomInt`, `sqrt` exist
- **Free-threaded detection:** `PythonFreeThreadUtil` works it out automatically, so "no other Deephaven configuration is required" holds under the defaults. Line 77 ("may still evaluate… out of order") matches the `SelectColumn.isParallelizable` javadoc.
- **Barriers:** "each barrier can only be declared by one operation" matches the javadoc ("declared by at most one filter"). Results of 0–9 and 10–19 follow from the contract.
- **`with_serial` with `view`/`update_view`:** rejected under the default config, as line 156 says. Python's `view`, `update_view` and `lazy_update` only accept strings anyway. The engine check only runs when `STATELESS_SELECT_BY_DEFAULT` is true, and `lazy_update` has no such guard.
- **Across columns:** cross-column parallelism isn't limited by the row threshold (layers run through `UpdateScheduler`), so line 60 is right.

---

## 3. Placement (Concept guide: configuration detail in the narrative)
Property names and defaults appear inline in the narrative at:
- line 50 (`updateThreads`)
- line 66 (`minimumParallelSortRows`, `parallelSort`)
- line 83 (`OperationInitializationThreadPool.threads`)
- line 87 (`updateThreads`)
- line 124 (`minimumParallelSelectRows`, `parallelWhereRowsPerSegment`, row counts)
- lines 320–323 (implicit-barrier property and field)

Rewrite these in plain terms ("once the table or update is large enough to be worth splitting") and move the values to one Configuration section at the end. `query-table-configuration.md` at f2 already documents every `QueryTable.*` property used here, so link to it. The two thread-pool properties aren't on that page, so they belong in this page's Configuration section. Don't add the precision from findings 1.9 or 1.12 inline either; put it in the same section or reference.

---

## 4. Links
- **Internal links:** every target resolves at f2ef483084, including `Selectable.md`, `Filter.md`, `crash-course/parallelization.md`, `dag.md`, `query-table-configuration.md`, `engine-locking.md`, and the select, filter and sort reference pages. The anchors all resolve too: `#with_serial`, `update.md#serial-execution`, `where.md#serial-execution`, and every in-page anchor.
- **Suggested internal links:** `docs/python/reference/query-language/types/Barrier.md` and `ConcurrencyControl.md` exist at f2. The page links to external pydoc for both (lines 137, 153, 240, 356).
- **Inconsistent target:** line 153 links `with_serial` to `update.md#serial-execution`, while everywhere else it links to `Selectable.md#with_serial`.
- **External, check by hand:** the Python free-threading HOWTO, Oracle's `Runtime.availableProcessors`, and the pydoc `concurrency_control` URLs.

---

## 5. Author queries
- **AQ1 [Controlling execution order, counter example]:** Should the illustrative race table be replaced with output from a real run on a GIL build? The current table can't happen there (1.5). Or should the example say it assumes a free-threaded build?
- **AQ2 [How parallelization works]:** Is the "three ways" framing (tables, rows, columns) meant to cover every mechanism, including sort, `update_by`, snapshots and partitioned transforms? If so, finding 1.13 applies.

---

## 6. Follow-ups outside this file
- Fix the `ConcurrencyControl.withSerial` javadoc default for `serialSelectImplicitBarriers` (1.12).
- The Groovy sibling needs the same pass. Findings 1.1, 1.2, 1.4, 1.6, 1.7, 1.8, 1.9, 1.12 and 1.13 describe engine behavior, not language behavior, so they probably apply there as well.