<!-- Saved by the orchestrating session from the subagent's returned text; the subagent's own Write was refused. -->

**Summary:** The worst errors are the "what gets parallelized" classification and the two counter examples. `view`/`update_view`/`lazy_update` are wrongly listed as "not parallelized". `sort` is only parallelized at initialization, never on updates, and `update_by`, `range_join` and snapshots are missing from the list. The counter examples show behavior the engine doesn't produce: on a standard Python build a Python formula is never split within a column, the sample output can't happen, and the barrier example's "without X it races" claims are false for that exact code. The API names, property names, defaults and link targets all check out against source and the f2ef483084 tree. What needs work is the "never concurrent" GIL wording, the "on a single thread" paraphrase of `with_serial`, the implicit-barrier section mixing two properties, and property values dropped into the concept-guide narrative.

---

# Accuracy review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857, commit f2ef483084)

**Input:** `.claude/skills/deephaven-core-accuracy-check/evals/files/parallelization-pre-review.md`. It is byte-identical to `git show f2ef483084:docs/python/conceptual/query-engine/parallelization.md`.
**Category:** Concept guide (`conceptual/`). The sidebar puts it under Best practices → Performance, but that is for discoverability and doesn't change the category. So the Concept-guide placement rule applies: property names and defaults don't belong in the narrative.
**Scope:** Accuracy and links only, Python file only. The Groovy sibling (`docs/groovy/conceptual/query-engine/parallelization.md`) exists but you ruled it out of scope, so I didn't do the cross-language check or the Groovy duplicate sweep. Every finding below probably applies there too.
**Mode:** Report only. I made no edits.

## 1. Incorrect or overstated claims (high priority)

**1.1 The classification "What does NOT get parallelized" is the wrong mental model for `view` / `update_view` / `lazy_update` (line 70).**
The doc says these are "lazily evaluated when cells are accessed, not computed upfront," and lists them as not parallelized. The first half is true: all three produce a `ViewColumnSource` through `AbstractFormulaColumn.getViewColumnSource(lazy)`. But a deferred formula runs on whichever thread reads the cell. If the reader is a parallel `update`/`select`, a parallel `where`, or a parallel snapshot, the formula runs concurrently on those threads. A `view`/`update_view` formula can also be evaluated again every time the same row is read.

The engine's own guard shows this. In `QueryTable.viewOrUpdateView`:
> "An updateView can fetch things in any order; therefore we cannot allow it to be stateful" … `"A stateful column cannot safely be used in a view or updateView."`

and just above that:
> `"view and updateView cannot respect barriers"`

- **Fix:** Take these operations out of the "not parallelized" list. Say instead that they do no work when the table is created or updated, and that their formulas run whenever and wherever the column is read, possibly concurrently and possibly more than once. Also replace "upfront," which is ambiguous.

**1.2 `sort` is not parallelized during updates (lines 66 and 87).**
Line 66 lists `sort` under "What gets parallelized," and line 87 says updates parallelize "just like during initialization." But `SortHelpers.parallelizableOperationInitializer()` returns null on update-graph threads:
> "Update graph refresh threads have the poisoned ExecutionContext … a sort listener running there sorts serially."

- **Fix:** Scope the `sort` entry to initialization, and make line 87 stop implying that every initialization-time parallelism carries over to updates.

**1.3 The "What gets parallelized" list is incomplete (lines 62–66).**
Deriving the list from every use of `OperationInitializerJobScheduler` / `UpdateGraphJobScheduler` / `canParallelize()` turns up parallel paths the doc leaves out:
- **`update_by`**: `UpdateBy.java` parallelizes at initialization and on updates.
- **`range_join`**: `RangeJoinOperation.initialize` parallelizes.
- **Snapshots**: `ConstructSnapshot` reads columns in parallel.

At minimum, add `update_by`, since users see it directly. The link target `reference/table-operations/update-by-operations/updateBy.md` exists at f2ef483084.

**1.4 The GIL callout says Python-backed code is "never run concurrently" (line 75).**
Source only supports "never split within a column or filter":
- `DhFormulaColumn.isParallelizable()` and `FormulaColumnPython.isParallelizable()` return `PythonFreeThreadUtil.isPythonFreeThreaded()`.
- `ConditionFilter.permitParallelization()` returns false for Python filters on a GIL build.
- Column-level concurrency is a separate gate. `SelectColumnLayer.allowCrossColumnParallelization()` returns `isStateless()`, which is true by default, so two Python-backed columns in one `update` can still run at the same time. Across-table notifications can also run Python code from different operations at the same time.
- **Fix:** Say "not split across threads" instead of "never run concurrently."

**1.5 The first counter example's sample output can't happen on a standard Python build (lines 175–190).**
On a GIL build, neither `A` nor `B` is split across rows (see 1.4), so each column is evaluated in row order on one thread and `A` is strictly increasing. The sample table shows A=5 followed by A=4, and A=9, B=8. Only a free-threaded build could produce that. The same problems affect the notes under the table:
- "gaps (no 10-19 visible)" means nothing for 5 rows sampled out of a 5,000,000-row table.
- "`B` not following `A + 1`" treats something as an invariant when no execution order promises it.

What does race on a GIL build is `A` against `B`: both are stateless by default, so they can run in parallel with each other.

The fix (lines 194–211) also switches to a single `ID` column, so it doesn't show how to fix the two-column case it just described.
- **Fix:** Replace the invented table with a description of the failure the engine actually allows, and keep both columns in the fix. ⚠️ Needs SME input: whether to show sample output at all, and if so, output captured from a real run on a named build.

**1.6 Barrier example: the "without X" claims don't match what this code does (lines 244 and 280).**
With both columns `.with_serial()`, both are non-stateless. `anyParallelColumns()` is therefore false and `QueryTable` uses an `ImmediateJobScheduler`, so the columns run one after the other on the calling thread.
- **Line 244**, "both start simultaneously, both read `counter = 0`": that is not what happens without the barrier in this example. The barrier is still the only guaranteed ordering. The `ConcurrencyControl.withSerial` javadoc says that with implicit barriers off, "no additional ordering between selectable expressions is imposed." Describe the risk as a lost guarantee, not a certain race, and drop "both read 0."
- **Line 280**, "Without `with_serial`, rows within each column would also race": this is false for this code. The table has 10 rows, far below `MINIMUM_PARALLEL_SELECT_ROWS`, and on a GIL build the Python formula isn't split in any case. Say that `with_serial` is what guarantees row order.

**1.7 "Stateless mode / Stateful mode" mixes up two properties, and the first sentence names a Java field (lines 320–323).**
- "When `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is enabled" names a static field, not a configuration property. The property is `QueryTable.serialSelectImplicitBarriers`.
- Its default is `!STATELESS_SELECT_BY_DEFAULT`, so setting `QueryTable.statelessSelectByDefault=false` also turns implicit barriers on. The doc gives only the explicit `=true` route.
- What it does is described accurately. `SelectAndViewAnalyzer` makes each non-stateless column declare an implicit barrier and respect every earlier one.

**Source bug to file separately:** the `ConcurrencyControl.withSerial` javadoc in `table-api` says this property defaults "to the value of" `statelessSelectByDefault`. The code defaults to its negation.

**1.8 "processes rows one at a time, in order, on a single thread" (line 148).**
The contract is:
> "never be invoked concurrently with itself … Rows are evaluated sequentially in row set order."

It never mentions a single thread. Line 213 is a faithful paraphrase; line 148 isn't.
- **Fix:** Drop "on a single thread," or reword to match line 213.

**1.9 "Concurrency control works the same way for `Filter` as it does for `Selectable`" (line 135).**
The javadoc gives them different rules. For a filter, "serial acts as an absolute reordering barrier." For selectables, ordering depends on the implicit-barrier setting. Line 234 of the doc even describes the filter-only reordering behavior, so the page contradicts itself.
- **Fix:** Say the same methods are available on both, not that they behave the same.

**1.10 "By default, Deephaven parallelizes operations that are stateless" (line 93).**
This suggests the engine checks each formula for statelessness. It doesn't. With the default setting, `DhFormulaColumn.isStateless()` just returns `true` (`if (QueryTable.STATELESS_SELECT_BY_DEFAULT) return true;`), and `ConditionFilter` does the same through `STATELESS_FILTERS_BY_DEFAULT`.
- **Fix:** "By default, Deephaven assumes every formula and filter is stateless…"

**1.11 Absolute wording in the breaking-change callout (line 9).**
- "will now produce incorrect results" should be "can."
- "Deephaven 40 and earlier assumed all formulas required sequential processing" is stronger than what the pre-41 code path shows: the old path still marks a formula stateless when every param is an immutable type and every column it uses is stateless.
- The version is confirmed: the default flipped in `8ea55b6a1d` (DH-20714), and the first tag containing it is v41.0.0.

## 2. Precision issues (medium)

- **Line 50:** "`updateThreads` greater than 1, which is the default." The default is `-1`, which resolves to `availableProcessors()`, so it's greater than 1 only on a multi-core host. This is also configuration injection (see section 3).
- **Line 124 (the NOTE):** The numbers are correct: `1L << 22` is about 4.19M rows, and `where` needs `numberOfRows / 2 > 1 << 16`. But the sentence frames the threshold as the size of "a table." During updates, the count is that cycle's added plus modified rows, and a cycle with shifts is never split (`!hasShifts`).
- **Lines 95–99:** "Same output for same input" is stricter than what parallel safety needs. The page's own first example uses `randomGaussian`/`randomInt`, which don't meet that bar, yet they are thread-safe (`ThreadLocalRandom` in `io.deephaven.function.Random`). See AQ1.
- **Line 77:** "the engine may still evaluate a non-parallelizable column out of order." ⚠️ Could not verify. For selectables, the non-parallel path processes rows in row-set order. The difference I can confirm is for filters: without `with_serial`, a filter can be reordered relative to other filters and can evaluate rows that later filters would remove. See AQ2.
- **Serial filter example (lines 219–232):** The text says to use explicit `Filter` objects "when a filter has stateful side effects," but the example uses `is_null`, which has none. So `with_serial` isn't needed there, and at 1000 rows no split would happen anyway. It shows the syntax but not the reason. You could also mention `Filter.from_("…")` for string conditions that need `with_serial`.
- **Line 314:** "A and B run in parallel" should be "can run in parallel." That depends on the operation initializer's `canParallelize()`.
- **Line 156:** Accurate for Python. `view`/`update_view`/`lazy_update` accept only strings, not `Selectable`, and the engine rejects stateful columns and barriers in `view`/`updateView`. Consider adding `lazy_update` to that sentence.

## 3. Configuration placement (Concept guide)

Property names and defaults are dropped into the narrative at lines 50, 66, 83, 87, 124, and 320–323. That's the "configuration injection" pattern.
- Every name and default is correct against `QueryTable.java`, `PeriodicUpdateGraph.java`, and `OperationInitializationThreadPool.java`.
- All of them are already listed in `docs/python/conceptual/query-table-configuration.md` at f2ef483084 (tables at lines 30–47).
- **Recommendation:** Describe these at the concept level in the narrative, then either add one Configuration section at the end or rely on the existing link at line 126. Don't add any new inline values while fixing 1.2, 1.7, or section 2.

## 4. Verified accurate (with source)

- `Selectable.parse`, `with_serial`, `with_declared_barriers`, `with_respected_barriers` take a single `Barrier` or a sequence (`py/server/deephaven/table.py`). `update`/`select` accept `Selectable`.
- `Barrier()` takes no arguments. `filters.is_null` / `not_` return a `Filter` that has `with_serial`.
- "Each barrier can only be declared by one operation": `addDeclaredBarriersToMap` throws "Duplicate barrier".
- Implicit barriers make serial columns wait for each other (`SelectAndViewAnalyzer`, ~line 176).
- The two thread pools and their property names are correct. Both default to `-1`, which resolves to `availableProcessors()`.
- Parallelism across tables comes from `ConcurrentNotificationProcessor`. Parallelism across columns is gated by `ENABLE_PARALLEL_SELECT_AND_UPDATE` and `anyParallelColumns()`.
- `where` and `select`/`update` parallelize both at initialization and on updates.
- Free-threaded detection (`PythonFreeThreadUtil`) needs no Deephaven configuration.
- Partition-filter prioritization matches `PartitionAwareSourceTable`. Not mentioned in the doc: once a serial filter appears, no later filter is prioritized either.

## 5. Links

- **Internal targets:** All exist at f2ef483084. `types/Selectable.md`, `types/Filter.md`, and `crash-course/parallelization.md` are added by that PR, so they have to merge together.
- **Anchors:** `Selectable.md#with_serial`, `update.md#serial-execution`, `where.md#serial-execution`, and all the in-page anchors resolve.
- **Line 153:** the `with_serial` method links to `update.md#serial-execution`. A better target is `Selectable.md#with_serial` or `ConcurrencyControl.md`.
- **`Barrier` / `ConcurrencyControl` links:** they point to external pydoc, but the internal `types/Barrier.md` and `types/ConcurrencyControl.md` exist at f2ef483084. Consider linking to those.
- **Suggested new link:** `updateBy.md`, if `update_by` is added per 1.3.
- **External links to check by hand:** the pydoc `concurrency_control` URLs, the Python free-threading howto, and the Oracle `availableProcessors` javadoc.

## 6. Completeness sweep (Python file)

I re-grepped the file for each finding:
- Sort-on-update (1.2): lines 66 and 87.
- Race wording (1.6): lines 244 and 280.
- Single-thread wording (1.8): line 148. Line 213 is fine.
- "Assumes all formulas can run in parallel": lines 9 and 345.
- `view` classification (1.1): line 70. Line 156 is fine.
- Configuration injection (3): lines 50, 66, 83, 87, 124, 320–323.
- The "Choosing an approach" section and the Quick reference table repeat nothing that is wrong.

**Groovy sibling not swept (out of scope).** It very likely repeats 1.1–1.3 and 1.6–1.9.

## Author queries

- **AQ1** [When parallelization is safe by default, bullets]: Is determinism part of the stateless contract, or is thread-safety plus order-independence enough?
- **AQ2** [Python GIL callout, para 2]: Which out-of-order case does "may still evaluate a non-parallelizable column out of order" mean for selectables?
- **AQ3** [Counter example output table]: Remove the table, or replace it with output captured from a real run on a named Python build?
