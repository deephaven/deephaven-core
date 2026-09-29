# Full review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857, snapshot at `f2ef483084`)

**Category:** Concept guide. It lives under `conceptual/`. The sidebar puts it under Best practices → Performance, but that placement is for discoverability and doesn't change the category, so the concept-guide tone and structure profile applies.
**Scope:** This covers the Python file only. The Groovy sibling (`docs/groovy/conceptual/query-engine/parallelization.md`) exists, but you ruled it out of scope. Most of the Accuracy findings below are shared claims, so it will almost certainly need the same fixes.
**Mode:** Report only. Nothing was edited.

## Editorial summary

The page has the right skeleton: how it works, when it's safe, how to control it, how to choose. The `with_serial` vs. barriers contrast (lines 139–144) is placed exactly where a reader needs it. The biggest problem is the mental model. The "What does NOT get parallelized" list tells readers that `view`, `update_view` and `lazy_update` are a safe place for stateful code, and source says the opposite. Second, the counter example teaches a failure that a standard Python build can't reproduce, and it fixes a different problem than the one it shows. Structurally, configuration detail is spread through the narrative, and the page has two competing decision guides and three copies of the counter example. **Verdict: needs revision.** It doesn't need restructuring: the outline holds, but the content inside it needs consolidating.

## Developmental notes

1. **Purpose (whole page).** After reading, a reader should be able to decide whether their formula or filter is safe under Deephaven's default parallel execution, and if it isn't, apply `with_serial` and/or barriers correctly. The page supports this purpose. It's clearest in "Choosing an approach", which is also the last section a reader reaches.
2. **Key message is stated, but it sits behind a breaking-change box (lines 6–11).** "Most queries need no changes" appears at line 6, then again at lines 130, 151 and 343. Line 6 is the one that counts. Suggested fix: keep line 6 as the opening message. Shrink the version callout to a short upgrade note that links to the controls, so a new 41+ reader doesn't start with an alarm about behavior they never had.
3. **Mental model the reader leaves with.** These are the page's core claims as a reader would summarize them. They were handed to the accuracy step as its first targets.
   - C1. Deephaven parallelizes across tables, rows and columns.
   - C2. `view`, `update_view` and `lazy_update` are not parallelized. **This is wrong** (see Accuracy A1).
   - C3. On a GIL Python build, Python formulas never run concurrently. **This is overstated** (A3).
   - C4. "Stateless" means the formula doesn't read globals. **Partly wrong** (A8).
   - C5. `with_serial` means rows run in order on a single thread. **"Single thread" is overstated** (A7).
   - C6. Barriers order one column relative to another.
   - C7. Before 41, every formula ran sequentially. **Overstated, and the callout leaves out an ordering change** (A4).
   - C8. Implicit barriers are off by default. **Only while `statelessSelectByDefault` keeps its default** (A6).
4. **Audience fit.**
   - The Quick reference table (lines 13–22) uses `with_serial`, "Barriers" and "implicit barriers" about 100 lines before "Key concepts" defines them (line 132).
   - "Partition filters" and "partitioning columns" (line 329) are never tied to partitioned source tables such as Parquet or Iceberg, so most readers won't know whether the section applies to them.
   - "Update Graph Processor Thread Pool" (line 87) uses a retired internal name. The update graph class is now `PeriodicUpdateGraph`.
5. **Progression.** The main thread is interrupted twice by detours: the GIL caution (lines 74–77) and the configuration-heavy "Query phases and thread pools" (lines 79–89). Both sit between "what gets parallelized" and "when it's safe". The page then closes with two decision guides that overlap: Quick reference at the top and "Choosing an approach" at the bottom.
6. **Scope.** About eight property names and defaults appear in running prose (lines 50, 66, 83, 87, 124, 126, 320–323). "Implicit barriers" (lines 318–325) is configuration. It isn't a concept. All of this belongs in one Configuration section at the end, or behind the link to `query-table-configuration.md`.

## Accuracy

These findings come from the accuracy step. Source was checked in `/Users/margaretkennedy/dhc-skills-chip`. The doc snapshot is byte-identical to `f2ef483084:docs/python/conceptual/query-engine/parallelization.md`.

**A1. Deferred operations are classified wrongly (What does NOT get parallelized, line 70; restated at line 156).** "Not computed upfront" is true. "Not parallelized" is the wrong model:
- `view`, `update_view` and `lazy_update` produce view column sources whose formulas run on whatever thread reads them. When a parallel `update`, `select` or `where` downstream reads the column, the formula runs concurrently on those threads. A `view` or `update_view` formula can also run again on every read.
- The engine's own guard shows the intent. `QueryTable.viewOrUpdateView` throws `"view and updateView cannot respect barriers"`. With `STATELESS_SELECT_BY_DEFAULT` it also throws `"A stateful column cannot safely be used in a view or updateView."`

Fix: drop these three from the "not parallelized" list. Say that their formulas are evaluated later, on the reading thread, possibly concurrently and more than once, so they must be stateless. This is the most expensive error on the page, because it points readers at exactly the wrong place to put stateful code.

**A2. The `with_serial` restriction leaves out `lazy_update` (IMPORTANT box, line 156).** `QueryTable.lazyUpdate` (around line 2152) builds its result through `SelectAndViewAnalyzer` in `VIEW_LAZY` mode. It has neither the barrier guard nor the stateful-column guard that `viewOrUpdateView` has. As a result, `lazy_update` accepts a serial or barrier `Selectable` without complaint and without any ordering guarantee. Line 70 already groups `lazy_update` with the other two, so the box should cover all three. See AQ4.

**A3. The GIL caution overstates the guarantee (lines 75–77).**
- The per-column claim is correct. `DhFormulaColumn.isParallelizable()` returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`, and `ConditionFilter.permitParallelization()` returns false for Python-backed filters on a non-free-threaded build. So a Python column's rows are never split across threads.
- "Never run concurrently" is still wrong. On the default 41+ config, a Python column is `isStateless()` = true. `SelectColumnLayer.allowCrossColumnParallelization()` returns `selectColumn.isStateless()`, so two Python columns in one `update` can be scheduled on different threads at the same time, and their calls interleave under the GIL.
- That cross-column interleaving is what actually breaks the counter example on a standard build.

Fix: state it as "a Python-backed column is never split across threads on a GIL build, but separate columns can still run concurrently." That gives readers the real reason to use `with_serial` whatever build they run. The "may still evaluate a non-parallelizable column out of order" clause (line 77) has no source backing. The serial path processes rows in row-set order. See AQ1.

**A4. The breaking-change callout (line 9) is overstated and leaves something out.** Commit `8ea55b6a1d` (DH-20714) is first contained in the v41.0.0 tags, so the 41 boundary is correct. But:
- (a) Before 41, `DhFormulaColumn.isStateless()` still returned true for formulas whose parameters were all immutable types. Not all formulas were treated as sequential.
- (b) The same change flipped the default of `QueryTable.serialSelectImplicitBarriers` (it is `!STATELESS_SELECT_BY_DEFAULT`) from true to false. So pre-41 code with two serial columns got implicit ordering between them and no longer does.

Fix: soften (a) to something like "treated most formulas as requiring sequential processing". Add (b), because it is the break that bites users who already use `with_serial`. See AQ5.

**A5. The counter example's failure isn't reproducible as shown (lines 175–190).**
- On a standard GIL build, `get_and_increment_counter()` is Python-backed, so its column is never split by rows (A3). Column `A` would come out monotonically increasing, with gaps where `B` interleaved. The illustrated `A` = 0, 1, 5, 4, 9 isn't possible there.
- "Gaps (no 10–19 visible)" doesn't mean anything for a 5M-row table shown five rows at a time.
- "`B` not following `A + 1`" implies the correct result is `B = A + 1`. The barrier section (lines 244, 280) says the intended result is `A` = 0…N−1 and `B` = N…2N−1. The two sections contradict each other.
- "Row 3 has `A=5`" counts rows from 1, while the page counts from 0 elsewhere ("row 0, then row 1").

Fix: rewrite the failure description around cross-column interleaving, drop the `A + 1` framing, and drop the fabricated-looking table unless it was actually observed (AQ2).

**A6. Implicit barriers (lines 320–325).**
- `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is a Java field. The only user-facing name is the property `QueryTable.serialSelectImplicitBarriers`, so naming both reads as two settings.
- "Stateless mode" and "Stateful mode" aren't engine terms.
- "(default)" holds only while `statelessSelectByDefault=true`, because the default is derived as `!STATELESS_SELECT_BY_DEFAULT` (QueryTable.java lines 400–402).

Fix: move this to the Configuration section. Describe it as one property whose default follows `statelessSelectByDefault`.

Also, the Quick reference row "Multiple operations sharing state → Barriers or implicit barriers" (line 21) recommends a setting the page later says most users shouldn't change.

Separately, source has a bug worth a follow-up ticket. The `ConcurrencyControl.withSerial()` javadoc says this property defaults "to the value of `QueryTable.statelessSelectByDefault`", which is the opposite of the code (AQ6).

**A7. "On a single thread" isn't part of the contract (Serialization, line 148; see also line 213).** The `ConcurrencyControl.withSerial()` javadoc promises "never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order." It never names a single thread, and a serial column can run on different threads in different cycles. Line 213 ("only one thread processes the column at a time") matches the contract. Line 148 should match it too.

**A8. The definition of stateless overreaches (lines 93–99).** "Doesn't read … global variables" is too broad. Reading an immutable global constant is safe, and the engine's own pre-41 heuristic treated immutable query-scope parameters as stateless. The real test is not reading shared state that changes, and not writing shared state. Relatedly, "thread-safe" (lines 17, 337) is used as if it meant the same as "stateless". Those are different properties; there's a style note on this below.

**A9. The barrier example's "would race" claim is overstated for this exact code (lines 244, 280).**
- Both columns are serial, so neither is `isStateless()`. That means `analyzer.anyParallelColumns()` is false, and static initialization uses `ImmediateJobScheduler` (QueryTable.java around lines 1825–1830). The layers run in order on one thread.
- Without the barrier, this code would in practice still give 0–9 and 10–19. What it would lose is the guarantee: add one parallel column, or run it on a refreshing table, and the ordering is no longer promised.
- "Both read `counter = 0` and produce overlapping results" also misdescribes a shared counter. Concurrent callers get interleaved values, not two copies of 0–9.

Fix: say "without the barrier, the engine doesn't guarantee that A finishes before B starts." Keep the example: the barrier is the guarantee.

**A10. Row-splitting during updates depends on the size of each cycle's changes (line 87, "just like during initialization").** In `SelectColumnLayer`, the split happens when `added + modified` for the cycle is at least `MINIMUM_PARALLEL_SELECT_ROWS` (`1L << 22`). Typical ticks never reach that, so live updates mostly get cross-table and cross-column parallelism, not row splits. The thresholds at line 124 are otherwise correct: `1L << 22` for select, and `numberOfRows / 2 > parallelWhereRowsPerSegment` with a default of `1 << 16` for `where`. They just belong in the Configuration section.

**A11. The "What gets parallelized" list is incomplete (lines 62–66).**
- `UpdateBy` caches its inputs and processes buckets through `OperationInitializerJobScheduler` / `UpdateGraphJobScheduler` (UpdateBy.java lines 237–338).
- Snapshot construction and range join also use the job schedulers.

In a concept guide it's fine to say "including" rather than list everything, but as written the list reads as complete. Also, the "does not" item "Operations waiting for dependencies" (line 72) is a scheduling state, not an operation.

**A12. Minor points.**
- Line 50: the default for `PeriodicUpdateGraph.updateThreads` is `-1`, meaning `availableProcessors()`. That isn't the same as "greater than 1" on a single-core host.
- Line 52: "Independent tables run in parallel automatically" applies to update cycles, not to the initialization of separately created operations.

**Verified correct:**
- The property names and defaults at lines 66, 83, 87, 124 and 126.
- `OperationInitializationThreadPool.threads` = `-1`.
- "Each barrier can only be declared by one operation" (javadoc: "declared by at most one filter"; the analyzer throws `Duplicate barrier`).
- The serial-filter reordering claim (javadoc: "serial acts as an absolute reordering barrier").
- The partition-filter behavior. In `PartitionAwareSourceTable`, `isPrioritizablePartitioningFilter` has no statelessness check, and a serial filter stops prioritization. Note that it stops it for every filter after it too, not only for the serial one. The line 331 wording could say so.
- All Python APIs: `Selectable.parse`, `with_serial`, `with_declared_barriers`, `with_respected_barriers` (accepts a sequence), `deephaven.filters.is_null` and `not_`, and `Filter.with_serial`.
- `randomGaussian` and `randomInt` in `io.deephaven.function.Random`.

**Links.**
- Every relative link resolves at `f2ef483084`, including `where.md#serial-execution`, `update.md#serial-execution` and `Selectable.md#with_serial`.
- On the current checkout (`373cece093`, which is not a descendant of `f2ef483084`), `crash-course/parallelization.md`, `reference/query-language/types/Selectable.md` and `Filter.md` don't exist. Confirm they land with this PR, or re-check them after a rebase.
- Check the external pydoc and Oracle `availableProcessors` links manually.

**Re-verify step (inbound anchors).** No edits were applied. But any restructuring from the next section has to keep these anchors, or update the pages that link to them:
- `#serialization` is linked from `conceptual/query-table-configuration.md:238`, `reference/table-operations/select/view.md:23` and `update-view.md:23`.
- `#barriers` is linked from `reference/query-language/types/Selectable.md:56,79` and `Filter.md:91,112`.
- The page links to its own `#example-a-counter-needs-serialization` and `#example-extending-the-counter-with-a-barrier` (lines 338–339). Merging the counter examples (S3) will break those two links.

## Structure

1. **Configuration detail is spread through the narrative (Level of abstraction, lines 50, 66, 83, 87, 89, 124, 126, 318–325).** Fix: add one `## Configuration` section before Key takeaways. It should hold the thread-pool properties, the parallel thresholds, `parallelSort`, the stateless-by-default properties and implicit barriers. Rewrite the narrative sentences at concept level, for example "once the table is large enough to be worth splitting".
2. **The main thread detours before it finishes (Topic interleaving, lines 74–89).** The GIL caution is a platform caveat placed inside "Within a single table". "Query phases and thread pools" is mechanism plus configuration and interrupts the path from "what's parallelized" to "when it's safe". Fix: move the GIL caution into "When parallelization is safe by default" or give it its own short "Python formulas" subsection. Cut the thread-pool section down to one paragraph about initialization vs. updates and move its properties to Configuration.
3. **The counter example appears three times (Near-verbatim repeated examples, lines 162–178, 194–211, 246–278).** The same function is redefined three times, and the page runs past 350 lines. Fix: use one two-column counter as a wrong-then-right pair. Show the plain version, then `with_serial` plus a barrier, with both outputs, and refer back to it elsewhere. This also resolves A5's contradiction about the intended result.
4. **Two decision guides compete (Quick reference at lines 13–22 vs. Choosing an approach at lines 333–339).** They list different cases: the table has "Non-thread-safe library" and "implicit barriers", while the prose has "cumulative calculations" and "cache". Fix: keep one table near the top as an orientation map. Its early position is good. Fold the extra cases from "Choosing an approach" into it, and turn that section into a pointer or remove it.
5. **Parent and child headings don't match (lines 26, 28, 54).** The intro to "How parallelization works" names three ways: tables, rows, columns. The child headings name two: "Across tables" and "Within a single table", with rows and columns as bold labels. Fix: either give all three their own headings, or change line 26 to "in two ways: across tables, and within a table (by rows and by columns)".
6. **Rows in the same table or list aren't the same kind of thing.**
   - The Quick reference mixes a specific example ("Global counter") with categories ("Pure column math"), and "logging" doesn't affect results.
   - "What does NOT get parallelized" (lines 68–72) mixes operations, a user-applied marker, and a scheduling state.
   - Fix: make each list one kind of item, and turn examples into an example column.
7. **"Stateful partition filters" (lines 327–331) is an orphaned aside.** It's niche, sits under "Controlling execution order", has no Quick reference row, and doesn't say which tables it applies to. Fix: move it after Barriers under an "Advanced: filters on partitioned tables" heading, with one sentence saying it applies only to partitioned source tables. Or move it to the `where` reference page.
8. **The same warning appears twice with different wording (lines 144 and 282–283).** "You often need both" and "you typically need both `with_serial` and a barrier" state the same point in different words. Keep the line 144 version and let the example's comments carry it.
9. **Terms are used before they're defined (lines 13–22 vs. 132).** Add a one-line forward reference under the Quick reference, such as "Terms are defined in Controlling execution order".
10. **Key takeaways includes a configuration fact (line 348).** "Both thread pools use all CPU cores by default" isn't a key message. Replace it with the view/update_view takeaway from A1.

## Examples

1. **The unsafe and fixed counter examples don't illustrate the same problem (lines 175 vs. 209–210).** The unsafe version has two columns. The fix has one column (`ID`) and never shows how to repair `A` and `B`. The failure shown is also not reproducible (A5). Fix: use the wrong-then-right pair from Structure 3.
2. **The serial-filter example has no side effects (lines 219–232).** The lead-in says to use this when a filter "has stateful side effects", but `is_null` and `not_(is_null(...))` are pure, so `with_serial` is doing no work. Fix: use a filter that calls a Python function that records or counts rows, or present this as a syntax-only example and say so.
3. **The multiple-barriers example uses pure formulas (lines 291–311).** `A = i * 2` and similar formulas have no dependency that needs a barrier. The example shows syntax and execution order but not why you'd want it. Add a comment saying so, or give C and D a real read-after-write on shared state.
4. **The `skip-test` counter block (line 162) is justified** because its output is non-deterministic. But its illustrative output table can't come from a real run (A5). Either use an output from a real run on a named build, or describe the failure in prose.
5. **The tested blocks with 5M rows (line 210) make five million Python calls** in the docs snapshotter, which is slow for a point a much smaller table can make. Use a smaller count, and keep it above the thresholds only if the example needs parallelism. It does for the unsafe version, but that version is `skip-test`.
6. **The stateless examples (lines 103–121) produce eight output tables** for four one-line points. Consider putting all four formulas in one table.
7. **Minor:** `' '` (line 112) is a character literal inside a string built with backticks. It works, but `` ` ` `` is more consistent with the rest of the example.

## Style

- **Coined labels (3 terms, lines 26–60, 83, 87).** "Across tables / across rows / across columns" are ad-hoc labels. Prefer standard terms such as "concurrent table updates" and "concurrent row calculations", or define the labels once.
- **"Stateless" and "thread-safe" used as synonyms (lines 17, 93, 337).** They're different properties. Pick "stateless" as the page's term and use "thread-safe" only for library calls.
- **Parenthetical caveats (about 6: lines 50, 66, 71, 83, 124, 285).** Example: line 50 "(This depends on `PeriodicUpdateGraph.updateThreads` being greater than 1 …)". These go away once Structure 1 is done.
- **Passive or impersonal voice (about 8).** Examples: line 83 "This is handled by the …", line 70 "are lazily evaluated when cells are accessed". Suggested rewrite: "Deephaven runs initialization on the operation-initialization thread pool."
- **Future tense (1).** Line 9 "will now produce" should be "produces".
- **Link consistency (3).**
  - Line 50: the link text "Thread pools" doesn't match the heading "Query phases and thread pools".
  - Line 153: `with_serial` links to `update.md#serial-execution`, while everywhere else it links to `Selectable.md#with_serial`.
  - `Barrier` (lines 137, 240) and `ConcurrencyControl` (line 153) link to external pydoc, even though internal reference pages exist (`reference/query-language/types/Barrier.md`, `ConcurrencyControl.md`).
- **Title-cased coined names (2, lines 83, 87).** "Operation Initialization Thread Pool" and "Update Graph Processor Thread Pool" should be lowercase descriptions. The second also uses a retired class name.
- **One idea per paragraph (lines 87, 331).** Each packs three or four claims. Split them.
- **Bullet periods (lines 64–66).** Sentence fragments carry periods. Minor.
- **Clean on the mechanical checks:** no dot-prefixed method names, no empty `()` in prose, no "here" links, no curly quotes, em dashes correctly spaced (the `0–9` en dashes are correct range usage), headings in sentence case, and the first mention of each method is linked.

## Author queries

- **AQ1 [How parallelization works, GIL caution, line 77]:** Which code path evaluates a non-parallelizable column's rows out of row-set order? `SelectColumnLayer.doSerialApplyUpdate` appears to go in order. Is the real hazard cross-column concurrency?
- **AQ2 [Example: a counter needs serialization, lines 182–190]:** Was this output observed? On which Python build and configuration? On a GIL build, column `A` should be monotonic.
- **AQ3 [Stateful partition filters, line 331]:** Please confirm the section applies only to partition-aware source tables, and that `Date=today()` is the right example once filters are stateless by default.
- **AQ4 [Serialization, IMPORTANT box, line 156]:** `lazyUpdate` doesn't reject serial or barrier columns the way `viewOrUpdateView` does. Is that intended, and what should the doc promise for `lazy_update` plus `with_serial`?
- **AQ5 [Breaking-change callout, line 9]:** Should the 41 note mention that `serialSelectImplicitBarriers` flipped from true to false, so two serial columns lost their implicit ordering?
- **AQ6 [source, not the doc]:** The `ConcurrencyControl.withSerial()` javadoc (table-api) says `serialSelectImplicitBarriers` defaults to the value of `statelessSelectByDefault`. `QueryTable` uses the negation. Should this be filed as a javadoc fix?

## Strengths

1. The `with_serial` vs. barriers comparison (lines 139–144) comes exactly where both are first named, and it's clear.
2. Putting a Quick reference near the top is the right shape for a long concept guide. Keep it there once it's cleaned up (Structure 4 and 6).
3. The note at lines 282–285 about when a respecting column does *not* need `with_serial` is a subtle, correct point that other docs miss. Keep it when you consolidate the examples.