# Full review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857, f2ef483084)

**Category:** Concept guide. It lives under `conceptual/`. The sidebar puts it under Best practices → Performance to help readers find it, but that placement doesn't change the category. I judged links and anchors against the tree at f2ef483084, where every internal target exists. The snapshot is byte-identical to that commit's file. The Groovy sibling was out of scope and I didn't review it, but a quick grep shows it repeats most of the shared wrong claims below (noted per finding). Nothing was edited.

## Editorial summary

The page is well organized for a concept guide. It opens with a quick-reference table, moves from "how it works" to "when it's safe" to "how to control it," and ends with a decision section. But the mental model it builds is wrong in several places, and the wrong places are the most-read ones.

The biggest issue is that the page says `with_serial` alone is enough for shared state: a global counter, file I/O, or a non-thread-safe library. The page's own counter example disproves this. It uses two columns, and its "fix" quietly drops to one. The Python GIL callout makes a related error: it says Python formulas "never run concurrently" on a standard build, but source shows two Python columns in one `update` still run on separate threads.

**Developmental verdict: needs revision.** The structure mostly holds. The accuracy and example problems need fixing before technical sign-off.

## Developmental notes

1. **The key message is buried under a breaking-change warning (lines 6–11).** The reader's first impression is "Deephaven will break your code." The real point is "Deephaven parallelizes for you, most queries need no changes, and the controls below are for the exceptions." That sentence appears only at line 343 (Key takeaways) and half-appears at line 130. Suggested fix: state it plainly in the intro and shrink the callout to a version note that links to [Controlling execution order](#controlling-execution-order). Its history claim is also wrong; see Accuracy A5.
2. **The page teaches "`with_serial` makes shared state safe."** Summarized as a reader would take it, the page's core claims are:
   - Deephaven parallelizes across tables, rows, and columns.
   - Pure formulas are safe.
   - `view`, `update_view`, and `lazy_update` are not parallelized.
   - Python code doesn't run in parallel on a GIL build.
   - `with_serial` fixes counters, I/O, and unsafe libraries.
   - Barriers order columns.

   The third, fourth and fifth claims are wrong or incomplete (A1–A3). Those are the claims a reader will act on.
3. **The definition of "stateless" rules out reading ordinary variables (lines 93–99).** "Doesn't read … global variables" makes every formula that reads a query-scope variable look unsafe, for example `threshold = 5; t.where("X > threshold")`. That is the most common pattern in the docs. The real distinction is shared *mutable* state and order-dependence. Suggested fix: "doesn't modify shared state, and doesn't read state that another formula modifies."
4. **Audience fit.** "Partition filters" (lines 327–331) assumes the reader knows which tables have partitioning columns: Parquet or Iceberg sources laid out in partition directories. "Update Graph Processor" (line 87) is a legacy internal name. Suggested fix: add one clause on where partitioning columns come from, and use neutral names for the thread pools.
5. **Scope: configuration values are scattered through the narrative.** Property names and defaults appear at lines 50, 66, 83, 87, 124, 126, and 320–323. Suggested fix: move them into one Configuration section at the end, or link to `../query-table-configuration.md`. See the Structure and Style sections.

## Accuracy

**A1. The prescriptive rows say `with_serial` alone is enough. It isn't whenever more than one column touches the resource.**
- Where: Quick reference lines 18, 20, 22; Choosing an approach line 338; Key takeaways line 346; line 213 ("global state updates happen sequentially without race conditions").
- Source: `table-api/.../ConcurrencyControl.java` `withSerial` promises only "The expression will never be invoked concurrently with itself." When `SERIAL_SELECT_IMPLICIT_BARRIERS` is false, which is the default, "no additional ordering between selectable expressions is imposed."
- A counter or a file shared by two columns needs `with_serial` plus barriers (or implicit barriers). "Forces single-threaded access" (line 22) overstates the contract: a different column can call the same library at the same time.
- Line 144 ("you often need both") is correct, but the summary tables drop that qualification.
- Fix: in each row, say "one column" or add "+ barrier if several columns share it." The Groovy sibling repeats lines 18 and 22 (its lines 18 and 22).

**A2. The GIL callout is wrong across columns (line 75, "they're never run concurrently").**
- Within a column, the callout is right. `FormulaColumnPython.isParallelizable()` and `DhFormulaColumn.isParallelizable()` return false unless `PythonFreeThreadUtil.isPythonFreeThreaded()`, and `ConditionFilter.permitParallelization()` returns false for Python filters on a GIL build.
- Across columns, it is wrong. `SelectColumnLayer.allowCrossColumnParallelization()` returns `selectColumn.isStateless()`, not `isParallelizable()`. Python columns are stateless by default (`FormulaColumnPython.isStateless` returns `STATELESS_SELECT_BY_DEFAULT`). Each layer is `jobScheduler.submit`-ted, so two Python columns in one `update` run on separate threads. Their execution interleaves under the GIL.
- Tables updating in parallel through the update graph also run Python concurrently.
- So the counter race is real on a standard build. That is exactly the A/B interleaving shown at lines 182–188.
- Fix: "On a GIL build, Deephaven doesn't split a Python formula's rows across threads. Different Python columns and tables can still run at the same time."

**A3. "What does NOT get parallelized" lists deferred operations as if they were serial (line 70).**
- `view`, `update_view`, and `lazy_update` produce `ViewColumnSource`s. Their formulas run on whatever thread reads the column, which includes a parallel `update` or `where` downstream, and `view`/`update_view` can evaluate the same row more than once.
- `QueryTable.viewOrUpdateView` guards against this: it throws "A stateful column cannot safely be used in a view or updateView." and "view and updateView cannot respect barriers".
- "Not computed upfront" is true. "Not parallelized" is the wrong model.
- Also, "Operations waiting for dependencies" (line 72) is not a kind of operation. Cut it.

**A4. The IMPORTANT box at line 156 leaves out `lazy_update`.** `QueryTable.lazyUpdate` has no stateful or barrier guard, so `with_serial` there is accepted silently and guarantees nothing. It's worse than `view`: the call doesn't fail, it just gives no guarantee. Mention it.

**A5. The breaking-change history is overstated (line 9).**
- The claim that 40 "assumed all formulas required sequential processing" is wrong for selectables. In v0.40.9, `DhFormulaColumn.isStateless()` still returned true when all params were immutable types and all used columns were stateless.
- Filters were serial by default in 40 (`permitParallelization()` returned `STATELESS_FILTERS_BY_DEFAULT`, which was false).
- The change to default true is commit 8ea55b6a1d (DH-20714), first tagged in v41.x. The version number is correct.
- "Will now produce incorrect results" should be "can."
- The Groovy sibling's line 9 repeats this.

**A6. The counter example's "fix" doesn't fix the problem it shows (lines 175–211).**
- The broken version has columns A and B. The fixed version has a single column, `ID`.
- Applying `with_serial` to both A and B would still interleave them, because implicit barriers are off by default (A1).
- Line 190 says "`B` not following `A + 1`." That implies the reader should expect row-by-row evaluation across columns. Deephaven evaluates column by column, so even with both columns serial and a barrier, B is never A+1. That teaches the wrong model; remove it.
- "gaps (no 10-19 visible)" is meaningless in a 5-row excerpt of 5M rows.
- The Groovy sibling's line 170 repeats this.

**A7. The barrier example's "without a barrier" claim can't be reproduced as written (lines 244 and 280).**
- In `empty_table(10).update([col_a, col_b])` with both columns serial, every layer returns `allowCrossColumnParallelization() == false`. `QueryTable` then picks `ImmediateJobScheduler`, and the layers run in index order.
- Without the barrier, today's engine would still produce 0–9 and 10–19.
- The barrier is needed for the *guarantee* (per the javadoc), not to change this run. "Both columns would start simultaneously, both read `counter = 0`" is doubly wrong: the columns wouldn't start together here, and a real race interleaves values rather than both reading 0.
- Fix: "Without the barrier, the engine doesn't guarantee A finishes before B starts, so the result can change between runs or versions."

**A8. The Quick reference row for "Column A must finish before Column B → Barriers" (line 19) leaves out the automatic case.** `ConcurrencyControl` says: "if column B references column A, then the necessary inputs from column A are evaluated before column B." Barriers are only needed when the dependency is hidden, through shared state. Say so, or readers will add barriers they don't need.

**A9. The Implicit barriers section is muddled (lines 320–323).**
- `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is a Java field, not a setting a user can change. The property is `QueryTable.serialSelectImplicitBarriers`. The section presents both as if they were separate things.
- Its default is `!STATELESS_SELECT_BY_DEFAULT` (`QueryTable.java` line 401). So "off by default" holds only while `statelessSelectByDefault=true`. A user who flips that setting (line 126) also turns implicit barriers on.
- "Stateless mode" and "Stateful mode" aren't engine terms.

**A10. "Concurrency control works the same way for `Filter` as it does for `Selectable`" (line 135) is contradicted at line 234.** Per the javadoc, for a filter, serial "acts as an absolute reordering barrier." For selectables, it depends on the implicit-barriers setting, which applies only to selectables. The Groovy sibling's line 124 repeats this.

**A11. "Choosing an approach" recommends `with_serial` for "cumulative calculations" and "processing events in chronological sequence" (line 338).**
- Cumulative calculations have a dedicated, parallel-safe operation (`update_by`, e.g. `cum_sum`). Recommending a global accumulator with `with_serial` sends readers toward an anti-pattern.
- Row-set order is chronological only if the table is sorted by time.
- The Groovy sibling's line 293 repeats this.

**A12. The "What gets parallelized" list is incomplete (lines 62–66).** Grepping for job-scheduler use in `engine/table/.../impl` also turns up `updateby/UpdateBy.java`, `rangejoin/RangeJoinOperation.java`, `sources/UnionSourceManager.java` (merge), `remote/ConstructSnapshot.java`, and source-location initialization. At least `update_by` is user-relevant. Either add the missing operations or say "for example."

**A13. The partition filters section is accurate, with one omission (line 331).** `PartitionAwareSourceTable.where` stops prioritizing *every* filter after the first serial filter, not just a serial partition filter: "once we've found a serial filter, then we cannot prioritize any filter." Prioritization doesn't consult statelessness at all (`isPrioritizablePartitioningFilter`), which confirms "treats it as stateless even when configured stateful." Barriers also block prioritization (`missingBarrier`).

**Verified correct:**
- `QueryTable.minimumParallelSelectRows` is `1L << 22` (4,194,304).
- `where` parallelizes when `numberOfRows / 2 > parallelWhereRowsPerSegment` (65,536).
- `minimumParallelSortRows` is `1L << 20`, and `parallelSort` defaults to true.
- `statelessSelectByDefault` and `statelessFiltersByDefault` both default to true.
- `PeriodicUpdateGraph.updateThreads` and `OperationInitializationThreadPool.threads` both default to -1, which resolves to `availableProcessors()`.
- Parallel update-graph notification processing starts when `updateThreads > 1`.
- `Barrier` may be declared by at most one operation.
- Python APIs: `Selectable.parse`, `with_serial`, `with_declared_barriers`, `with_respected_barriers` (a sequence is accepted), `deephaven.filters.is_null`/`not_`, `deephaven.concurrency_control.Barrier`.
- `time_table` is append-only, so using `i` at line 38 is legal.
- The "Multiple barriers" execution order at line 314 is correct.
- Internal links and anchors all resolve at f2ef483084: `Selectable.md#with_serial`, `update.md#serial-execution`, `where.md#serial-execution`, the crash-course page, `dag.md`, `engine-locking.md`, `query-table-configuration.md`, and all six in-page anchors.
- External links need a manual check: the pydoc Barrier and ConcurrencyControl pages, python.org free-threading, and the Oracle `availableProcessors`.

**Inbound links (checked across the whole corpus at f2ef483084):** other pages link to `#serialization` (3 Python, 3 Groovy) and `#barriers` (4 Python, 4 Groovy) from `query-table-configuration.md`, `Filter.md`, `Selectable.md`, `view.md` and `update-view.md`. Any structural rename of "Serialization" or "Barriers" must keep those anchors.

**Follow-up outside this doc:** the `ConcurrencyControl.java` javadoc (`table-api`, lines 42–43) says `serialSelectImplicitBarriers` defaults "to the value of `statelessSelectByDefault`." The code uses the negation. The doc follows the code, which is correct, but the javadoc should be fixed.

## Structure

1. **The same distinction is explained three times (lines 139–144, 283–285, 333–339), plus the Quick reference.** "`with_serial` vs. barriers / you often need both" appears at line 144, in the IMPORTANT box at line 283, and again at line 285. "Choosing an approach" (lines 333–339) re-teaches the Quick reference. Suggested fix: keep the compare/contrast at 139–144 and fold 283–285 into one sentence after the barrier example. Either merge "Choosing an approach" into the Quick reference, with links, or cut it down to a pointer.
2. **Configuration values sit in the narrative instead of in one section** (lines 50, 66, 83, 87, 124, 126, 320–323). Suggested fix: add a "Configuration" section before Related documentation that lists all of them. The narrative then says "once a table is large enough" or "uses all cores by default."
3. **The GIL callout is in the wrong place (lines 74–77).** It sits under "Within a single table," but it governs everything Python-specific and its second paragraph is about `with_serial`. Suggested fix: move paragraph 1, once corrected, to a short "Python and the GIL" subsection after "Query phases and thread pools." Move paragraph 2 into Serialization.
4. **The intro's list doesn't match the headings (line 26).** The intro says "three ways: across tables, across rows, and across columns," but the headings are "Across tables" and "Within a single table," with rows and columns as bold paragraphs. The link text "[Thread pools]" (line 50) doesn't match its heading, "Query phases and thread pools." Suggested fix: make the list two-level, or make it three headings. Rename the link text.
5. **The stateless examples block (lines 103–121) shows four near-identical examples of the same point.** Two would do.
6. **Stateful partition filters (lines 327–331) is a loose aside.** It has a bridge sentence but no row in the Quick reference and no mention in the takeaways. Suggested fix: move it after "Serial filters" as a `####`, since it's filter-specific, or move it into the Configuration or advanced area.
7. **One list mixes different kinds of item (lines 68–72).** An operation class, a user marking, and "operations waiting for dependencies" (not an operation) share one list. This goes away with A3.
8. **Strengths to keep:** the Quick reference is placed early, and the page moves in a sensible order: model, then safety, then controls, then choosing.

Re-verify note: any section moved or renamed under points 1, 3 and 6 needs a spot-check per claim and the inbound-anchor constraint above (`#serialization`, `#barriers`).

## Examples

1. **The counter "fix" (lines 194–211) doesn't match the problem (lines 162–178).** Use a before-and-after pair on the *same* two-column query. Make the corrected version the barrier example (lines 246–278), which already exists, so the page teaches one scenario that builds up instead of three counter variants.
2. **The serial filters example (lines 219–232) has no side effects.** `is_null`/`not_` filters are pure, so the example contradicts its own lead-in ("when a filter has stateful side effects"). Use a filter that calls a stateful function, or relabel it as syntax only.
3. **The multiple-barriers example (lines 291–312) uses pure formulas.** No barrier is load-bearing, so it shows syntax, not why you'd need the barriers. Acceptable if framed as "syntax for multiple dependencies." Otherwise tie it to the counter.
4. **The serial fix (line 210) runs 5,000,000 Python calls in a tested block.** That is slow for the snapshotter, and on a GIL build the row threshold it seems sized to exceed doesn't apply to Python anyway. 10–1,000 rows would do.
5. **The `skip-test` block (line 162) is reasonable** because its output is nondeterministic. The hand-made output table (lines 182–188) is unvalidated, so label it clearly as illustrative.
6. **Line 112 uses a char literal, `' '`.** Deephaven examples use backtick strings: `` "FullName = FirstName + ` ` + LastName" ``.

## Style

- **Configuration detail wedged into sentences (7 instances).** Examples: line 50, "(This depends on `PeriodicUpdateGraph.updateThreads` being greater than 1…)", and line 66, "(`QueryTable.minimumParallelSortRows`, about 1 million rows by default)". Move these to the Configuration section; don't re-sentence them in place.
- **Home-made labels (the heading/label pattern, about 6 instances).** "Across tables" and "Across rows" should be "concurrent table calculations" and "concurrent row calculations." "Stateless mode" and "Stateful mode" (lines 322–323) should be named by the actual setting.
- **"Stateless" and "thread-safe" used as if they were synonyms (3 instances):** line 17 "Thread-safe, no shared state", line 337 "and is thread-safe", line 346. Pick one term per property.
- **Links to pydoc where internal reference pages exist (4 instances).** `Barrier` (lines 137, 240) and `ConcurrencyControl` (lines 153, 356) link to pydoc, but `reference/query-language/types/Barrier.md` and `ConcurrencyControl.md` exist at f2ef483084. `with_serial` links to `Selectable.md#with_serial` (line 9) but to `update.md#serial-execution` at line 153; pick one. `with_declared_barriers` and `with_respected_barriers` are never linked in prose.
- **Passive voice (about 5 instances):** "This is handled by the **Operation Initialization Thread Pool**" (line 83, and the same at line 87) → "The operation-initialization thread pool handles this…". Also "these are lazily evaluated" (line 70).
- **Title Case bold for things that aren't proper nouns:** "**Operation Initialization Thread Pool**", "**Update Graph Processor Thread Pool**" (lines 83, 87).
- **Mechanical checks were clean:** no dot-prefixed method names in prose, no empty `()` in prose, no `[here]` links, no curly quotes, and em dashes are spaced correctly. The en dashes in "0–9" are number ranges, not parenthetical dashes. There is one future-tense "will" (line 9).

## Author queries

- **AQ1** [Within a single table, line 75]: Is it intended that Python-backed columns run across columns in parallel on a GIL build? `allowCrossColumnParallelization` checks `isStateless`, not `isParallelizable`. Confirm before rewording the callout.
- **AQ2** [line 77]: "The engine may still evaluate a non-parallelizable column out of order." Is there a real code path where `doSerialApplyUpdate` doesn't follow row-set order, or is this contract-only? If it's contract-only, say "isn't guaranteed to" instead.
- **AQ3** [line 75]: Is "no other Deephaven configuration is required" for free-threaded Python true? Are there jpy or build requirements?
- **AQ4** [line 9]: How should the 40-vs-41 change be described, given that pure selectables were already treated as stateless in 40? One option: "filters and formulas with query-scope objects were serial by default."
- **AQ5** [Stateful partition filters, line 331]: What does "avoid repeated evaluation" refer to in the implementation?
- **AQ6** [Implicit barriers]: The `ConcurrencyControl` javadoc and the code disagree on the `serialSelectImplicitBarriers` default. Confirm the code is authoritative and file a javadoc fix.

## Strengths

1. The Quick reference table appears early, which is the right move for a long concept page. Keep it once its rows are corrected.
2. The "`with_serial` vs. barriers" comparison (lines 139–144) is clear and sits where both terms are first named.
3. The thresholds note (line 124) honestly says the small examples don't actually run in parallel. Keep that idea at a lower level of detail once the numbers move to Configuration.