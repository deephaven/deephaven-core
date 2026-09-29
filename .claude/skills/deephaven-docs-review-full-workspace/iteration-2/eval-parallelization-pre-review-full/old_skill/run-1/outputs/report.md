## Editorial summary

**Category:** Concept guide. It lives in `docs/python/conceptual/query-engine/`. The sidebar lists it under Best practices → Performance, but that placement is only for discoverability and doesn't change the category. Everything was verified against source at `/Users/margaretkennedy/dhc-skills-chip`; links were checked at commit f2ef483084, where the snapshot is byte-identical to `docs/python/conceptual/query-engine/parallelization.md`.

The page has a good spine: parallel by default, then `with_serial`, then barriers, told through one counter example that builds as it goes. But it gives the reader two wrong ideas about the engine:

- **Deferred operations:** `view`, `update_view` and `lazy_update` are presented as "not parallelized", as if they were safe.
- **The Python GIL:** the page says that on a standard (GIL) build, Python-backed formulas never run concurrently. That's false for columns. Two Python columns in the same `update` can still run on different threads.

The second error also undercuts the counter examples: their narrated behavior doesn't match what the engine actually does. Configuration detail is also scattered through the explanations.

**Verdict: needs revision.** It doesn't need restructuring. The outline mostly works; the problems are the wrong ideas, the examples, and where configuration detail sits.

## Developmental notes

1. **Two core claims about what runs in parallel are wrong** (How parallelization works, lines 68–77). Readers will conclude that deferred columns are a safe home for stateful formulas, and that a standard Python build protects them from races. Both are false (see Accuracy 1–2). Fix the claims first: every later section builds on them.
2. **Purpose and key message are mostly there, but split up.** After reading, the reader should be able to decide whether their query needs `with_serial`, a barrier, or both, and write it. The line that most queries need no changes appears at line 130 and in Key takeaways (343), not in the intro. Suggested intro sentence: "Deephaven parallelizes queries for you; most need no changes — the controls below are for formulas with side effects or order dependence."
3. **Too much implementation and configuration detail for a concept guide.** Thread pool property names, row thresholds, `SERIAL_SELECT_IMPLICIT_BARRIERS`, and partition-filter internals interrupt the explanation. They belong in one Configuration section at the end, or behind a link to `query-table-configuration.md`.
4. **The progression is sound once the thread-pool detour moves.** The order is how it works, then when it's safe, then how to control it, then choosing, then takeaways. Only the "Query phases and thread pools" section (79–89) breaks it (see Structure 3).
5. **Audience fit.** "Update graph", "notifications", "selectables", "barriers" and "partitioning columns" all appear before they're introduced (see Structure 6).

## Accuracy

Listed by impact; each item gives its location.

1. **Wrong idea: deferred operations listed as "not parallelized"** (line 70). `view`, `update_view` and `lazy_update` all produce a `ViewColumnSource` (`AbstractFormulaColumn.getViewColumnSource`, line 248). Their formulas run later, on whichever thread reads the column, and that reader can be a parallel `update`, `where`, or chunked read. A `view`/`update_view` formula can also run again on every read of the same row.
   - The engine itself refuses order controls here: `QueryTable.java:2036` throws "view and updateView cannot respect barriers", and `:2062` throws "A stateful column cannot safely be used in a view or updateView" when `statelessSelectByDefault` is true (the default).
   - "Upfront" is also ambiguous: it could mean static tables, initialization, or update cycles.
   - Fix: reclassify these as "deferred: computed when read, possibly concurrently and more than once — keep their formulas pure." The IMPORTANT callout at line 156 is consistent with this. In Python the question is partly moot, because `view`, `update_view` and `lazy_update` accept only strings (`table.py:1389/1408/1427`).
2. **The GIL caution overstates the protection** (line 75). "On a standard (GIL-enabled) build, they're never run concurrently" is true for filters but not for formulas:
   - Filters: `ConditionFilter.permitParallelization`, lines 806–817, returns false without free threading. Correct as written.
   - Formulas: `isParallelizable()` only controls splitting one column's rows. `SelectColumnLayer.java:115–117` requires `isStateless() && isParallelizable()` for that split.
   - Running different columns at the same time is controlled separately: `allowCrossColumnParallelization()` returns `selectColumn.isStateless()` (`SelectColumnLayer.java:657–659`).
   - Python formulas report themselves stateless by default: `FormulaColumnPython.isStateless` returns `STATELESS_SELECT_BY_DEFAULT`, and `DhFormulaColumn.isStateless` returns true.
   - So two Python-backed columns in one `update` can run concurrently on different threads even with the GIL. That's exactly the counter race on line 175.
   - Fix: "on a standard build, a Python-backed formula isn't split across rows, but it can still run at the same time as other columns." The second paragraph (line 77) is accurate; keep it.
3. **"Implicit barriers" misnames the setting and invents two modes** (lines 320–323).
   - `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is a Java static field, not a property.
   - The property is `QueryTable.serialSelectImplicitBarriers`. Its default is `!STATELESS_SELECT_BY_DEFAULT` (`QueryTable.java:400–402`): false by default, and true automatically if `statelessSelectByDefault=false`.
   - "Stateless mode" and "Stateful mode" aren't engine terms, and they blur this setting with `statelessSelectByDefault`.
   - The setting applies only to selectables. A serial filter always acts as an absolute reordering barrier (`ConcurrencyControl.java`, `withSerial` javadoc).
   - Separately, that javadoc says the property defaults "to the value of `statelessSelectByDefault`", which contradicts the code. That's a source-side bug for follow-up (AQ3).
4. **The barrier example's narration is wrong or can't be reproduced** (lines 244 and 280).
   - "Both read `counter = 0`" is false: the columns share one counter, so without ordering their increments would interleave. Neither restarts at 0.
   - "Without the barrier, both columns would race" doesn't happen for this exact code. Both columns are serial, so neither allows cross-column parallelism. `anyParallelColumns()` is then false, and `QueryTable.java:1825–1830` picks `ImmediateJobScheduler`, which runs the layers one after another on the calling thread.
   - The barrier is still the only guarantee, because nothing promises layer order, so keep it. Reword to "nothing guarantees A finishes before B starts."
   - "Without `with_serial`, rows within each column would also race" doesn't happen either: 10 rows is below `minimumParallelSelectRows`, and a Python UDF on a GIL build isn't split. The real risk without `with_serial` is the two columns running at the same time.
5. **The race output and its explanation don't match the engine** (lines 182–190).
   - Out-of-order values inside one column (A=5 before A=4) need row splitting, which a Python UDF doesn't get on a standard build.
   - "B not following A + 1" contradicts the page's own model, where serial columns give A = 0..N−1 and B = N..2N−1.
   - "Gaps (no 10–19 visible)" has no meaning for a 5-row excerpt of 5 million rows.
   - The failure that can really happen is the two columns interleaving. The block is `skip-test`, so this output was never generated (AQ1).
6. **The "fix" doesn't fix the query it follows** (lines 192–211). The broken query has two columns, A and B. The fix switches to a single column, `ID`. For the two-column version under default settings, `with_serial` on both isn't guaranteed to be enough: there are no implicit barriers, so a barrier is needed, which is what the next section says. The same gap appears in Quick reference ("Global counter → `with_serial`") and in Choosing an approach (line 338).
7. **Line 93 implies the engine detects statelessness.** "Deephaven parallelizes operations that are **stateless**" reads as detection. In fact the engine *assumes* statelessness by default: `DhFormulaColumn.isStateless` returns true when `STATELESS_SELECT_BY_DEFAULT` is set, and the filter side does the same. Key takeaways (345) says it correctly. Reword line 93 to say the engine assumes, and the user is responsible for telling it otherwise.
8. **The "What gets parallelized" list is incomplete** (lines 62–72).
   - Missing: `update_by` (`UpdateBy.java:307–340` builds parallel job schedulers), `range_join`, parallel snapshots (`ConstructSnapshot`, `QueryTable.enableParallelSnapshot`), and merged-table source management.
   - "Operations waiting for dependencies" isn't a kind of operation.
   - Either complete the list or frame it as examples (AQ5).
9. **Row splitting is described as unconditional** (lines 58 and 83). The size threshold only appears in the NOTE at line 124, about 65 lines later. Splitting also needs no row shifts, a destination that isn't redirected, and more than one thread (`SelectColumnLayer.java:115–117, 203–208`). Fix at the right level: "once the table is large enough to be worth splitting."
10. **Overstated guarantees in Quick reference.** "File I/O or logging → Serialize access to shared resource" and "Non-thread-safe library → Forces single-threaded access" promise more than the contract gives. The contract is "never be invoked concurrently with itself" (`ConcurrencyControl.java`). Two different columns or filters using the same file or library can still run at the same time. Line 148's "on a single thread" also names something the javadoc doesn't promise. It's harmless, but "never runs concurrently with itself, rows in row-set order" is the exact wording.
11. **The page contradicts itself about `Filter`** (line 135). "Concurrency control works the same way for `Filter` as it does for `Selectable`" conflicts with line 234 (a serial filter can't be reordered against other filters) and with the javadoc, which treats serial filters and selectables differently. Qualify line 135.
12. **The breaking-change callout needs tightening** (line 9).
   - Verified: commit 8ea55b6a1d (DH-20714) first ships in v41.0.0; the last tag without it is v0.40.9. But "Deephaven 40" isn't a release name. `query-table-configuration.md:238` says "Starting in Deephaven Core 41".
   - "Required sequential processing" overstates the old behavior. Before 41, "stateful" meant no row splitting within a column, plus implicit barriers between stateful columns, and it also applied to filters.
   - "Will now produce incorrect results" should be "can".
13. **Line 50: "updateThreads > 1 … which is the default."** The default is `-1`, meaning `availableProcessors()` (`PeriodicUpdateGraph.java:55, 141`), so it's only >1 on a multi-core machine. Minor; move it to the Configuration section anyway.
14. **Partition filters (lines 329–331) are mostly right, but the definition is incomplete.** `PartitionAwareSourceTable.isPrioritizablePartitioningFilter` (line 422) doesn't consult statelessness, so "treated as stateless unless marked serial" holds. But eligibility also needs a filter that isn't refreshing, doesn't use `i`/`ii`/`k`, and isn't a reindexing filter. It also only applies to partition-aware source tables. And once any serial filter appears, it and every filter after it lose prioritization (lines 332ff) (AQ4).
15. **Verified correct:**
   - Property names and defaults:
     - `minimumParallelSelectRows` = 1<<22
     - `parallelWhereRowsPerSegment` = 1<<16, with `numberOfRows / 2 > segment` (`AbstractFilterExecution.java:732`)
     - `minimumParallelSortRows` = 1<<20
     - `parallelSort` = true
     - `statelessSelectByDefault` / `statelessFiltersByDefault` = true
     - `OperationInitializationThreadPool.threads` = -1
     - `PeriodicUpdateGraph.updateThreads` = -1
   - Python API: `Selectable.parse`, `with_serial`, `with_declared_barriers`, and `with_respected_barriers` (accepts a sequence); `deephaven.filters.is_null` / `not_`; `update` accepting one `Selectable`.
   - "Each barrier can only be declared by one operation" matches "declared by at most one".
   - The distinction between "not concurrent" and "row-set order" matches the `SelectColumn.isParallelizable` javadoc.

**Links**

- **L1 — an inbound link is broken (from the corpus-wide scan).** `docs/python/how-to-guides/predicate-pushdown.md:13`, and the Groovy copy, link to `…/parallelization/#controlling-concurrency-for-select-update-and-where`. That heading doesn't exist. The current heading is "Controlling execution order", so the anchor is `#controlling-execution-order`. The link is also an absolute deephaven.io URL rather than a relative path. The other inbound anchors (`#serialization`, `#barriers`) resolve.
- **L2 — external links where internal pages exist.** `Barrier` (lines 137, 240) and `ConcurrencyControl` (line 153, Related documentation) point to the external Pydoc. This PR adds `reference/query-language/types/Barrier.md` and `ConcurrencyControl.md`; link those instead.
- **L3 — inconsistent `with_serial` targets.** Line 153 links `with_serial` to `update.md#serial-execution`; everywhere else it goes to `Selectable.md#with_serial`. The filter version is `Filter.md#with_serial`.
- **Resolved:** every other internal link and anchor resolves at f2ef483084.

## Structure

1. **Headings don't match the enumeration** (line 26 vs. lines 28/54). The intro names three ways: across tables, rows, and columns. The headings then split it two ways ("Across tables" / "Within a single table") and turn rows and columns into bold paragraphs. "Query phases" then brings "across tables" back only for updates. Either use three `###` headings matching line 26, or rewrite line 26 as "two ways, the second in two forms."
2. **Configuration detail is injected into the narrative** (lines 50, 66, 83, 87, 124, 126, 320–323). Consolidate into one Configuration section at the end, or link to `query-table-configuration.md`. Move the GIL caution out of "Within a single table" into a short Python-build note, placed near "When parallelization is safe by default".
3. **Topic interleaving and a split explanation.** "Query phases and thread pools" (79–89) is an implementation and configuration detour between the explanation of how parallelization works and when it's safe. Move it to the Configuration section. The NOTE at line 124 finishes the row-splitting explanation from line 58, but in a different section. Merge it into line 58 at the right level of abstraction.
4. **The same content is explained more than once.**
   - The `with_serial` vs. barriers contrast appears at 139–144, again in the IMPORTANT callout at 283, then 285 and 346–347.
   - "Choosing an approach" (333–339) repeats Quick reference (13–22).
   - "Most queries don't need serial" appears at 130, 151 and 343.
   - Keep the early contrast at 139–144 (good) and cut the IMPORTANT callout to a back-reference. Merge Choosing an approach into Quick reference, or the other way round.
5. **The Quick reference rows are different kinds of thing.** It mixes categories ("Pure column math"), specific examples ("Global counter"), items that don't meet its own test ("logging" doesn't change output unless order matters), and a setting rather than a solution ("implicit barriers"). Keep all rows as scenario categories, with examples in a separate column.
6. **Terms are used before they're defined.**
   - "Barriers" and "implicit barriers" appear in the table at line 19 but are defined at 240 and 320.
   - "Update graph" and "notifications" appear at line 50; the link only comes at 52.
   - "selectables" appears at line 75; Key concepts comes at 132.
   - Move Key concepts earlier, or add forward references.
7. **Two sections sit apart from the flow.** "Stateful partition filters" (327) depends on partitioned source tables, which the page never introduces; move it to the Configuration section or the `where`/`Filter` reference. "Implicit barriers" (318) is a setting; move it to the Configuration section with a one-line pointer.
8. **Re-verify step:** this was a report-only review, so no structural edits were applied and no spot-checks were needed. The corpus-wide inbound-link scan was run anyway and found L1.

## Examples

1. **No true wrong-then-right pair** (lines 162–211). The fix changes the query from two columns to one. Show the same two-column query fixed, then point ahead to the barrier section for the cross-column part (see Accuracy 6).
2. **The serial-filter example shows no side effects** (lines 219–232). The lead-in promises filters "with stateful side effects", but `is_null` / `not_` have none. The lead-in also says string filters are parallelized, but the example never shows `Filter.from_("…").with_serial()` (`filters.py:118`), the string-filter equivalent of `Selectable.parse`. Use a filter that calls a Python function that records or counts.
3. **The barrier example's prose contradicts what the code does** (see Accuracy 4). The code is right; fix the sentences around it.
4. **The multiple-barriers example has no observable effect** (lines 291–312). Pure arithmetic with no shared state gives the same output with or without barriers. Say it shows the API shape only, or make the ordering visible.
5. **The runnable counter fix uses 5,000,000 rows** (line 210). That's 5 million serial Python calls in the docs snapshot test, and the size only mattered for the race demo. Use 10 rows, as the barrier example does. The race block's `skip-test` is justified (its output isn't deterministic), but its printed output was never generated (AQ1).
6. **The stateless examples are four near-identical cases** (103–121). Two would do. `FirstName + ' ' + LastName` uses a char literal, which works through char concatenation; a backtick string `` ` ` `` matches the rest of the page.

## Style

- **Made-up labels (pattern, about 10 uses):** "across tables / across rows / across columns" (lines 26, 28, 58, 60, 83, 87). Prefer standard terms, e.g. "concurrent table updates", "splitting rows across threads", "computing independent columns concurrently".
- **"Stateless" and "thread-safe" treated as synonyms (3 uses):** Quick reference "Thread-safe, no shared state" (17); line 337 lists both; line 346. Pick one meaning for each.
- **Mid-sentence configuration parentheticals (4 uses):** lines 50, 66, 83, 87 (see Structure 2).
- **Passive voice (about 5 uses):** "are lazily evaluated" (70), "This is handled by" (83, 87), "is controlled by" (320). Future "will" once (line 9).
- **Related documentation:** add the internal Selectable, Filter, Barrier and ConcurrencyControl pages, `query-table-configuration.md`, and the Crash Course; replace the external "ConcurrencyControl Pydoc" link.
- **Clean on the mechanical checks:** no method names with a leading dot, no empty `()` in prose, no `[here]` links, em dashes spaced correctly, sentence-case headings, straight quotes. First mentions of `select`, `update`, `where`, `with_serial`, `view` and `sort` are linked.

## Author queries

- AQ1 [Example: a counter needs serialization, lines 182–190]: Was this output captured from a real run, and on which Python build? The engine path suggests the race shows up only between the two columns, not out of order within one column.
- AQ2 [Within a single table, GIL caution, line 75]: Is it intended that Python-backed columns on a GIL build still run at the same time as other columns (`SelectColumnLayer.allowCrossColumnParallelization` returns `isStateless`)? The doc should state whatever the intended behavior is.
- AQ3 [Implicit barriers, line 320]: The `ConcurrencyControl.withSerial` javadoc says `serialSelectImplicitBarriers` defaults to the value of `statelessSelectByDefault`, but `QueryTable.java:401` uses its negation. Which is intended? The docs should follow the code; the javadoc needs a follow-up fix.
- AQ4 [Stateful partition filters, line 331]: Should the page say that one serial filter blocks prioritization of every filter after it, and name which table types have partitioning columns?
- AQ5 [What gets parallelized, line 62]: Is this list meant to be complete? If so, add `update_by`, `range_join`, snapshots and merge.
- AQ6 [Breaking change callout, line 9]: Which version naming do you prefer: "0.40 and earlier" / "Deephaven Core 41"?

## Strengths

- The counter-to-serial-to-barrier story builds one lesson on the last; keep that arc when fixing the example details.
- The `with_serial` vs. barriers contrast (139–144) comes right where both are first named, and "you often need both" is the right key idea.
- The GIL note's second paragraph separates "not concurrent" from "in row-set order" and matches the engine's javadoc exactly; keep it as written.

The Groovy sibling was out of scope, but it repeats several of these claims word for word (at f2ef483084: lines 64–66, 113, 170, 275–277). Whatever fixes land here should be mirrored there.