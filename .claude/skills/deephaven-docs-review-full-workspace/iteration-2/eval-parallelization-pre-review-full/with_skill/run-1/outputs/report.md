# Full review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857 snapshot, f2ef483084)

**Category:** Concept guide. It lives under `conceptual/`. It appears in the sidebar under Best practices and troubleshooting → Performance, but that placement is for discoverability and does not change the category. It gets the concept-guide profile: an explanatory tone, a page that builds a mental model, and a heavier weight on split explanations and configuration placement.
**Scope:** The Python file only, as you asked. I did not review the Groovy sibling. A quick grep shows it repeats the three biggest accuracy findings (quick-reference rows, the "NOT parallelized" list, and the 40-vs-41 callout), so those fixes need to go into both files.
**Mode:** Report only. I edited nothing.

---

## Editorial summary

The page is well-motivated and mostly accurate. The outline is sound: how it works, then when it's safe, then how to control it, then how to choose. The thresholds, defaults, and barrier rules all check out against source. The biggest problem is the mental model the summary sections teach. The Quick reference, "Choosing an approach," and Key takeaways all say `with_serial` alone handles a global counter, a non-thread-safe library, or file I/O. The engine only promises that a serial expression is "never invoked concurrently with itself," so that advice breaks as soon as two columns share the resource. That is exactly the situation in the page's own counter example. Two other classifications are also wrong: `view`/`update_view`/`lazy_update` as "not parallelized," and Python formulas "never run concurrently" on GIL builds. **Verdict: needs revision.** The structure doesn't need rebuilding. It needs accuracy fixes, one Configuration section, and a rebuilt counter example.

## Developmental notes

1. **The page teaches a wrong rule of thumb in its most-read sections** (Quick reference lines 18, 20, 22; Choosing an approach line 338; Key takeaways line 346). A reader who skims only the table will put `with_serial` on two columns that share a counter and still get races. The body already says the right thing ("you often need both," line 144) but the summaries drop it. Fix: split the rows by how many columns touch the resource, for example "Global counter used by one column → `with_serial`; shared by several columns → `with_serial` on each, plus barriers." (Details under Accuracy A1.)
2. **Purpose is clear and the key message comes early.** Line 6 says there's "no configuration required" and line 130 says "Most queries work correctly." But the Breaking-change callout (lines 8–11) sits between them and reads as an alarm before the reader knows what `with_serial` is. Consider opening with one plain sentence of the key message ("Most queries need no changes; the controls below are for formulas with side effects"), then the callout.
3. **Mental model the reader forms.** These are the page's core claims, as I handed them to the accuracy step:
   - (a) The engine parallelizes across tables, rows, and columns. Correct.
   - (b) `view`/`update_view`/`lazy_update` are not parallelized. **Wrong.**
   - (c) On GIL Python, Python formulas never run concurrently. **Partly wrong.**
   - (d) `with_serial` alone protects a shared resource. **Wrong when there's more than one column.**
   - (e) Barriers order columns, and shared state needs both. Correct.
   - (f) Deephaven 40 ran everything sequentially. **Overstated.**
4. **Audience fit.** Several terms are used before they're defined or never defined:
   - "update graph" (line 30; linked at line 52)
   - "notifications" (line 50)
   - "partitioning columns" and "location" (lines 329–331)
   - the page-coined names "Operation Initialization Thread Pool" and "Update Graph Processor Thread Pool" (lines 83, 87). These aren't class names, and "Update Graph Processor" is legacy vocabulary.
   - `with_serial`, barriers, and "implicit barriers" appear in the Quick reference (line 13) before the Key concepts list (line 132) defines them.
5. **Scope.** There are three kinds of detail that belong elsewhere:
   - About eight configuration properties are embedded in the narrative (see Structure S1).
   - The thread-pool subsection is mostly configuration.
   - "Stateful partition filters" (lines 327–331) is a niche data-source topic that the Quick reference and "Choosing an approach" never mention.

## Accuracy

Everything here comes from the accuracy step. Source is at `/Users/margaretkennedy/dhc-skills-chip`.

**A1. Prescriptive rows claim `with_serial` is enough on its own (high impact).**
- **Where:** Quick reference rows "Global counter," "File I/O or logging," and "Non-thread-safe library" (lines 18, 20, 22); "Choosing an approach" line 338; Key takeaways line 346.
- **Source:** `table-api/src/main/java/io/deephaven/api/ConcurrencyControl.java` `withSerial` javadoc: "The expression will never be invoked concurrently with itself," and "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed."
- **Default:** that setting is `false` by default (`QueryTable.java:400-402`, `!STATELESS_SELECT_BY_DEFAULT`).
- **Consequence:** two serial columns calling the same counter, logger, or library can still run at the same time. The row reason "Forces single-threaded access" (line 22) also overstates the contract.
- **Fix:** qualify each row by how many columns use the resource, and point the multi-column case at barriers or implicit barriers.
- **Same pattern in the body:** line 213, "global state updates happen sequentially without race conditions," and line 148, "on a single thread." Both need the same qualification.

**A2. "What does NOT get parallelized" misclassifies deferred operations (high impact).**
- **Where:** line 70.
- **Source:** `view`/`update_view`/`lazy_update` produce view column sources whose formulas run on whatever thread reads the cell. A parallel `update`, a parallel `where`, or a chunked downstream read can therefore evaluate them concurrently.
- **Engine guard:** `QueryTable.java` `viewOrUpdateView` throws `"A stateful column cannot safely be used in a view or updateView."` and `"view and updateView cannot respect barriers"`.
- **The PR's own reference pages** (`reference/table-operations/select/view.md:23` and `update-view.md:23`) say rows may be evaluated "by any thread," which contradicts this page.
- **Fix:** reframe these as "not computed when the table is created or updated; evaluated later on the reading thread, which may be one of many." Do not present them as not parallelized or safe for stateful formulas.

**A3. The GIL callout overstates what a GIL build guarantees (high impact).**
- **Where:** line 75, "on a standard (GIL-enabled) build, they're never run concurrently."
- **For filters it holds:** `ConditionFilter.permitParallelization` returns `false` for Python-backed filters unless the build is free-threaded.
- **For selectables it's only half true:**
  - `SelectColumnLayer.java:115-117` blocks splitting a column's *rows* across threads unless `sc.isParallelizable()` (`DhFormulaColumn.isParallelizable` / `FormulaColumnPython.isParallelizable` return `false` on GIL builds).
  - But `SelectColumnLayer.allowCrossColumnParallelization()` (line 657) checks only `isStateless()`. Each layer is submitted to the job scheduler on its own (`SelectColumnLayer.java` `jobScheduler.submit(...)`).
  - So on a GIL build, two Python columns in one `update` can still run on different threads, interleaving at GIL switches.
- **Fix:** "On a standard build, Deephaven doesn't split a Python formula's rows across threads, but separate Python columns in the same operation can still run at the same time." The second paragraph of the callout (line 77) is correct and worth keeping. It matches the `SelectColumn.isParallelizable` javadoc: "the engine may choose to evaluate it out-of-order."

**A4. The breaking-change callout overstates the pre-41 behavior (medium).**
- **Where:** line 9, "Deephaven 40 and earlier assumed all formulas required sequential processing."
- **Version is right:** the default flipped in `8ea55b6a1d` (DH-20714), and the first tag containing it is v41.0.0.
- **The description of 40 is not:** before the flip, `DhFormulaColumn.isStateless()` still returned `true` for formulas whose parameters were all immutable types and whose used columns were stateless. Many Java-only formulas were already parallel.
- **Also missing:** 41 turned implicit serial barriers *off* by default (`serialSelectImplicitBarriers` defaults to `!statelessSelectByDefault`, and it was `true` before). That is also a behavior change for code that relied on it.
- **Wording:** "will now produce incorrect results" should be "can." See AQ2.

**A5. The counter "without serialization" output is not reproducible as written (medium).**
- **Where:** lines 180–190, a `skip-test` block.
- **On a GIL build:** each column's rows run sequentially (A3), so `A` can't go "out of order" within its own column the way the table shows (row with `A=4` after `A=5`).
- **On a free-threaded build:** the 5M-row table is split into chunks of at least `MINIMUM_PARALLEL_SELECT_ROWS` = 4,194,304 rows (`SelectColumnLayer.java:205-208`). The first five rows are all in one chunk on one thread.
- **"Gaps (no 10-19 visible)"** is meaningless for five rows out of five million.
- **Fix:** describe the failure modes in words ("values interleave between A and B and can repeat") or show output that was actually observed. See AQ3.

**A6. "Concurrency control works the same way for `Filter`" (line 135) is overstated (medium).**
- The javadoc gives filters and selectables different semantics. "For a filter, serial acts as an absolute reordering barrier," while for selectables the ordering depends on `SERIAL_SELECT_IMPLICIT_BARRIERS`.
- Line 234 states the filter behavior correctly. Line 135 contradicts it.

**A7. Implicit barriers are described with a Java field name and misleading mode labels (medium).**
- **Where:** lines 320–323.
- **Field name:** `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is the Java field. The property readers can set is `QueryTable.serialSelectImplicitBarriers`.
- **Mode labels:** "Stateless mode (default)" and "Stateful mode" are really about `statelessSelectByDefault`. The property's default follows `!statelessSelectByDefault` (`QueryTable.java:395-402`), so setting `statelessSelectByDefault=false` also turns implicit barriers on. The page never says this.
- **Scope:** implicit barriers apply only to selectables (`SelectAndViewAnalyzer.java:176`).
- **Javadoc bug (source, not doc):** the `ConcurrencyControl.withSerial` javadoc says the property defaults "to the value of `QueryTable.statelessSelectByDefault`," but the code uses the negation. This is worth a separate source issue.

**A8. "What gets parallelized" is incomplete (medium).**
- **Where:** lines 62–66.
- The engine also parallelizes `update_by` (`UpdateBy.java` `iterateParallel`), range joins, and snapshot construction (`ConstructSnapshot`, `QueryTable.enableParallelSnapshot`), among others (from the `OperationInitializerJobScheduler`/`UpdateGraphJobScheduler` users).
- **Fix:** either list them, or retitle the list "Examples of operations that parallelize."
- **Mismatched item:** "Operations waiting for dependencies" (line 72) is a scheduling state, not an operation type. Cut it.

**A9. Small imprecisions**
- **Line 50:** "`PeriodicUpdateGraph.updateThreads` being greater than 1, which is the default." The default is `-1`, which resolves to `availableProcessors()` (`PeriodicUpdateGraph.java:55, 140-141`). It's greater than 1 only on a multi-core machine.
- **Line 124:**
  - Row splitting keys off the size of the update (added + modified rows), not the table (`SelectColumnLayer.java:192`).
  - Columns that return `Table`/`RowSet` bypass the threshold entirely (`SelectColumnLayer.java:204-207`).
  - "Evaluates the formula on a single core" is true per column, but separate columns can still run on different cores.
- **Line 52:** "Independent tables … run in parallel automatically" applies to update cycles, not to a script's initial, sequential creation of each table.
- **Line 156:** the `view`/`update_view` guard applies only when `statelessSelectByDefault=true` (the default). `lazy_update` has no guard at all (`QueryTable.lazyUpdate`). See AQ4.

**Verified correct (quoted source checked):**
- Defaults and thresholds:
  - `OperationInitializationThreadPool.threads` default `-1` → `availableProcessors` (`ThreadHelpers.getOrComputeThreadCountProperty`)
  - `minimumParallelSelectRows` `1L << 22` ≈ 4.2M
  - `parallelWhereRowsPerSegment` `1 << 16`, with the `numberOfRows / 2 >` test (`AbstractFilterExecution.shouldParallelizeFilter`)
  - `minimumParallelSortRows` `1L << 20`
  - `parallelSort` default `true`
  - `statelessSelectByDefault` and `statelessFiltersByDefault` defaults `true`
- Across-table concurrency: `ConcurrentNotificationProcessor` when `updateThreads > 1`.
- Barrier rules: "Each barrier can only be declared by one operation" (`addDeclaredBarriersToMap` throws "Duplicate barrier").
- Partition-filter behavior (`PartitionAwareSourceTable` `isSerial()` gating).
- All Python APIs and imports: `Selectable.parse`, `with_serial`, and `with_declared_barriers`/`with_respected_barriers` (each takes one `Barrier` or a sequence); `deephaven.filters.is_null`/`not_`; `deephaven.concurrency_control.Barrier`.

**Links**
- Every relative link resolves at f2ef483084, including the new `Selectable.md#with_serial`, `Filter.md`, `update.md#serial-execution`, `where.md#serial-execution`, and the Crash Course page. The in-page anchors resolve too.
- Two external links need a manual check: the python.org free-threading page and the Java 11 `availableProcessors` javadoc.
- **Missing from the links:** respected barriers must be declared by an expression to their *left* (`SelectAndViewAnalyzer.java:197-200`, "Respected barrier … is not defined"; javadoc: "It is an error to respect a barrier that has not already been defined per the natural left to right ordering"). Readers will hit this error, and the page never mentions it. Worth one sentence in Barriers.

**Re-verify step (applies to the proposed moves, since nothing was edited):**
- Other pages link into this one at `#serialization` (`query-table-configuration.md:238`, `view.md:23`, `update-view.md:23`) and `#barriers` (`Filter.md:91,112`; `Selectable.md:56,79`).
- Any rename of "Serialization" (see Style) or restructure of Barriers must keep those anchors or update all six links.
- No page links to `#query-phases-and-thread-pools` from outside, so folding that section into a Configuration section only affects the in-page link at line 50.
- I spot-checked the rewrites proposed in A1 and A3 against the javadoc and `SelectColumnLayer` cited above.

## Structure

**S1. Configuration detail is injected into the narrative (high).**
- **Where:** lines 50, 66, 83, 87, 89, 124, 126, and 320–323 all name properties or defaults mid-explanation.
- **Fix:** add one "Configuration" section before Key takeaways and give it a small table covering:
  - `PeriodicUpdateGraph.updateThreads`
  - `OperationInitializationThreadPool.threads`
  - `minimumParallelSelectRows`
  - `parallelWhereRowsPerSegment`
  - `minimumParallelSortRows`/`parallelSort`
  - `statelessSelectByDefault`/`statelessFiltersByDefault`
  - `serialSelectImplicitBarriers`
- **Where each topic lands:** link that section to `query-table-configuration.md`. Rewrite each narrative sentence at concept level, for example line 66 becomes "`sort`, once the table is large enough to be worth splitting." "Query phases and thread pools" can shrink to its two-phase explanation. "Implicit barriers" can become two sentences that point to the Configuration section.

**S2. The GIL callout sits at the wrong level (medium).**
- **Where:** it's under "How parallelization works › Within a single table" (lines 74–77), but it governs Python formulas in every section and matters most for Controlling execution order.
- **Fix:** move it to its own short subsection, such as "Python formulas and the GIL," right before "Controlling execution order," with a one-line pointer left behind.

**S3. Three summaries restate the same guidance and drift (medium).**
- **Where:** Quick reference (line 13), Choosing an approach (line 333), and Key takeaways (line 341). The `with_serial` sufficiency error (A1) is repeated in all three.
- **Fix:** keep the Quick reference as the early map, cut "Choosing an approach" down to links into the body (or fold it into the table's "Why" column), and keep Key takeaways to the key message.
- The Quick reference also uses `with_serial`, barriers, and implicit barriers before they're defined at line 132. Add a lead-in such as "Terms are explained in Controlling execution order."

**S4. Parent/child terminology doesn't match (medium).**
- **Where:** line 26 promises "three ways: across tables, across rows, and across columns," but the child headings are "Across tables" and "Within a single table," with rows and columns as bold labels.
- **Fix:** "at two levels: across tables, and within one table (across rows and across columns)."

**S5. Lists mix categories (medium).**
- **Quick reference rows** mix a formula type ("Pure column math"), a specific example ("Global counter"), an ordering requirement ("Column A must finish before Column B"), and resource types. Pick one kind, such as "what your formula does," and turn specific examples into categories.
- **"What does NOT get parallelized"** mixes operations, a user flag, and a scheduling state (A8).

**S6. "Stateful partition filters" is an orphaned aside (medium).**
- **Where:** lines 327–331. There's no bridge from Barriers, it defines "partitioning columns" and "location" only implicitly, and nothing in the Quick reference or the summaries points to it.
- **Fix:** add a bridge sentence ("If you filter partitioned source tables, such as Parquet or Iceberg…") and move it after "Choosing an approach," or move it to `Filter.md` and leave a link.

**S7. Terms are used before they're defined (low).** "Update graph" first appears at line 30 but is linked at line 52. Move the link to its first use.

**S8. Length and repeated examples (low).** The page is 356 lines, and the counter function is redefined in three blocks. The barrier example openly builds on the counter, which is legitimate. The defect is covered under Examples E1–E2. Consider `test-set` so the helper is defined once.

## Examples

**E1. The counter "wrong then right" pair doesn't match up (high).**
- The unsafe example (lines 162–178) uses two columns, `A` and `B`.
- The "fix" (lines 194–211) quietly drops to one column, `ID`. With two serial columns and default settings, the fix would *not* fix the two-column race (A1).
- **Fix:** have the fix show both columns, then either show `with_serial` plus a barrier (which is the barrier example, so merge the two), or say explicitly that the single-column form only covers the one-column case.
- **Performance:** the fix also runs a serial Python call 5,000,000 times in a docs snapshot. Use a small table, since row count isn't what the example demonstrates.

**E2. The unsafe output is invented and untested (medium).** The unsafe block is `skip-test` and its output table isn't reproducible (A5). Either describe the failure in words or label the output as illustrative.

**E3. The serial filters example doesn't show what its lead-in says (medium).**
- **Where:** lines 217–232. The lead-in says to use serial filters "when a filter has stateful side effects," but `is_null`/`not_(is_null)` are pure. Removing `with_serial` changes nothing, so the reader learns only the syntax.
- **Fix:** use a filter with a real side effect, such as the pattern at `reference/table-operations/filter/where.md:157` ("# Use with_serial because the filter has side effects"), or link to it.

**E4. The multiple barriers example has nothing to observe (low).**
- **Where:** lines 291–312. The formulas are pure, so the output is identical with or without barriers, and the lead-in "when columns have different dependencies" isn't reflected in the code.
- **Fix:** say it shows structure only, or add shared state so the ordering is visible.

**E5. The barrier example works well (keep it).** The barrier is load-bearing by contract, and the paragraph after it (line 285) explains when `with_serial` is and isn't needed.

**E6. The stateless examples (lines 103–121) are fine.** The NOTE at line 124 explains why the small tables don't actually parallelize. After S1 that NOTE should shrink to one line, such as "Small tables like these run on one core; the examples show what's safe, not a speedup."

## Style

- **Coined labels (pattern, about 6 instances).** "Across tables," "across rows," and "across columns" (lines 26, 28, 58, 60, 83, 87) are terse ad hoc labels. The style guide names these exact phrases. Prefer "concurrent table updates" and "concurrent row calculations."
- **Terms used as synonyms that aren't (pattern, 2 instances).**
  - "Stateless" and "thread-safe" are used interchangeably: line 17 ("Thread-safe, no shared state") and line 337.
  - Line 93 says "stateless … each row's result depends only on that row's input values." The standard term is "pure function." Pick one term and keep it.
- **Mid-sentence parentheticals carrying configuration or caveats (pattern, about 8 instances; see S1).** Line 50 "(This depends on `PeriodicUpdateGraph.updateThreads` …)", line 66, and line 83 "(default `-1`, …)". Per S1, the fix is a Configuration section, not a separate sentence in the same paragraph.
- **Passive voice (pattern, about 6 instances).** Line 83 "This is handled by the **Operation Initialization Thread Pool**" → "The operation initialization thread pool does this work." Also lines 70, 87 (twice), 141, and 234.
- **"Serialization" collides with data serialization** (Barrage, Arrow). This affects the heading at line 146 and lines 148 and 192. Prefer "Serial execution," which matches `update.md`/`where.md` "## Serial execution." Keep an `#serialization` anchor or update the six inbound links first (see Accuracy › Re-verify step).
- **Inconsistent link targets (2 instances).**
  - `with_serial` links to `Selectable.md#with_serial` at lines 9, 71, 136, and 346, but to `update.md#serial-execution` at line 153.
  - `Barrier` and `ConcurrencyControl` link to the external pydoc (lines 137, 153, 240, 356), but internal reference pages `reference/query-language/types/Barrier.md` and `ConcurrencyControl.md` exist at f2ef483084. Link those instead.
- **Heading doesn't match the body (low).** "Stateful partition filters" (line 327), but the body says "Partition filters."
- **Multi-idea paragraphs (low).** Lines 83, 87, and 331 each carry three or more claims. Split them.
- **Future tense (low, 1 instance).** Line 9 "will now produce" → "now produces" (or "can produce," per A4).
- **Code comments that restate the code (low).** "# Column A declares barrier_a" (line 299) and "# Create filters with serial evaluation" (line 223). Say *why* instead.
- **Clean on the mechanical checks.** No dot-prefixed methods or empty `()` in prose, no `[here]` links, no curly quotes, and em dashes are spaced. En dashes appear only in numeric ranges (0–9), which is fine. Headings are in sentence case, and Related documentation is present.

## Author queries

- AQ1 [Python GIL limitation, line 75]: Is it intended that on GIL builds, separate stateless Python columns can run on different threads (`allowCrossColumnParallelization` checks only `isStateless`)? Please confirm with an engine SME before rewording.
- AQ2 [Breaking-change callout, line 9]: What exactly should the 40 → 41 description say, given that pre-41 formulas with immutable parameters were already stateless and implicit serial barriers defaulted to on?
- AQ3 [Example: a counter needs serialization, lines 182–190]: Where did the sample output come from? Was it observed on a free-threaded build, and at what table size?
- AQ4 [IMPORTANT callout, line 156]: `lazy_update` doesn't reject serial or stateful columns (`QueryTable.lazyUpdate` has no guard). Should the callout name it, and what should readers expect?
- AQ5 [Stateful partition filters, lines 327–331]: Who is this section for? Does it belong here, or on `Filter.md` or a partitioned-source page?
- AQ6 [Query phases and thread pools, lines 83, 87]: Are "Operation Initialization Thread Pool" and "Update Graph Processor Thread Pool" names you want to establish? Otherwise, use descriptive lowercase terms.

## Strengths

1. **The `with_serial` vs. barriers comparison where both are first named** (lines 139–144), including "you often need both." It's the right orientation at the right place. Keep it, and make the summaries agree with it.
2. **The "not the same guarantee" paragraph** (line 77) and the read-only-respecter nuance (line 285). Both are accurate distinctions that most readers would miss.
3. **The numbers are right.** Every threshold and default the page cites matches source. After S1 they only need a new home, not correction.

**Follow-up (out of scope here):** the Groovy sibling `docs/groovy/conceptual/query-engine/parallelization.md` has the same Quick reference rows (lines 18, 22), "NOT parallelized" list (line 66), and 40/41 callout (line 9). Fix A1, A2, and A4 there in the same pass.