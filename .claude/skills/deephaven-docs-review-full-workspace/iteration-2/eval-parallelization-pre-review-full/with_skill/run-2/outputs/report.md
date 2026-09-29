# Full review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857 snapshot, f2ef483084)

**Category:** Concept guide (directory `conceptual/`). The sidebar puts it under Performance next to how-to pages to make it easier to find. That placement doesn't change the category.
**Scope:** Python file only, as you asked. The Groovy sibling repeats most of the accuracy defects below (see Accuracy A10), but I didn't review it.
**Mode:** Report only. Nothing was edited.

## Editorial summary

The page has a sound skeleton: motivation, how it works, what's safe by default, how to control it, how to choose. Its central message, "Deephaven parallelizes for you; most code needs no changes," is stated early. The biggest problem is the mental model in the prescriptive parts. The Quick reference table, the breaking-change callout, and the takeaways all say `with_serial` alone fixes a shared counter, file I/O, or a non-thread-safe library. The contract only promises that the expression is "never invoked concurrently with itself." The page's own two-column counter needs a barrier as well, and the 41 release flipped implicit barriers off, which makes this more likely to bite. Several classifications are also wrong:
- `view`, `update_view` and `lazy_update` are listed as "not parallelized."
- On a standard GIL build, Python formulas are said to be "never run concurrently."
- Before 41, "all formulas" supposedly ran sequentially.

**Verdict: needs revision.** The structure mostly holds. It needs consolidation (three counter examples, three summary sections, configuration detail scattered through the narrative), not a rebuild.

## Developmental notes

1. **The prescriptive layer teaches "`with_serial` = thread safety."** This is in Quick reference (lines 18, 20, 22), the breaking-change callout (line 9), "Choosing an approach" (line 338) and Key takeaways (line 346). A reader who stops at the table, which is what the table is for, will under-protect shared state. Suggested fix: every row that involves a resource shared by more than one column, filter or table should read "`with_serial` + barrier (or implicit barriers)". The body already says this ("you often need both", line 144). The summaries drop it. See Accuracy A1.
2. **The page never says when you *don't* need a barrier.** The engine already orders B after A when B's formula references A. The `ConcurrencyControl` javadoc says: "if column B references column A, then the necessary inputs from column A are evaluated before column B." Without that sentence, the Quick reference row "Column A must finish before Column B → Barriers" (line 19) tells readers to add barriers they don't need. Barriers exist for *hidden* dependencies: shared state that isn't a column reference. Add one sentence to the with_serial-vs-barriers comparison (lines 139–144).
3. **Mental model to fix: "the engine parallelizes stateless operations" (line 93).** This reads as if the engine *detects* statelessness. It doesn't. By default it *assumes* every formula is stateless, and the correctness burden is on the user. Retitle and reframe "When parallelization is safe by default" around what the engine assumes and what you must guarantee.
4. **Scope: configuration detail is threaded through the narrative.** Property names and defaults appear inline at lines 50, 66, 83, 87, 124, 126 and 320–323. Collect them in one "Configuration" section at the end, or point to `../query-table-configuration.md`, and rewrite the narrative at concept level ("once the table is large enough to be worth splitting").
5. **Audience fit.** These terms appear before they're defined, or are never defined:
   - "barriers" and "implicit barriers" in the Quick reference (lines 19, 21), defined at lines 137 and 318.
   - "notifications" (line 50).
   - "free-threaded" (line 75).
   - "partition filters", "location" (line 329).

   For a reader who knows Python but not this feature, the Quick reference is opaque as the second thing on the page.
6. **Idiomatic alternative missing.** "Cumulative calculations" and "sequential numbering" are listed as `with_serial` cases (line 338). Most readers should use `update_by` (e.g. cumulative sum) or `i`/`ii`, not a Python global. One pointer would stop the page from teaching the global-counter pattern as the norm.

## Accuracy

Surfaced by the accuracy step. Items are ordered by impact.

**A1. The Quick reference / takeaway remedies aren't sufficient** (lines 18, 20, 22, 346; also 9 and 338). Source: `table-api/.../ConcurrencyControl.java`, `withSerial`:
- "The expression will never be invoked concurrently with itself."
- With `SERIAL_SELECT_IMPLICIT_BARRIERS` false, "no additional ordering between selectable expressions is imposed… To impose further ordering constraints, use barriers."

`QueryTable.java` sets `serialSelectImplicitBarriers` to `!STATELESS_SELECT_BY_DEFAULT`, which is false in 41+.

What the rows get wrong:
- **Global counter:** `with_serial` is enough only if a single expression touches the counter. The page's own counter example has two columns (line 176).
- **Non-thread-safe library → "Forces single-threaded access":** false. Another column, a filter, or another table on an update-graph thread can call the library at the same time.
- **File I/O or logging:** same problem as the library row.

Fix the rows as in Developmental note 1. The same claim appears in all six places listed there.

**A2. The breaking-change callout misstates both sides** (line 9).
- *Before 41:* the parent of `8ea55b6a1d` (DH-20714) has `statelessSelectByDefault` defaulting to `false`. But `DhFormulaColumn.isStateless()` still returned true when every param was an immutable type and every used column was stateless. Pure column math was parallelized before 41. What changed was the treatment of formulas that reference mutable or unknown objects (Python callables, for example), plus condition filters (`ConditionFilter.permitParallelization` returned `STATELESS_FILTERS_BY_DEFAULT`, which was false).
- *After 41:* the remedy ("mark it with `with_serial`") is incomplete. The same commit flipped implicit barriers off. Code with two stateful columns that was ordered in 40 needs `with_serial` **plus** a barrier, or `serialSelectImplicitBarriers=true`.
- "will now produce incorrect results" should be "can produce".

**A3. `view`/`update_view`/`lazy_update` listed as "not parallelized" is the wrong model** (line 70). These produce a `ViewColumnSource` via `AbstractFormulaColumn.getViewColumnSource`. Their formulas run on whatever thread reads the cell, including the threads of a parallel downstream `update` or `where`. They're the *least* safe place for a stateful formula. `QueryTable.viewOrUpdateView` throws `"A stateful column cannot safely be used in a view or updateView."` and `"view and updateView cannot respect barriers"`. The page's own IMPORTANT box (line 156) is closer to correct. Suggested rewrite: "Not computed up front. Their formulas run later on whichever thread reads the column, possibly many threads at once, so they must be stateless." Also, in Python these methods only accept strings (`table.py`: `formulas: Union[str, Sequence[str]]`), so the restriction is enforced by the signature.

**A4. The GIL callout overstates the guarantee** (line 75): "on a standard (GIL-enabled) build, they're never run concurrently." What the source shows:
- `FormulaColumnPython.isParallelizable()` and `DhFormulaColumn.isParallelizable()` return false without free-threading. That blocks only *within-column* row splitting (`SelectColumnLayer`: `canParallelizeThisColumn = … sc.isStateless() && sc.isParallelizable()`).
- Separate columns are still separate layers. Each is submitted to the `JobScheduler`, and `anyParallelColumns()` checks only `isStateless()`. So two unmarked Python columns in one `update` run concurrently, interleaved at GIL granularity.
- Independent tables also update concurrently.

The page's own barrier example (lines 244, 280) contradicts the absolute. Suggested rewrite: "a single Python-backed formula or filter isn't split across threads." Then say plainly that a GIL build doesn't protect shared state.

**A5. The counter "race" output teaches a wrong model** (lines 180–190).
- "`B` not following `A + 1`" implies row-by-row evaluation across columns. The engine evaluates column by column (layers). Even fully sequential execution gives B = 5,000,000 + row, never A + 1.
- The out-of-order A values (A=5 then A=4) can't happen on a standard GIL build, because column A isn't split within itself (A4). They're possible only on free-threaded Python.
- "gaps (no 10-19 visible)" is meaningless in a five-row excerpt of a five-million-row table.

Replace the table with a description that's true on both builds: A and B interleave, so neither column is contiguous.

**A6. The barrier example's runtime claim is stronger than the implementation** (lines 244, 280). Both columns are `with_serial`, so `anyParallelColumns()` is false. `QueryTable` then uses `ImmediateJobScheduler`, and the layers run in list order on the calling thread. Removing the barrier would likely still produce 0–9 / 10–19 today. The barrier is load-bearing by *contract* ("no additional ordering… is imposed"), not observably. Reword "both columns would race and produce unpredictable results" and "both read `counter = 0`, and produce overlapping… results" to "nothing guarantees A finishes before B starts" (see AQ2).

**A7. "Serialization processes rows… on a single thread"** (line 148) adds a resource the contract never names. The contract says only "never invoked concurrently with itself" and "rows are evaluated sequentially in row set order." Line 213 ("only one thread processes the column at a time") is correct. Use that wording at line 148.

**A8. Stateless definition is too broad** (line 97): "Doesn't read… global variables." Reading an immutable constant from the query scope is fine; before 41 the engine explicitly treated immutable-type params as stateless (`DhFormulaColumn.isImmutableType`). Use "Doesn't read or modify *mutable* shared state."

**A9. The "What gets parallelized" list is incomplete** (lines 62–66). `update_by` (`UpdateBy.java`, `iterateParallel`), `range_join` (`RangeJoinOperation.java`), and Barrage snapshots (`ConstructSnapshot`, `QueryTable.enableParallelSnapshot`) also parallelize. Either add them or frame the list as examples. Line 72, "Operations waiting for dependencies," isn't an operation category; drop it.

**A10. Cross-language note (sibling not reviewed).** The Groovy page at f2ef483084 repeats A1 (lines 18, 20, 22), A2 (line 9) and A3 (line 66). Mirror the fixes there in the same pass.

**Minor accuracy points:**
- Line 50: "`updateThreads` being greater than 1, which is the default." The default is `-1`, meaning `availableProcessors()` (`PeriodicUpdateGraph.java`). It's only >1 on a multi-core host. This detail also belongs in the Configuration section.
- Lines 320–323: `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is a Java field, not something users set. Name only the property. The default isn't fixed at false: it follows `!statelessSelectByDefault`. "Stateless mode"/"Stateful mode" aren't engine terms.
- Line 240: the rule that a column can only respect a barrier declared *earlier* in the list is missing. `SelectAndViewAnalyzer` throws `"Respected barrier, … is not defined for …"`, and the javadoc says "It is an error to respect a barrier that has not already been defined per the natural left to right ordering." It's worth one clause, because `[col_b, col_a]` fails.

**Verified accurate (no change needed):**
- Thresholds:
  - `minimumParallelSelectRows` = `1L << 22`.
  - `where` parallelizes when `numberOfRows / 2 > parallelWhereRowsPerSegment` (`1 << 16`), in `AbstractFilterExecution.shouldParallelizeFilter`.
  - `minimumParallelSortRows` = `1L << 20` and `parallelSort` = true.
- Thread pools: `OperationInitializationThreadPool.threads` and `PeriodicUpdateGraph.updateThreads` both default to -1, which means all available processors.
- Cross-column scheduling: `SelectAndViewAnalyzer.UpdateScheduler`.
- Serial filters: they act as a reordering barrier.
- Partition filters: prioritization is keyed on `isSerial()` in `PartitionAwareSourceTable.whereImpl`.
- Python APIs:
  - `Selectable.parse`, `with_serial`, `with_declared_barriers`, `with_respected_barriers` (single `Barrier` or a sequence), in `deephaven.table`.
  - `Barrier`, in `deephaven.concurrency_control`.
  - `is_null`, `not_`, `Filter.with_serial`, in `deephaven.filters`.
- Built-ins: `randomGaussian` and `randomInt`.
- Links: every internal link target and anchor exists in the f2ef483084 tree, including `Selectable.md#with_serial`, `update.md#serial-execution`, `where.md#serial-execution`, `Filter.md`, and the crash-course page. External pydoc and Oracle links need a manual check.

## Structure

1. **Near-verbatim repeated examples and length fatigue.** `get_and_increment_counter` is defined three times (lines 162–178, 194–211, 246–278) on a 356-line page. Use one canonical two-column counter as a wrong-then-right pair, in this order:
   - unsafe;
   - `with_serial` alone, still wrong across columns;
   - `with_serial` + barrier.

   Share the definition with a `test-set` rather than redefining it.
2. **Topic interleaving and level of abstraction.** "How parallelization works" detours into the GIL caution (lines 74–77, under "Within a single table") and a configuration-heavy "Query phases and thread pools" section (lines 79–89) before the page gets to "what's safe." Move the thread-pool properties and every other inline property (see Developmental note 4) into one closing "Configuration" section. Keep phases as two short concept paragraphs. Move the GIL note to its own short section after "Controlling execution order," where its row-order caveat is actionable.
3. **Three summaries of the same decision.** Quick reference (lines 13–22), "Choosing an approach" (lines 333–339), and Key takeaways (lines 341–350) overlap. Keep the early Quick reference as the orientation map, but correct it (A1) and link its terms. Merge "Choosing an approach" into Key takeaways.
4. **Parent/child terminology mismatch.** The intro says "three ways: across tables, across rows, and across columns" (line 26). The children are "Across tables" and "Within a single table," with rows and columns as bold run-ins. Either make the parent say "two levels," or give rows and columns their own `###` headings. Rename to standard terms as well (see Style).
5. **Orphaned aside: "Stateful partition filters"** (lines 327–331). There's no bridge to it, it isn't in the Quick reference, and it depends on undefined concepts (partitioned source tables, locations). Move it to the `where` or `Filter` reference, or to a short "Advanced" note at the end with one sentence of context.
6. **Duplicated callouts with drifting wording.** "Most queries don't need serial execution" appears at lines 130, 151 and 343. "You need both" appears at line 144 and again in the IMPORTANT box at line 283, which is followed by line 285. State each once; cut the line-283 box.
7. **Category consistency.** "What does NOT get parallelized" mixes operation types, a user control, and a scheduler state (A9). Quick reference rows mix a formula kind ("pure column math"), a specific example ("global counter"), and a non-output-changing case ("logging").

**Re-verify (what the proposed moves and renames would touch):**
- **Inbound anchors, corpus-wide scan at f2ef483084:**
  - `#serialization` is linked from `python/conceptual/query-table-configuration.md:238`, `python/reference/table-operations/select/view.md:23`, `update-view.md:23`, and the three Groovy equivalents.
  - `#barriers` is linked from `python/reference/query-language/types/Filter.md:91,112`, `Selectable.md:56,79`, and the four Groovy equivalents.

  Renaming "Serialization" to "Serial execution" (recommended in Style) means updating all six `#serialization` links. Keep "Barriers" as a heading.
- **Within-doc anchors:**
  - `#controlling-execution-order` (lines 11, 71).
  - `#query-phases-and-thread-pools` (line 50; its link text "Thread pools" already mismatches).
  - `#when-parallelization-is-safe-by-default`, `#example-a-counter-needs-serialization`, `#example-extending-the-counter-with-a-barrier` (lines 337–339).

  Merging the counter examples (item 1) and moving the thread-pool section (item 2) breaks the last four. If "Choosing an approach" is folded into the takeaways, those links go with it.
- **Caveats that must survive the moves:**
  - the GIL row-order caveat (line 77);
  - "the respecting column needn't be serial if it only reads" (line 285);
  - "barriers for filters are uncommon" (line 316);
  - the `view`/`update_view` restriction (line 156).
- **Checks on my own proposed wording:** "the engine orders B after A when B references A" and "`with_serial` + barrier for multi-column shared state" were both checked against the `ConcurrencyControl` javadoc. They're isolated to the rows named and need no further escalation.

## Examples

1. **The counter "fix" drops the problem** (lines 194–211). The unsafe example has columns A and B; the fix computes a single `ID` column. The reader never sees the two-column case fixed until the barrier section, and there it's framed as an "extension." Solve the same query that was shown broken (Structure item 1).
2. **The serial-filter example has no side effects** (lines 219–232). `is_null(...).with_serial()` illustrates syntax, not "a filter with stateful side effects," which is how the lead-in (line 217) introduces it. Use a filter that calls a stateful Python function, or say explicitly that the example only shows syntax.
3. **The multiple-barriers example has no motivation** (lines 291–312). Pure `i * n` columns don't need barriers. A one-line comment ("in practice these would share state") keeps readers from copying barriers onto pure math.
4. **The fix example is expensive to test** (line 210). The tested block makes 5,000,000 Python calls, yet the note at line 124 says the examples use small tables. `with_serial` correctness doesn't depend on size, so use 10 rows.
5. **The `skip-test` at line 162 has no stated reason.** The output is non-deterministic, so the skip is justified, but tell the reader why ("output varies per run").

## Style

- **Configuration injection in narrative:** 7 places (lines 50, 66, 83, 87, 124, 126, 320–323). Example: line 66, "(`QueryTable.minimumParallelSortRows`, about 1 million rows by default) — disable with `QueryTable.parallelSort=false`." Fix: "once the table is large enough," with the property moved to the Configuration section.
- **Coined or non-standard terms:** "across tables/rows/columns" (lines 26–60) could become "concurrent table updates," "row-level (data) parallelism," and "column-level parallelism." "Serialization" (line 146) collides with the CS meaning of object serialization; the reference pages use "Serial execution." "Stateless mode"/"Stateful mode" (lines 322–323) aren't engine terms. "Update Graph Processor Thread Pool" (line 87) uses a legacy class name; the class is `PeriodicUpdateGraph`.
- **Term conflation:** "stateless" and "thread-safe" are used as if they meant the same thing (line 17 "Thread-safe, no shared state"; line 337). They're different properties.
- **First-mention linking and link targets.** "update graph" is bare at line 30 and linked at line 52; move the link to line 30. `Barrier` (lines 137, 240) and `ConcurrencyControl` (lines 153, 356) link to external pydoc, but internal `reference/query-language/types/Barrier.md` and `ConcurrencyControl.md` exist; prefer those. The `with_serial` link at line 153 targets `update.md#serial-execution`, but everywhere else it targets `Selectable.md#with_serial`.
- **Passive voice:** about 5 instances. Example: line 70, "these are lazily evaluated," could become "Deephaven evaluates these lazily." Line 142, "Rows within each column can still be parallelized," could become "Deephaven can still parallelize rows within each column."
- **Version naming:** "Deephaven 41+", "Deephaven 40" (line 9). Elsewhere the corpus uses "Deephaven v0.40" and "Deephaven Core 41". Pick one form (AQ3).
- **Mechanical checks found nothing:** no dot-prefixed methods in prose, no empty parentheses, no `[here]` links, no curly quotes, and em dash spacing is correct throughout. Headings are sentence case. The Related documentation section is present; consider adding the Selectable, Filter, Barrier and query-table-configuration pages.

## Author queries

- **AQ1 [Within a single table, line 77]:** For a non-serial Python column on a GIL build, can rows really be evaluated "out of order"? In `SelectColumnLayer`, serial and non-parallelizable columns take the same `doSerialApplyUpdate` path. The only differences I found are implicit barriers and filter reordering. Please have an engine SME confirm before keeping this caveat.
- **AQ2 [Barriers example, lines 244/280]:** When all columns are serial, the engine uses `ImmediateJobScheduler` and runs the layers in list order, so removing the barrier likely still gives 0–9 / 10–19. Do we teach the contract ("not guaranteed") and drop "would race"?
- **AQ3 [Callout, line 9]:** The release was tagged 0.41.0, and `gradle.properties` now says major version 43. What's the approved way to name versions ("Deephaven 41", "Core 41", "v0.41")?
- **AQ4 [Serialization]:** On a refreshing table, a `with_serial` counter re-runs for modified rows on every cycle, and row-set order holds only within one update. Should the page say so, or scope the counter examples to static tables explicitly?
- **AQ5 [Stateful partition filters, line 331]:** `PartitionAwareSourceTable.whereImpl` also stops prioritizing *every later* filter, partitioning ones included, once it sees a serial filter. Is that user-relevant enough to mention? And does this section belong on this page at all?

## Strengths

1. The with_serial-vs-barriers comparison (lines 139–144), placed where both mechanisms are first named, with "you often need both." It's the right idea at the right spot; the summaries just need to keep it.
2. The note that small examples illustrate the correctness contract, not speedup (line 124). The instinct is right; only the configuration detail inside it should move.
3. The Quick reference as an early orientation map. Once its rows are corrected, keep it at the top.

**Out of scope, for a follow-up:** current `main`'s `docs/python/conceptual/query-table-configuration.md` links to `parallelization.md#controlling-concurrency-for-select-update-and-where` (line 222), an anchor that doesn't exist in this page, and lists `statelessFiltersByDefault` as `false` (line 42) when the source default is `true`. The DOC-857 branch has already fixed both.