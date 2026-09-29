<!-- Saved by the orchestrating session from the subagent's returned text; the subagent's own Write was refused. -->

**Two-sentence summary:** The page's biggest problem is that it teaches the wrong mental model in three places. It says `view`/`update_view`/`lazy_update` are "not parallelized," that Python columns "never run concurrently" on standard (GIL) Python builds, and that `with_serial` alone protects shared counters, files, and non-thread-safe libraries — the quick-reference table prescribes exactly that — and all three are wrong per the engine source. The rest is revision rather than restructuring: config properties are scattered through about eight places in the narrative, three summaries overlap, the pre-41 breaking-change claim is wrong, some examples don't show what their lead-ins promise, and the rename breaks an anchor that predicate-pushdown.md links to in both languages.

---

# Full review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857, commit f2ef483084)

**Category:** Concept guide. It lives in `conceptual/`, even though the sidebar lists it under Best practices and troubleshooting → Performance for discoverability. It gets the concept-guide profile: explanatory tone, a mental model built across the page, extra weight on split explanations and interleaving, and no configuration detail inside the narrative.
**Scope:** Python file only. The Groovy sibling was out of scope, but a quick grep of it at f2ef483084 shows at least four of the accuracy findings below appear there word for word (the "40 and earlier" callout at line 9, "What does NOT get parallelized" at lines 64–66, the "Non-thread-safe library" row at line 22, and `Squared = sqrt(X)` at line 109). Fix both files in the same pass.
**Mode:** Report only. Nothing was edited. Because the structure step made no edits, the re-verify step covered only the link and anchor checks that the proposed renames would affect.
**Verification basis:** Source in `/Users/margaretkennedy/dhc-skills-chip`:
- Engine: `QueryTable.java`, `SelectColumnLayer.java`, `SelectAndViewAnalyzer.java`, `DhFormulaColumn.java`, `FormulaColumnPython.java`, `ConditionFilter.java`, `SortHelpers.java`, `AbstractFilterExecution.java`, `PartitionAwareSourceTable.java`, `PeriodicUpdateGraph.java`, `OperationInitializationThreadPool.java`.
- API: `table-api/.../ConcurrencyControl.java`.
- Python: `py/server/deephaven/{table,filters,concurrency_control}.py`.

Link targets were checked against the f2ef483084 tree.

## Editorial summary

This is a large improvement over the current page. It opens with motivation, it has a real quick-reference table, and the counter/barrier examples give the reader something concrete. It isn't ready to merge, though, because the mental model it teaches is wrong in three places that matter:

- It says `view`/`update_view`/`lazy_update` are "not parallelized" (deferred formulas run on whatever threads read them, including parallel ones).
- It says Python-backed formulas "never run concurrently" on a standard GIL build (independent columns in one `update` are dispatched to separate threads regardless).
- It says `with_serial` alone protects a counter, file, or non-thread-safe library (it only prevents a column from running concurrently with *itself*).

The single biggest issue is that last point. The quick-reference table, the page's most-read artifact, prescribes `with_serial` for the cases where it is insufficient. Around those, configuration properties are scattered through the narrative in about eight places, and the page ends with three overlapping summaries. **Developmental verdict: needs revision.** The skeleton is sound, and none of this needs restructuring.

## Developmental notes

1. **The key message is stated late, and the page opens on alarm.** *Where:* lines 6–11, 130, 343. *Why it matters:* the reader who needs this page most wants to know "do I have to change anything?". The page answers "most code works correctly without changes" only in *Controlling execution order* (line 130) and *Key takeaways* (line 343). Before that, the first thing a reader meets after "no configuration required" is an IMPORTANT box saying code "will now produce incorrect results." *Fix:* put the main point in the intro's second sentence, for example: "Deephaven assumes every formula is safe to run in parallel. Most are, so most queries need no changes. This page explains how the engine parallelizes work and how to mark the exceptions." Then turn the breaking-change box into a shorter upgrade note that links to *Controlling execution order*.

2. **The page teaches "the engine parallelizes stateless operations," when the engine actually *assumes* every formula is stateless.** *Where:* the heading at line 91 and line 93. With the default `statelessSelectByDefault=true`, `DhFormulaColumn.isStateless()` returns `true` unconditionally, so the responsibility sits with the user. *Fix:* reframe the section as "What the engine assumes about your formulas" (a contract the reader must honor), not "When parallelization is safe."

3. **The page's definition of "stateless" is the wrong criterion, and the page's own examples break it.** Line 99 ("Produces the same output for the same input…") is contradicted by lines 39–40, which use `randomGaussian`/`randomInt` in a default-parallel `update`. By the page's own definition, the first example on the page is stateful, yet it is safe to parallelize. The real contract is no shared mutable state, no dependence on order, and thread safety. *Fix:* define the contract in those terms and drop "same output for same input."

4. **The core claims, as a reader would summarize them, handed to the accuracy step as its first targets:**
   1. Deephaven parallelizes in three ways (partly right, A5).
   2. `select`/`update`/`where`/`sort` are parallelized, and `view`/`update_view`/`lazy_update` are not (**wrong model**, A2; `sort` is initialization-only, A5).
   3. Python formulas never run concurrently on a GIL build (**wrong**, A3).
   4. The engine parallelizes stateless operations (**wrong framing**, note 2).
   5. `with_serial` handles a counter, file I/O, or a non-thread-safe library (**insufficient** across columns, A1).
   6. Deephaven 40 treated all formulas as sequential (**wrong**, A4).

5. **Audience fit: undefined engine vocabulary.** "Independent notifications" (line 50), "row-set order" (line 77), "location" and "partitioning columns" (lines 329–331), and the two thread-pool proper nouns (lines 83, 87) all arrive without a definition.

6. **Scope: configuration and implementation detail belong elsewhere.** The thread-pool properties, thresholds, and `*ByDefault`/implicit-barrier properties are reference material. *Stateful partition filters* is niche (partitioned on-disk sources). Structure findings S1 and S5 cover the fix.

## Accuracy

Highest impact first. Every finding comes from the accuracy step unless marked otherwise.

**A1. `with_serial` is prescribed for problems it doesn't solve across columns.** *Where:* the quick-reference rows at lines 18, 20, and 22; lines 338 and 346.
- *Source:* `ConcurrencyControl.withSerial` javadoc: "The expression will never be invoked concurrently with itself." It also says: "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed." `QueryTable.java:400–402` defaults that flag to `!STATELESS_SELECT_BY_DEFAULT`, which is `false`.
- So two serial columns that share a counter, a file, or a non-thread-safe library can still run concurrently with each other.
- The page's own barrier example exists precisely because `with_serial` isn't enough, and the quick-reference table contradicts it.
- *Fix:* change those rows to "`with_serial`, plus a barrier (or implicit barriers) when more than one column touches the resource."
- "Forces single-threaded access" (line 22), "on a single thread" (line 148), and "only one thread processes the column at a time" (line 213) overstate the javadoc's "never invoked concurrently with itself." Paraphrase the javadoc instead.

**A2. "What does NOT get parallelized: `view`, `update_view`, `lazy_update`" is a wrong mental model.** *Where:* line 70.
- All three produce a `ViewColumnSource` (`AbstractFormulaColumn.getViewColumnSource`). The formula runs on whichever thread reads the column. When the reader is a parallel `where` or `update`, the deferred formula runs concurrently, and possibly more than once per row.
- The engine's guards confirm this: `QueryTable.java:2035–2036` throws "view and updateView cannot respect barriers," and lines 2056–2062 throw "A stateful column cannot safely be used in a view or updateView."
- The PR's own `reference/table-operations/select/{view,update-view}.md:23` say rows may be evaluated "by any thread," which contradicts this bullet.
- "Not computed upfront" is also ambiguous about when it applies (static tables, initialization, or update cycles).
- *Fix:* give these three their own point: "evaluated on demand. No parallel work up front, but the formula runs on whichever thread reads it, possibly concurrently and more than once, so it must be stateless."

**A3. The GIL caution's "they're never run concurrently" (line 75) is false.**
- On a GIL build, `DhFormulaColumn.isParallelizable()` (lines 899–906) and `FormulaColumnPython.isParallelizable()` return `false`. That only stops a column's *rows* from being split (`SelectColumnLayer.java:115–117`).
- Cross-column scheduling is gated by `isStateless()` (`SelectColumnLayer.allowCrossColumnParallelization`, line 657), which is `true` by default. `anyParallelColumns()` then selects `OperationInitializerJobScheduler`, and each column layer is submitted separately.
- So `update(["A = f()", "B = f()"])` runs on two threads, interleaved by the GIL. That is exactly the counter race.
- The deferred path is a second route: a Java `where` filter reading a Python `update_view` column is parallelizable (`ConditionFilter.permitParallelization` checks only the filter's own inputs).
- *Fix:* "A GIL build never splits a Python column's rows across threads, but separate columns can still run at the same time, and the GIL doesn't make a read-then-increment atomic." Keep the second paragraph (line 77); it is correct.

**A4. The breaking-change callout (line 9) misstates pre-41 behavior.**
- The version is confirmed: commit `8ea55b6a1d`, "DH-20714: Convert Filters and Selectables to Stateless by Default (#7331)," flipped both defaults from `false` to `true`, first in tag v41.0.0.
- But pre-41 `DhFormulaColumn.isStateless()` treated a formula as stateless when its query-scope parameters were immutable and its used columns were stateless. Plain column math was parallelized in 40.
- The claim is closer to true for condition filters (`ConditionFilter.permitParallelization` returns `STATELESS_FILTERS_BY_DEFAULT`).
- "will now produce incorrect results" is an absolute. The accurate word is *may*.
- The callout also omits that 41 turned off implicit barriers between serial selectables (see AQ3).

**A5. The "What gets parallelized" classification isn't qualified by evaluation mode, and it is incomplete** (lines 62–72, 83, 87, 124).
- `sort` is parallel only at initialization. `SortHelpers.parallelizableOperationInitializer()`, lines 1131–1142: "a sort listener running there sorts serially." That contradicts line 87's "just like during initialization."
- The select/update threshold is compared against update size (`added + modified`, `SelectColumnLayer.java:192–205`), not table size.
- Cross-column parallelism has no row threshold, so line 124's "evaluates the formula on a single core" is true per column, not per operation.
- Line 83 overgeneralizes the claim to every table operation.
- Omissions derived from `JobScheduler` users: `update_by` (`UpdateBy.java` `iterateParallel`), snapshots (`ConstructSnapshot`, `enableParallelSnapshot`), range joins, and `RegionedColumnSourceManager` location initialization.
- Line 72 ("Operations waiting for dependencies") isn't an operation category.

**A6. The counter example's "you may see" output (lines 180–190) doesn't match a standard build, and the fix fixes a different query.** This finding overlaps with the examples step.
- On a GIL build, the rows of columns `A` and `B` aren't split, but the two columns run concurrently. `A` and `B` interleave, and each increases within its own column.
- "Row 4 has `A=4` after row 3 has `A=5`" needs row splitting, which only happens on free-threaded builds.
- The fix (line 209) uses a single `ID` column. On a GIL build that is race-free even without `with_serial`, so the fix isn't load-bearing there. It only matters on a free-threaded build, where 5M rows exceeds the 4,194,304-row split size.
- Per A1, `with_serial` alone wouldn't fix the two-column original anyway.

**A7. The barrier example's "without" claims (lines 244, 280) describe a race that the current engine wouldn't produce, instead of the missing guarantee.**
- When every column is serial, `anyParallelColumns()` is `false`, so `QueryTable` uses `ImmediateJobScheduler` (lines 1825–1831) and runs the columns left to right on the calling thread.
- "Both read `counter = 0`" isn't a plausible outcome even under a real race.
- The barrier *is* required for the guarantee (per the javadoc).
- *Fix:* phrase it as a guarantee: "nothing guarantees A finishes before B starts, so values can interleave." Do the same for the "rows within each column would also race" claim, since 10 Python rows are never split.

**A8. The implicit-barriers section (lines 320–323) has four problems.**
- It names the Java field `SERIAL_SELECT_IMPLICIT_BARRIERS` alongside the property `serialSelectImplicitBarriers` as though they were two different things.
- "Stateless mode / Stateful mode" appear nowhere in source.
- It omits that the default is derived from `!statelessSelectByDefault`.
- Implicit barriers apply to serial *selectables* only (`SelectAndViewAnalyzer.java:176`), not filters.

Separately, the `withSerial` javadoc says the property defaults to "the value of" `statelessSelectByDefault`, but the code uses its negation. That's an engine follow-up.

**A9. A barrier constraint is missing** (line 240). Javadoc: "It is an error to respect a barrier that has not already been defined per the natural left to right ordering." `SelectAndViewAnalyzer.java:197–200` throws. "Declared by one operation" is confirmed ("Duplicate barrier" exception, line 345).

**A10. The partition-filter paraphrase doesn't match the mechanism** (lines 329–331). `PartitionAwareSourceTable.getWithWhere` (lines 140–175) never checks statelessness. It hoists partitioning filters unless the filter is serial, **comes after any serial filter**, or respects an undeclared barrier. The section also never says it applies only to partition-aware source tables.

**A11. Stateless and thread-safe are conflated** (line 17 "Thread-safe, no shared state"; line 337). They are different properties.

**A12. Minor.** Line 50: the `updateThreads` default is `-1`, which resolves to `availableProcessors()`, not ">1". Line 124's where-threshold arithmetic and line 66's "about 1 million" are confirmed.

**Confirmed accurate:**
- All property names and defaults (`QueryTable.java:276, 337, 344, 353, 379, 388`; `OperationInitializationThreadPool.java:30`; ≤0 means all cores).
- `Selectable.parse`, `with_serial`/`with_declared_barriers`/`with_respected_barriers` (single `Barrier` or sequence), `deephaven.concurrency_control.Barrier`, and `is_null`/`not_` returning a `Filter`.
- `update`/`select` accept a `Selectable`, and `where` accepts a `Filter`.
- The serial-filter reordering guarantee (line 234), the "barriers don't make a column serial" callout (line 283), line 141, and the free-threaded gate.

**Links:**
- All relative links resolve in the f2ef483084 tree. `Selectable.md`, `Filter.md`, and the Crash Course page are new in this PR, so this page can't merge ahead of them.
- **Broken inbound anchor caused by this PR's heading rename:** `docs/{python,groovy}/how-to-guides/predicate-pushdown.md:13` link to `…/parallelization/#controlling-concurrency-for-select-update-and-where`. Change them to `#controlling-execution-order`.
- **Inbound anchors that must survive renames:** `#serialization` (6 links: `query-table-configuration.md:238` and `{view,update-view}.md:23`, both languages) and `#barriers` (`{Filter,Selectable}.md`, both languages).
- `Barrier` (line 137) and `ConcurrencyControl` (line 153) point at external pydoc. Use the PR's internal `Barrier.md` and `ConcurrencyControl.md`.
- Line 153 links `with_serial` to `update.md#serial-execution`, while the other mentions link to `Selectable.md#with_serial`. Pick one.
- External links are flagged for manual check.

## Structure

- **S1. Configuration injection (Level of abstraction).** Property names or defaults appear at lines 50, 66, 83, 87, 89, 124, 126, and 320–323: about eight places and 11 mentions. *Fix:* rewrite those sentences at the concept level, and add one `## Configuration` table before *Related documentation* that links to `../query-table-configuration.md`.
- **S2. Parent/child terminology mismatch.** Line 26 promises "three ways: across tables, across rows, and across columns," but the children are *Across tables* and *Within a single table*, plus *Query phases and thread pools* on a different axis. Make the enumeration and the headings match.
- **S3. Three overlapping summaries.** *Quick reference*, *Choosing an approach* (which points back to Quick reference and restates it), and *Key takeaways* cover the same material, plus the lines 139–144 contrast. Keep Quick reference and the first-mention contrast, fold *Choosing an approach*'s common cases into the table rows, and cut *Choosing an approach*.
- **S4. The GIL caution sits under *Within a single table*** (lines 74–77). Move it to its own `## Python formulas and the GIL` section after *Controlling execution order*; its second paragraph depends on knowing `with_serial`. Leave a one-line pointer behind.
- **S5. *Stateful partition filters* is an orphaned aside** (lines 327–331) with no bridge and no table row. Move it below Configuration as "Advanced: partition filters," or to the `Filter` reference page.
- **S6. The Quick reference table mixes categories.** It mixes a category ("Pure column math"), an example ("Global counter"), a requirement ("Column A must finish…"), and "logging," which usually doesn't meet the criterion. Make each row a property of the formula, and move examples to their own column.
- **S7. Forward reference.** Line 50 refers ahead to *Query phases and thread pools*. S1 resolves most of this; consider placing that section right after *Across tables*.
- **S8. Length.** The page is 356 lines with four counter-based blocks. The barrier example correctly builds on the counter example, so once A6/E1 are fixed no further consolidation is needed.
- *Re-verify step:* nothing moved in this report-only pass. The inbound-link scan above covers the anchors the proposed renames would affect.

## Examples

- **E1.** The counter pair isn't a wrong-then-right pair: a two-column broken query versus a one-column `ID` fix. Use the same query and state its intended output first (see A6).
- **E2.** The serial-filter example (lines 217–232) is led in with "stateful side effects," but `is_null`/`not_` are pure. It teaches serializing filters that don't need it, which contradicts line 151. Use a filter with a real side effect, or label the block as syntax only.
- **E3.** The multiple-barriers example (lines 291–311) uses pure formulas, so the output is identical with or without barriers. Label it as syntax only or give the columns a shared resource.
- **E4.** `Squared = sqrt(X)` (line 120) computes a square root. Rename it `SquareRoot`. The Groovy sibling has the same line.
- **E5.** The fixed counter example makes 5,000,000 Python calls in snapshot tests (line 210). On a GIL build that buys no row parallelism, so use a small table unless the example targets free-threaded builds.
- **E6.** Tags are correct: `skip-test` on the nondeterministic broken counter, and `ticking-table order=null` on the `time_table` example.
- **E7.** No example for partition filters. One would make A10's rule concrete, or it could live on the reference page.

## Style

- **Y1. Coined labels (3 labels, about 10 uses).** "across tables/rows/columns" (lines 26, 28, 58, 60, 83, 87) and "Stateless mode/Stateful mode" (lines 322–323). Use standard terms such as "concurrent table updates" and "splitting rows across threads."
- **Y2. "Stateless" and "thread-safe" used as synonyms (lines 17, 337).**
- **Y3. "Serialization" (lines 146, 148)** collides with data serialization (Arrow/Barrage). "Serial execution" matches the reference pages. Update the six `#serialization` inbound links if you rename.
- **Y4. Mid-sentence config/caveat parentheticals (about 6)**, for example line 50 and line 66. S1 fixes most of them.
- **Y5. Undefined jargon (5 terms):** notifications, row-set order, location, partitioning columns, and the thread-pool proper nouns.
- **Y6. First-mention linking.** `with_declared_barriers`/`with_respected_barriers` never appear in prose, so their reference anchor is never linked. `Barrier`/`ConcurrencyControl` link externally rather than to the internal pages.
- **Y7. Future "will" (1):** line 9.
- **Y8. Hyphen as a dash in a code comment (1):** line 208.
- **Mechanical checks, all clean:** no dot-prefixed method names in prose, no empty `()` in prose, no `[here]` links, no curly quotes, em dashes spaced correctly (23), sentence-case headings, *Related documentation* present. Consider adding the Selectable/Filter/Barrier reference pages and `query-table-configuration.md` to it.

## Author queries

- **AQ1 [Example: a counter needs serialization, lines 180–190]:** Was the sample output captured on free-threaded Python? On a GIL build I'd expect `A` and `B` to interleave but each increase within its own column. Which symptom should the default-build page show? (SME: Chip Kent or the DH-20714 author.)
- **AQ2 [Barrier example, line 280]:** With every column serial, the engine uses `ImmediateJobScheduler`, so removing the barrier likely still gives 0–9 and 10–19. Is that sequential execution guaranteed, or an implementation detail?
- **AQ3 [Breaking change, line 9]:** Should the callout mention that 41 also turned off implicit barriers between serial selectables?
- **AQ4 [line 156]:** Does `lazy_update` reject serial/stateful columns the way `view`/`update_view` do? It's moot for Python, but the Groovy sibling needs the answer.
- **AQ5 [Partition filters, line 331]:** Should the page keep the "treated as stateless" framing, or describe the actual hoisting rule?
- **AQ6 [lines 62–72]:** Should `update_by`, range joins, and snapshots be listed, or should the list become explicitly illustrative?
- **AQ7 (engine follow-up):** the `ConcurrencyControl.withSerial` javadoc's statement of the `serialSelectImplicitBarriers` default contradicts `QueryTable.java:401–402`.

## Strengths

1. **The "`with_serial` vs. barriers" contrast at first joint mention** (lines 139–144), including "you often need both." Keep it, and make the quick-reference table agree with it.
2. **The barrier example builds on the counter example**, and line 285's note on when a respecting column *doesn't* need `with_serial` is precise.
3. **Line 77, "Not running concurrently isn't the same as running in row-set order,"** is accurate and subtle. It should survive whatever happens to the first paragraph of the callout.
