# Structure review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857, commit f2ef483084)

**Category:** Concept guide. It lives under `conceptual/`. The sidebar files it under Best practices and troubleshooting → Performance, but that placement is for discoverability and doesn't change the category. Because it's a concept guide, I weighted split explanations, interleaving, and duplication more heavily. The page is 356 lines and repeats the same counter example three times, so it also meets the skill's consolidation-candidate threshold.

**Scope:** Python file only. This is a report; I made no edits. The snapshot matches `f2ef483084:docs/python/conceptual/query-engine/parallelization.md` exactly.

## Outline (from the fence-aware awk pass, checked by reading the file; there are no indented or `~~~` fences)

```
13  ## Quick reference
24  ## How parallelization works
28    ### Across tables
54    ### Within a single table
79    ### Query phases and thread pools
91  ## When parallelization is safe by default
128 ## Controlling execution order          (Key concepts glossary at 132, with_serial-vs-barriers at 139)
146   ### Serialization
158     #### Example: a counter needs serialization
215     #### Serial filters
236   ### Barriers
242     #### Example: extending the counter with a barrier
287     #### Multiple barriers
318     #### Implicit barriers
327   ### Stateful partition filters
333 ## Choosing an approach
341 ## Key takeaways
352 ## Related documentation
```

**Code examples:** 32 (across-tables ticking fan-out), 103 (four stateless examples), 162 (broken two-column counter, `skip-test`), 194 (counter fixed with `with_serial`, now one column `ID`), 219 (serial filters), 246 (counter + serial + barrier, two columns), 291 (multiple barriers, stateless formulas).
**Callouts:** 8 IMPORTANT (breaking change in 41+, plus a quick check), 74 CAUTION (GIL; "not the same guarantee `with_serial` provides"), 123 NOTE (parallel row thresholds), 150 NOTE ("most queries don't need serial"), 155 IMPORTANT (no `with_serial` on `view`/`update_view`), 282 IMPORTANT ("barriers don't make a column serial; you need both").
**Glossary:** "Key concepts" sits at line 132, about 37% of the way down. The top callout (line 9) and the Quick reference table (lines 18–21) already rely on the terms it defines.

## Findings (most confusing first)

1. **The counter example's problem and its fix don't line up (topic interleaving and a split example), lines 158–285.** The broken example at 175 uses *two* columns (`A`, `B`), and the symptom list at 190 includes "`B` not following `A + 1`." The fix at 209 quietly switches to a *single* column `ID`, so the reader never sees the two-column problem from 175–190 fixed there. That doesn't happen until 244–280, after a detour through `Serial filters` (215). Someone following the counter thread sees the problem, gets a fix for a different problem, switches topics, and only then gets the real resolution.
   **Fix:** Make the broken example single-column (`"A = get_and_increment_counter()"` only) so `with_serial` at 209 fully resolves it, and move the `B`/"`A + 1`" symptom to the barrier section as its motivation. Alternatively, keep the two-column setup and state at 213 that `with_serial` fixes row order within a column, with a forward pointer to the barrier example for the cross-column part. Either way, make the problem shown match the problem solved.

2. **The same counter example is repeated nearly word for word three times, lines 162–178, 194–211, 246–278.** `counter = 0` and `get_and_increment_counter()` are redefined each time, identically. Line 244 says "Building on the counter example above" but still pastes the whole setup again. Combined with the 356-line length, this is the page's main source of fatigue.
   **Fix:** Define the counter function once, in the first example. Show the fix at 209 and the barrier version at 261–277 as the changed lines only (`col = ...`, `barrier = ...`, `col_a`/`col_b`), with a callback such as "using the same `get_and_increment_counter` from above." If every block must run on its own for `order=` validation, keep the function but add a one-line comment calling it the same function as above, and trim the prose that re-explains it.

3. **`with_serial` and barriers are each explained in full three or four times, lines 132–144, 146–148, 213, 236–240, 280–285.** `with_serial` gets a full explanation at 136, 141, 148, and 213, each with slightly different wording: "one at a time, in order," "within one column… other columns can still run," "on a single thread," "only one thread processes the column at a time." Barriers get one at 137, 142, 238, and 240. None of these acknowledges the others, so the reader re-learns each mechanism several times.
   **Fix:** Keep the Key concepts and "`with_serial` vs. barriers" block at 132–144 as a short, labeled preview (one line each, plus the contrast). Give the one full definition at the start of `### Serialization` (148) and `### Barriers` (240). Cut 213 down to what the example proved, for example "each row saw the previous row's increment," rather than defining serial again.

4. **The "you need both" warning appears three times with different wording, lines 144, 280, 282–285.** Line 144 (prose) says "you often need both." Line 280 says "Without `with_serial`, rows within each column would also race." The IMPORTANT callout at 282 says "you typically need **both**." Line 285 then qualifies it ("That's not always true… only reads…"). Read together, "often," "typically," and "not always" sound like three different rules.
   **Fix:** State it once as the IMPORTANT callout right after the barrier example (282), with the 285 qualification folded into the same callout. Change 144 to a forward reference ("…see the barrier example below for when you need both"). Drop the last sentence of 280.

5. **Terms are used before they're defined, and the glossary is mid-page, lines 9, 11, 18–21, 30, 50, 132.** The opening callout (9) and the Quick reference table (18–21) rely on `with_serial`, "Barriers," and "implicit barriers," but Key concepts doesn't appear until 132. "Implicit barriers" isn't defined until 318, and that section also says "Most users don't need to change this setting." "Update graph" is used at 30 and 50 but only explained at 52. Line 50 names `PeriodicUpdateGraph.updateThreads` before the thread-pool section introduces it.
   **Fix:** Give the Quick reference a lead-in sentence ("`with_serial` forces in-order row processing within one column; a barrier makes one column finish before another starts. Both are explained under [Controlling execution order]"), or move the four Key concepts bullets directly above the table. At 30, reorder so the 52 sentence (what the update graph is) comes before the example. Drop the property name at 50 and keep just the forward link.

6. **Parallelism thresholds are split across two sections, lines 66 vs. 123–124.** "What gets parallelized" (62–66) gives the row threshold for `sort` inline. The thresholds for `select`/`update` (`minimumParallelSelectRows`) and `where` (`parallelWhereRowsPerSegment`) only show up later, in a NOTE inside "When parallelization is safe by default." That section is about correctness, and the NOTE even says it's not about speed. A reader looking for when an operation actually runs in parallel has to put two places together.
   **Fix:** Move the `select`/`update`/`where` thresholds into the "What gets parallelized" bullets at 64–65, next to the `sort` threshold. Cut the 123 NOTE to one sentence: "These examples are too small to actually run in parallel; see the thresholds under [What gets parallelized]."

7. **The page's thesis, the default in 41+ and how to change it, is split between line 9 and line 126, and the two aren't linked.** The opening callout announces that the default flipped to parallel. The configuration properties that control that default (`QueryTable.statelessSelectByDefault`, `statelessFiltersByDefault`) show up almost as an afterthought at 126, and neither place mentions the other. A migrating reader, who is exactly who the callout is for, won't find the setting that restores the old behavior.
   **Fix:** Add a clause to the 9–11 callout ("…or restore the pre-41 default with `QueryTable.statelessSelectByDefault`/`statelessFiltersByDefault`; see [When parallelization is safe by default]"), and have 126 link back to the breaking-change note.

8. **The decision guidance is covered four times, lines 13–22, 139–144, 333–339, 341–348.** The Quick reference table, the "`with_serial` vs. barriers" block, "Choosing an approach," and "Key takeaways" all cover which mechanism to use when. "Choosing an approach" (333) points back to the Quick reference and then re-explains it, and "Key takeaways" (345–347) repeats it a third time. Putting the table at the top was the right call, so keep it there. The problem is the closing duplicates.
   **Fix:** Merge "Choosing an approach" into "Key takeaways." Keep its three "Use X when… (as in [example])" bullets, which help most because they link back to the worked examples, and drop the takeaway bullets that repeat them (346–347).

9. **The Quick reference and the page body disagree about implicit barriers, lines 21 and 318–325.** The table lists "Barriers or implicit barriers" as a first-line solution. The body presents implicit barriers as a niche, off-by-default setting and says "Most users don't need to change this setting." There's also a terminology clash: 322–323 call the setting's values "Stateless mode" and "Stateful mode," reusing "stateless," which line 93 defined as a property of a formula, for a completely different idea.
   **Fix:** Change table row 21 to "Barriers" (or "Barriers (or enable implicit barriers)" with a link to #implicit-barriers). At 322–323, rename the modes to something like "Off (default)" and "On (`serialSelectImplicitBarriers=true`)" so "stateless" keeps one meaning. (Whether both `SERIAL_SELECT_IMPLICIT_BARRIERS` and `serialSelectImplicitBarriers` need to appear at 320 is a question for the accuracy check.)

10. **"Stateful partition filters" is an orphaned aside, lines 327–331.** It's an H3 sibling of `Serialization` and `Barriers`, which suggests a third control mechanism of equal weight. It's really a niche exception to serial filters. It has a bridge sentence (329) but no row in the Quick reference and no mention in Choosing an approach or Key takeaways.
   **Fix:** Demote it to a `####` under `### Serialization`, directly after `#### Serial filters` (215–234), where the rule it modifies is taught. Or move it after "Choosing an approach" under an "Advanced: partition filters" heading. Filter coverage has a related asymmetry: serialization gets a dedicated `#### Serial filters`, but barrier-on-filter coverage is a trailing paragraph at the end of `#### Multiple barriers` (316). Move 316 up to the end of the `### Barriers` intro (after 240) so it isn't tied to the multi-barrier example.

11. **Across-tables parallelism and the update graph are explained twice, lines 28–52 vs. 83–87.** "Query phases and thread pools" re-teaches across-rows (83), update-graph registration (85), and across-tables concurrency (87, "two tables that both depend on the same changed source can each finish updating…"). All three were already covered at 50–60, and the second pass doesn't acknowledge the first. The section intro also promises three ways ("across tables, across rows, and across columns," 26), but the H3s are "Across tables," "Within a single table," and "Query phases and thread pools," so the third H3 doesn't match the list.
   **Fix:** In 83–87, name the parallelism types with links instead of re-describing them ("…parallelizing [across rows and columns](#within-a-single-table) and [across tables](#across-tables)"). Change 26 to "…in three ways, described below, and uses two thread pools to do it," so the outline matches the promise.

12. **The GIL caution is in a section it doesn't belong to, lines 74–77.** It's placed inside "Within a single table," between the parallelized/not-parallelized lists and the thread-pool section, but it limits every Python-backed formula and filter, not just within-table work. For Python readers it's arguably the most practical fact on the page: on a standard build, Python formulas never run concurrently. Yet it's missing from both the Quick reference and Key takeaways.
   **Fix:** Move it to "When parallelization is safe by default" (after the 123 NOTE), since both are about when the engine actually runs something concurrently. Add one Key takeaways bullet: "Python-backed formulas and filters only run concurrently on a free-threaded Python build."

13. **The `view`/`update_view` limitation is stated twice with its reason re-explained, lines 70 and 155–156.** Line 70 says these operations aren't parallelized because they're lazy. The IMPORTANT at 156 repeats the laziness reasoning to explain why `with_serial` can't be used with them. That one is a different consequence, so the callout is fine, but the second explanation of *why* is redundant.
   **Fix:** Trim 156 to the consequence plus a link back ("…because they're evaluated lazily ([see above](#within-a-single-table)). Use `select` or `update` instead.").

14. **Links are inconsistent (minor; checked at f2ef483084, where all relative targets exist).** `with_serial` points to `Selectable.md#with_serial` at 9, 71, 136, and 346, but to `update.md#serial-execution` at 153, so the same term goes to two different pages. `Barrier` (137, 240) and `ConcurrencyControl` (153, 356) link to external pydoc even though in-repo reference pages exist at `reference/query-language/types/Barrier.md` and `ConcurrencyControl.md`, while `Selectable`/`Filter` link to their in-repo pages. Related documentation (352–356) doesn't list the Selectable, Filter, Barrier, or Crash Course pages the body depends on.
   **Fix:** Point every `with_serial` link at `Selectable.md#with_serial`. Switch `Barrier`/`ConcurrencyControl` to the local reference pages. Add those pages and the Crash Course to Related documentation. (The link-convention details are for `deephaven-writing-style` to confirm.)

## What already works
- Putting the Quick reference right after the intro gives readers an early map.
- The `with_serial` vs. barriers comparison at 139–144 comes at the first joint mention, which is the right place.
- "Choosing an approach" links back to the specific worked examples, which is the right way to reinforce them.

## Suggested order after restructuring
Intro → breaking-change callout (with the config knob) → Quick reference (with a one-line definition lead-in) → How parallelization works (with thresholds merged into "What gets parallelized," and thread pools without the repeated explanations) → When parallelization is safe by default (+ GIL caution) → Controlling execution order (short preview) → Serialization (one counter example, single-column; Serial filters; Stateful partition filters as a `####`) → Barriers (filter note moved up; counter extended with changed lines only; the "both" callout once; Multiple barriers; Implicit barriers) → Key takeaways (merged with Choosing an approach) → Related documentation.

After any of these moves, re-run `deephaven-core-accuracy-check` and `deephaven-writing-style`. Findings 1, 2, 4, and 9 in particular move or merge prose that makes technical claims, such as the counter output and the implicit-barrier modes.
