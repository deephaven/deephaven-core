<!-- Saved by the orchestrating session from the subagent's returned text; the subagent's own Write was refused. -->

**Summary:** The page has a clear purpose, but it's hard to follow for three main reasons. The counter example is written three times, and the "broken" version (columns A and B) isn't the code the `with_serial` "fix" (one ID column) actually repairs. Configuration property names and defaults appear in at least eight places with no Configuration section. And the "you need both `with_serial` and a barrier" rule is stated four times with drifting certainty. The next biggest problems are that the three ways promised at line 26 don't match the child headings, the Quick reference table uses terms before they're defined and is repeated by "Choosing an approach", and several asides (Python GIL, partition filters, thread pools) sit in the wrong sections.

---

# Structure review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857 snapshot, f2ef483084)

This review covers structure and organization only. Accuracy and prose style are separate passes. I didn't edit anything.

## Category and purpose
- **Category:** Concept guide. The file is in `conceptual/`. The sidebar files it under Best practices and troubleshooting > Performance for discoverability, but that isn't a category override. The concept-guide profile gives extra weight to split or duplicated explanations and to topics that interleave.
- **What the page is for:** after reading it, the reader should understand how Deephaven parallelizes work (across tables, rows, and columns), which formulas are safe to run in parallel by default, and when and how to use `with_serial` and barriers. The intro and headings support this, so the page's goal is clear.
- **Links:** at the PR commit, every relative link target resolves: `crash-course/parallelization.md`, `Selectable.md#with_serial`, `update.md#serial-execution`, `where.md#serial-execution`, `dag.md`, `engine-locking.md` and `query-table-configuration.md`.

## Structure map
The outline below was extracted with the fence-aware `awk` pass and checked by reading the file. The file uses no unusual fences.

```
13 ## Quick reference
24 ## How parallelization works
28   ### Across tables
54   ### Within a single table
79   ### Query phases and thread pools
91 ## When parallelization is safe by default
128 ## Controlling execution order
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

**Code examples:**

| Lines | What it shows |
| --- | --- |
| 32–48 | Across-table parallelism |
| 103–121 | Stateless operations |
| 162–178 | Broken counter (A and B) |
| 194–211 | `with_serial` fix (one column, ID) |
| 219–232 | Serial filters |
| 246–278 | Counter again, with serial plus a barrier |
| 291–312 | Multiple barriers |

The counter function appears three times.

**Callouts:**

| Lines | Type | What it says |
| --- | --- | --- |
| 8–11 | IMPORTANT | Breaking change in version 41 |
| 74–77 | CAUTION | Python GIL |
| 123–124 | NOTE | Row-count thresholds |
| 150–151 | NOTE | Most queries don't need serial execution |
| 155–156 | IMPORTANT | `with_serial` can't be used with `view`/`update_view` |
| 282–283 | IMPORTANT | You need both `with_serial` and a barrier |

**Glossary:** the "Key concepts" list is at lines 132–137, about 40% of the way down. `with_serial` and barriers are used from line 9 onward.

## Findings (most confusing first)

1. **Near-verbatim repeated examples, and the "broken" and "fixed" code don't match (158–213, 242–285).**
   - The counter function is written out in full three times.
   - The broken example computes columns A and B. The fix computes only `ID`, so it never repairs the code the reader was shown.
   - Line 190 says the output is wrong partly because "B not following A + 1". The page never set that up as the expected result, and line 244 later says B should continue from 10–19.
   - Line 190 also points to "no 10-19 visible" in a five-row excerpt of 5M rows.
   - **Fix:** use one counter scenario that builds step by step, and define the function once:
     - **Step 1, broken:** A and B with no controls.
     - **Step 2, add `with_serial` to each column:** rows within a column are fixed, but the columns still race each other.
     - **Step 3, add the barrier:** A gets 0–9 and B gets 10–19.

     Refer back to the function in later steps, and remove the `A + 1` claim. With 357 lines and three examples of the same scenario, the page also meets the length/fatigue threshold.

2. **Configuration detail is scattered and there's no Configuration section (level of abstraction).** Property names and defaults interrupt the explanation at:
   - line 50 (a parenthetical on `updateThreads`)
   - line 66 (`minimumParallelSortRows` and `parallelSort`, inside a list item)
   - line 83 (`OperationInitializationThreadPool.threads`)
   - line 87 (`PeriodicUpdateGraph.updateThreads`)
   - line 89 (`availableProcessors`)
   - line 124 (thresholds)
   - line 126 (`stateless*ByDefault`)
   - lines 320–323 (implicit-barrier property)

   **Fix:** add one `## Configuration` section before Key takeaways, as a table of property, default and effect, linking to `../query-table-configuration.md`. Replace the inline mentions with concept wording. Keep a pointer at line 124.

3. **The "need both" rule is stated four times with drifting wording (144, 280, 283, 285).** It goes from "often need both" to "typically need both" to "That's not always true". **Fix:** state it once, as the callout after the barrier example, with the qualification merged in. Cut the sentence at line 144 or make it a forward pointer. Keep the `with_serial` vs. barriers comparison at lines 139–142, which is well placed.

4. **Parent/child terminology mismatch (line 26 vs. lines 28, 54, 79).**
   - Line 26 promises "across tables, across rows, and across columns".
   - The children are "Across tables", "Within a single table" (rows and columns appear only as bold labels) and "Query phases and thread pools", which isn't one of the three ways.
   - The Crash Course uses separate `### Across rows` and `### Across columns` headings.

   **Fix:** use three `###` headings that match line 26, or rewrite line 26 to match the headings. Move the thread-pool section out (see finding 6).

5. **The Quick reference table comes before its terms, mixes kinds of rows, and is repeated at the end (13–22, 333–339).**
   - It uses "Barriers" and "implicit barriers" before they're defined at lines 236 and 318. Implicit barriers are also opt-in, not the default.
   - Its Scenario rows mix categories ("Pure column math"), a specific example ("Global counter"), a restated requirement ("Column A must finish before Column B") and logging, which doesn't change the output.
   - "Choosing an approach" restates the same three-way decision in prose.

   **Fix:** keep the table early, but add a two-line definition of the terms or forward links. Make the Scenario column all general categories, move examples into their own column, and drop logging. Fold "Choosing an approach" into the table as a "See" column and delete that section.

6. **"Query phases and thread pools" re-explains earlier content (79–89 vs. 28–60).** Line 87 re-teaches across-table concurrency (lines 50–52) and line 83 re-teaches across-row parallelism, without referring back. The section also mixes the phase concept with pool configuration. **Fix:** reduce it to a one- or two-sentence phase note in Across tables, and move the pools and properties to the Configuration section.

7. **The "when to use `with_serial`" lists drift between locations (11, 148, 338, 346).** Line 148 and line 338 list completely different cases. **Fix:** make line 148 the canonical list. Line 338 goes away with finding 5, and the summary and callout can stay short.

8. **The `view`/`update_view` fact is duplicated with different details (70 vs. 155–156).** Line 70 includes `lazy_update`; the callout doesn't. **Fix:** keep the callout in Serialization, and at line 70 either add a pointer to it or make the lists match.

9. **The GIL caution is at the wrong level of abstraction (74–77, under "Within a single table").** It's an environment caveat, and it also explains what `with_serial` guarantees 70 lines before Serialization. **Fix:** move it to the start of "When parallelization is safe by default", or to a note after the intro. Move the paragraph about the `with_serial` guarantee next to line 213.

10. **"What does NOT get parallelized" includes items outside its section and of different kinds (68–72).** Line 72, about waiting for dependencies, is about scheduling across tables, not within one table. The items mix a group of lazy operations, a user opt-out and a scheduling state. **Fix:** remove or move line 72, keep the list to operations only, and turn the `with_serial` item into a pointer. At line 66, move the sort thresholds to Configuration.

11. **"Stateful partition filters" is an orphaned aside, and its heading contradicts its content (327–331).**
    - It's filed under Controlling execution order, but it's an exception to the stateless-by-default rule at line 126.
    - The heading says "Stateful", but the body says unmarked partition filters are treated as stateless.
    - "location" isn't defined.
    - The Quick reference table doesn't mention it.

    **Fix:** move it under "When parallelization is safe by default" as "Partition filters are treated as stateless unless marked serial", define "location", and add a bridge sentence.

12. **The update graph is used before it's defined (30, 50 vs. 52).** **Fix:** swap the paragraphs at lines 50 and 52. Line 50's link text "Thread pools" doesn't match its target heading; fix it when the section moves.

13. **Implicit barriers use two names and an overloaded term (318–325).** Line 320 names both `SERIAL_SELECT_IMPLICIT_BARRIERS` and `serialSelectImplicitBarriers`. The "Stateless mode/Stateful mode" labels reuse "stateless" from line 93 with a different meaning. **Fix:** rename the modes as off and on, move the property to Configuration, and add a bridge sentence.

14. **The filter note under Multiple barriers is misplaced (line 316).** It's a general statement about barriers. **Fix:** move it into the barrier definition at line 240.

15. **The closing sections overlap (333–350).** Choosing an approach, Key takeaways and a second Crash Course pointer come in a row. The line-348 thread-pool bullet is a configuration detail, not a takeaway. **Fix:** keep only Key takeaways, and replace that bullet with the three-way model.

16. **Minor: the intro and the first section overlap (6 vs. 26).** **Fix:** start the first section directly with the three-way enumeration.

## Suggested outline after restructuring
```
intro + breaking-change callout
## Quick reference            (terms defined or forward-linked; absorbs "Choosing an approach")
## How parallelization works
  ### Across tables           (update graph defined first; brief phase note)
  ### Across rows
  ### Across columns
  (what is and isn't parallelized: operations only)
## When parallelization is safe by default
  (GIL / free-threading note)
  ### Partition filters are treated as stateless unless marked serial
## Controlling execution order
  (key concepts + with_serial vs. barriers comparison)
  ### Serialization           (canonical "when" list; view callout)
    #### Example: counter, steps 1–2
    #### Serial filters
  ### Barriers                (Filter note in the definition)
    #### Example: counter, step 3 ("need both" callout once)
    #### Multiple barriers
    #### Implicit barriers
## Configuration
## Key takeaways
## Related documentation
```

If these changes are applied, re-run `deephaven-core-accuracy-check` and `deephaven-writing-style` on the result. Merging the counter examples and moving the configuration values are the edits most likely to drop a caveat or break an anchor, such as `#example-a-counter-needs-serialization`, `#example-extending-the-counter-with-a-barrier` and `#query-phases-and-thread-pools`.
