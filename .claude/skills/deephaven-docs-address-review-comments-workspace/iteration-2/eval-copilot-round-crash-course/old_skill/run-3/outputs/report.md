Skills used: deephaven-core-accuracy-check (loads ref-deephaven-doc-categories) and deephaven-writing-style.

# Copilot round 7 on `crash-course/parallelization.md`: what to take and what to say

This page is a **Tutorial**: it's a Crash Course chapter under `getting-started/crash-course/`. So I checked two things for each comment: is the claim true, and does the fix belong in this page's narrative? Property names and defaults belong in the concept guide or the configuration reference, not here. The concept guide `conceptual/query-engine/parallelization.md` already names `OperationInitializationThreadPool.threads` and `PeriodicUpdateGraph.updateThreads` in "Query phases and thread pools", and has a "Barriers" section. At f2ef483084 the page is identical to the eval file. It also has a Groovy sibling, `docs/groovy/getting-started/crash-course/parallelization.md`, which shares several of the flagged sentences.

## Summary

| # | Line | Verdict | Short reason |
|---|------|---------|--------------|
| C1 | 155 | **Take** | Real `NameError` on copy-paste |
| C2 | 173 | **Take** | Prose and table disagree (table has no 6, and 3 is missing too) |
| C3 | 198 | **Take, in plain words** | Correct per `withSerial` contract; don't name the property |
| C4 | 2 | **Decline for this PR** | Every Crash Course title and sidebar label uses title case; fix them all together or not at all |
| C5 | 40 | **Decline** | Already accurate; the property belongs in the concept guide, which already has it |
| C6 | 55 | **Decline** | The note already qualifies the pool size; Copilot's framing is also incomplete |
| C7 | 57 | **Partial** | "A few million" is fine; the "table size vs. rows per update" point is worth a plain-language fix; no property or number inline |
| C8 | 180 | **Decline** | Bigger table wouldn't show anything on standard Python and would make the test slow; `with_serial` is needed for the guarantee regardless |
| C9 | 204 | **Take, in plain words** | "Runs in parallel by default" is too absolute; reword without listing the four gates |
| C10 | 198 | **Keep the claim, name the mechanism** | Source supports it; Copilot's "unsupported" means only that the page doesn't explain it |

---

## C1: missing `empty_table` import (line 155). Take.

**Change:** add `from deephaven import empty_table` as the first line of the broken-counter block (lines 144–156). The block is tagged `syntax`, so the test never runs it, but readers copy it. The Groovy sibling doesn't need this (`emptyTable` is available there without an import).

**Reply:**
> Good catch. The block is `syntax`-only, so tests never ran it. Added `from deephaven import empty_table` so it runs when copied.

## C2: "both return 6" vs. the table (line 173). Take.

**Verified:** the table (lines 160–168) is 1, 2, 2, 4, 5, 5, 7. 2 and 5 are duplicated, and **both 3 and 6** are missing (Copilot only mentions 6).

**Change (line 173):**
> Two cores might simultaneously read `counter = 4`, both add 1 to get 5, and both return 5. The result: duplicate IDs (2 and 5 each appear twice) and skipped numbers (3 and 6 are missing).

The Groovy sibling (line 141) has the same sentence but no output table, so nothing contradicts it there. No change needed.

**Reply:**
> Fixed. The walkthrough now uses `counter = 4` → 5, which matches the duplicated 5s in the table, and names both skipped values (3 and 6; the table skips 3 too).

## C3: `with_serial` overstated for shared state (line 198). Take, at tutorial level.

**Verified:** `table-api/.../ConcurrencyControl.java` promises that the expression "will never be invoked concurrently with itself". If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, "no additional ordering between selectable expressions is imposed … To impose further ordering constraints, use barriers." In `QueryTable.java`, `SERIAL_SELECT_IMPLICIT_BARRIERS = !STATELESS_SELECT_BY_DEFAULT`, and `statelessSelectByDefault` defaults to `true`, so the effective default is `false`. Copilot is right.

(Side finding: that javadoc says the property defaults "to the value of `QueryTable.statelessSelectByDefault`", but the code negates it. That's a javadoc bug worth a separate ticket.)

This page's own example is fine: only one column touches the counter. What's wrong is the general advice on line 198. Line 177 matches the contract ("never running concurrently with itself … row-set order"). Takeaway line 206 speaks of a single formula, so it's fine too.

**Change:** don't add the property name. After the **Trade-off** paragraph (line 200), add a short paragraph of its own:
> **Shared state across columns**: `with_serial` keeps a formula from running concurrently with *itself*. If two or more columns read or modify the same state, marking each one with `with_serial` isn't enough — you also need [barriers](../../conceptual/query-engine/parallelization.md#barriers).

Rewrite the note at line 198 as shown under C10 below. **Make the same change in Groovy** (lines 162–164, using `withSerial`).

**Reply:**
> Agreed. `with_serial` only guarantees the expression "will never be invoked concurrently with itself", and by default nothing orders two serial columns relative to each other. I added a short paragraph saying that when several columns share state you also need barriers, with a link to the Barriers section of the concept guide. I left the `serialSelectImplicitBarriers` property out of the Crash Course on purpose; the concept guide covers it. Applied to the Groovy page too.

## C4: title case in "Query Parallelization" (line 2). Decline for this PR.

**Verified:** across `docs/python`, sentence-case titles clearly outnumber title-case ones, so the rule matches the site. But every Crash Course chapter title uses title case ("Create Your First Tables", "Basic Table Operations", "Time and Calendars", "Real-time Plots"), and so does every Crash Course sidebar label ("Query Parallelization", "Query Strings"). Changing only this chapter would make it the one odd entry in the Crash Course sidebar. A real fix also has to touch the `label` in both `sidebar.json` files.

**Recommendation:** leave it and open a follow-up to sentence-case all Crash Course titles and labels in both languages at once. If you'd rather do it now, change the `title`, both sidebar labels, and the Groovy title together.

**Reply:**
> The style guide does call for sentence case, but every Crash Course chapter title and sidebar label currently uses title case. Changing just this one would make it the odd one out in the Crash Course nav. I'll handle all Crash Course titles in a separate follow-up (both languages, including `sidebar.json`) instead of changing one chapter here.

## C5: name `PeriodicUpdateGraph.updateThreads` (line 40). Decline.

**Verified:** the sentence is accurate. `PeriodicUpdateGraph.java` reads `updateThreads` with default `-1`, and any value ≤ 0 becomes `Runtime.getRuntime().availableProcessors()`. With 1 thread it uses `QueueNotificationProcessor`, so updates never run concurrently. Copilot's facts are right, but the sentence already says concurrency "depends on the update graph's thread pool". A tutorial shouldn't name the property inline, and the concept guide's "Query phases and thread pools" section already documents it with its default.

**Reply:**
> The sentence is accurate: the default of `-1` becomes `availableProcessors()`, and it already says concurrency depends on the update graph's thread pool. This is the Crash Course, so we keep property names out of the narrative. `PeriodicUpdateGraph.updateThreads` and its default are documented in the concept guide's "Query phases and thread pools" section, which this page links to at the end.

## C6: qualify chunk count with `OperationInitializationThreadPool.threads` (line 55). Decline.

**Verified:**
- The note already says: "This assumes the default operation-initialization thread pool, which uses one thread per core; a differently-sized pool changes the chunk count." That covers Copilot's concern.
- Copilot's framing is also incomplete:
  - `SelectColumnLayer.java` computes `divisionSize = max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(totalSize / threadCount))`, so the minimum-rows floor also caps the chunk count, not just the thread count.
  - The initialization pool governs only initialization (`OperationInitializerJobScheduler` in `QueryTable.java`). Updates use the update graph's pool (`UpdateGraphJobScheduler` in `SelectOrUpdateListener.java`).
- The note's own numbers check out: 20M rows / 4 threads = 5M per chunk, which is above 4,194,304, so there are 4 chunks.

**Reply:**
> The note already says it assumes the default operation-initialization pool and that a different pool size changes the chunk count. Naming the property here would add configuration detail to a tutorial, and the concept guide already documents `OperationInitializationThreadPool.threads`. Also, the thread count alone doesn't set the chunk count: the minimum-rows floor caps it, and updates use the update graph's pool, not the initialization pool. So "chunk count = `OperationInitializationThreadPool.threads`" would be a less accurate statement.

## C7: exact threshold (line 57). Partial.

**Verified:**
- `QueryTable.minimumParallelSelectRows` defaults to `1L << 22` (4,194,304).
- The check is `totalSize >= MINIMUM_PARALLEL_SELECT_ROWS`, where `totalSize = upstream.added().size() + upstream.modified().size()`.
- Formulas that return `Table` or `RowSet` are split whenever `totalSize > 0`.

All of Copilot's facts are correct. But "a few million" is an accurate description of 4.2M at tutorial level. The one point that could actually mislead a reader is "once a **table** is large enough": a huge live table that grows by small updates never crosses the threshold. The `Table`/`RowSet` exception is out of scope for a Crash Course reader.

**Change (line 57):**
> Deephaven only splits a single column's row-wise computation across cores when there are enough rows to compute at once — a few million by default. For a live table, that means the rows added or changed in one update, not the table's total size. Below that threshold, that column's own computation runs on a single core, though independent columns and other downstream tables can still run concurrently.

**Related cleanup (same problem, flagged by my sweep):** lines 158 and 171 already put the property and number inline ("the default `QueryTable.minimumParallelSelectRows` (about 4.2 million rows)" and "about 4.2 million rows"). That contradicts line 57's level of detail and adds configuration detail to a tutorial.
- Rewrite 158 as "…with a table above the parallelization threshold (a few million rows)…".
- Drop "(about 4.2 million rows)" from 171.
- If you want the exact value reachable, link once to `../../conceptual/query-table-configuration.md` (it lists `QueryTable.minimumParallelSelectRows` with `1L << 22`).
- In Groovy, line 144 does the same and should get the same fix.

**Reply:**
> The exact value and the `>=` against added-plus-modified rows are right. For the Crash Course I kept "a few million by default" and left out the property name and the `Table`/`RowSet` special case; those are in the query table configuration reference. Your point that the count is per update, not the table size, is the one that could mislead a reader, so I added a sentence about it. I also removed the inline property name and "about 4.2 million" from the broken-counter section so the page states the threshold one way, consistently.

## C8: grow the corrected example past 4,194,304 rows (line 180). Decline.

**Verified:**
- In Python, `DhFormulaColumn.isParallelizable()` returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`. `get_next_id` is a Python callable, so on a standard (GIL) build this formula is **never** split across cores, at any size. A 5M-row example wouldn't show a race on most readers' builds.
- It would also make the snapshot test call a Python function more than 4 million times.
- More importantly, `with_serial` is needed at 100 rows for the *guarantee*. `SelectColumn.isParallelizable()`'s javadoc says: "Even if a column is not parallelizable, the engine may choose to evaluate it out-of-order and may not evaluate all rows individually." The note at line 198 already tells readers the 100-row example wouldn't show the race.

**Reply:**
> I'm keeping this at 100 rows. The example shows the syntax, and `with_serial` is what guarantees the result at any size. The engine's contract allows out-of-order evaluation even for columns it doesn't parallelize, and the note under the example already says this size wouldn't show the race. Raising it past 4.2M rows wouldn't show the race on standard Python either: Python-backed formulas are parallelized only on free-threaded builds. It would also make the doc test call a Python function more than 4 million times.

## C9: "runs formulas in parallel by default" (line 204). Take, at tutorial level.

**Verified:** splitting only happens if all of these hold:
- `canParallelizeThisColumn` (the column is stateless and parallelizable; Python-backed only on free-threaded builds),
- `jobScheduler.threadCount() > 1`,
- `!hasShifts`,
- and the row threshold is met.

So "runs in parallel by default" is false for every example on this page and for any Python UDF on a standard build. What *is* the default is the assumption: `QueryTable.statelessSelectByDefault=true`.

**Change (line 204):**
> - By default, Deephaven assumes your formulas are stateless and is free to run them in parallel — so formulas need to be stateless unless you mark them otherwise.

Don't list the four conditions in a takeaway bullet. Same fix for Groovy line 168.

**Sweep result:** the code comment at line 154, `# INCORRECT: parallel execution corrupts the counter`, states the same absolute claim. Change it to `can corrupt`.

**Reply:**
> Agreed that it's too absolute. What's actually the default is the *assumption* that formulas are stateless, not parallel execution itself. I reworded the takeaway to say that, and softened the matching code comment in the broken example. I didn't list the individual conditions (threshold, thread count, shifts, free-threaded Python) in the takeaways; the "Across rows" section and the concept guide cover when splitting happens. Applied to Groovy too.

## C10: "parallelization isn't the only way execution order can vary" (line 198). Keep it, and state the mechanism.

**Verified:** the claim is supported. `SelectColumn.isParallelizable()` javadoc says: "Even if a column is not parallelizable, the engine may choose to evaluate it out-of-order and may not evaluate all rows individually." The concept guide makes the same point ("the engine may still evaluate a non-parallelizable column out of order"). The page just never explains it, which is what Copilot noticed. Removing the clause would weaken the reason for "regardless of table size". Separately, "`with_serial` is the only thing that guarantees…" is an absolute I couldn't fully verify, so drop it.

**Change (line 198 note, combined with C3):**
> This example uses only 100 rows, well below the threshold where Deephaven would split it across cores, so it wouldn't show the race from the broken version above even without `with_serial`. Use `with_serial` any time your formula depends on shared state or row order, regardless of table size: even when Deephaven doesn't split a formula across cores, it doesn't promise to evaluate rows in order or to evaluate every row individually. `with_serial` guarantees rows are processed one at a time, in row-set order.

Same change in Groovy line 162.

**Reply:**
> The claim is correct. The engine's contract (`SelectColumn.isParallelizable`) says a column that isn't parallelized may still be evaluated out of order and may not be evaluated row by row. You're right that the page didn't explain it, so the note now says so in plain terms instead of hinting at it. I also dropped the "only thing that guarantees" wording.

---

## Changes outside the flagged lines

- **Groovy sibling** (`docs/groovy/getting-started/crash-course/parallelization.md`): apply C3, C7 (line 55 plus the property in the line 144 note), C9 (line 168), and C10 (line 162). C1 and C2 don't apply there.
- **Consolidation (C7):** after these edits, the threshold appears only as "a few million", with at most one link to the configuration reference. No `QueryTable.*` property names should be left in the page. Re-read "When it breaks" through "Key takeaways" as a whole after editing, so the C3 and C10 additions don't pile up into stacked caveats.
- **Separate tickets:**
  - The `ConcurrencyControl.withSerial` javadoc misstates the `serialSelectImplicitBarriers` default.
  - A Crash Course sentence-case title sweep (C4).

Nothing has been edited.