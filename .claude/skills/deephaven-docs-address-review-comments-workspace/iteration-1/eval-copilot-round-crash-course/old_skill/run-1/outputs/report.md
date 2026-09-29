Skills used: `deephaven-core-accuracy-check` and `deephaven-writing-style`, with `ref-deephaven-doc-categories` for the category (a Crash Course page, so a Tutorial) and the Level of abstraction check from `deephaven-doc-structure-review`.

# Copilot round 7 on the Crash Course parallelization page

The file you gave me is identical to `docs/python/getting-started/crash-course/parallelization.md` at `f2ef483084`. The Groovy version at `docs/groovy/getting-started/crash-course/parallelization.md` is covered in the sweep at the end.

Copilot is right about the facts in nearly every comment. The disagreement is about where some of those facts go. Five comments (C5, C6, C7, C8, C9) ask for property names, defaults, or a much bigger example on a Crash Course page. That page is written for a reader with zero context. The concept guide (`conceptual/query-engine/parallelization.md`, section "Query phases and thread pools") and the configuration reference (`conceptual/query-table-configuration.md`) already carry those property names and defaults. My advice is to make those sentences correct in plain terms and link out, rather than add the properties inline. Doing it inline is how review rounds pile caveats into a page.

## Summary

| # | Line | Verdict | Change |
|---|---|---|---|
| C1 | 155 | **Take** | Add `from deephaven import empty_table` |
| C2 | 173 | **Take** | Rewrite the sentence so it matches the table |
| C3 | 198 | **Take** | Say `with_serial` protects one formula; shared state across columns also needs barriers (one sentence plus a link) |
| C4 | 2 | **Decline** | Crash Course titles use Title Case by convention |
| C5 | 40 | **Decline as written** | True, but the property belongs in the concept guide; swap the parenthetical for a link |
| C6 | 55 | **Decline** | The note already says this in plain terms; Copilot's version is also incomplete |
| C7 | 57 | **Take in part** | The threshold is per update, not per table size, so fix that; put the exact value behind a link, not inline |
| C8 | 180 | **Decline** | A bigger table wouldn't show the fix on standard Python builds and would slow the doc build |
| C9 | 204 | **Take in part** | Rewrite the takeaway so it isn't absolute, without listing every condition |
| C10 | 198 | **Take** | Remove the unsupported clause and give the real reason instead |

## Each comment

### C1: missing import (line 155): take

The block is tagged `syntax`, so the doc tests never run it and nothing catches the `NameError`. A reader who copies it does hit the error.

**Change:** add `from deephaven import empty_table` at the top of the block, as the fixed example at line 180 does.

> **Reply:** Good catch. Added the import so the broken example can be copied and run as-is, matching the fixed version below it.

### C2: the prose and the table disagree (line 173): take

The table shows 1, 2, 2, 4, 5, 5, 7. That's two 2s and two 5s, with 3 and 6 missing, so no row ever shows the "both return 6" case the prose describes. The skipped numbers also need a second cause. `counter += 1; return counter` reads the global counter again on return, so a call can return a value that another core already incremented.

**Change (line 173):**

> Two cores might both read `counter = 1`, both add 1, and both return 2. Other calls interleave so that some numbers are never returned at all. The result: duplicate IDs and skipped numbers, like the repeated 2s and 5s and the missing 3 and 6 above.

> **Reply:** Agreed. I rewrote the explanation to match the table: the duplicated 2 comes from two cores reading the same value, and the missing numbers come from calls interleaving. The prose and the illustration now describe the same run.

### C3: "Use `with_serial` any time your formula depends on shared state" (line 198): take

I checked this against the source. In `table-api/.../ConcurrencyControl.java`, the `withSerial` contract says "The expression will never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order". For selectables it adds: "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed... To impose further ordering constraints, use barriers."

In `QueryTable.java:400-402`, `SERIAL_SELECT_IMPLICIT_BARRIERS` defaults to `!STATELESS_SELECT_BY_DEFAULT`, which is `false`.

So `with_serial` on its own is enough only when a single formula touches the shared state. This page's example has one column, so the example is correct. The advice at line 198 is broader than the example and overstates it.

Keep the fix to one sentence on this page. The concept guide already has a `### Barriers` section with a counter example, so link there. This comment and C10 both land on the same NOTE, so I've drafted one rewrite that covers both, under C10.

> **Reply:** Agreed. `with_serial` only promises that an expression never runs concurrently with itself; with the default `serialSelectImplicitBarriers=false`, it doesn't order separate serial columns. I narrowed the note to "protects one formula" and added a pointer to the barriers section of the parallelization concept guide for state shared across columns. I didn't explain barriers on this page because it's the Crash Course; the concept guide covers them in depth.

### C4: title case (line 2): decline

The style guide's sentence-case rule is right for body headings, and every body heading on this page already uses sentence case. Page titles in the Crash Course follow a different, established convention. Nearly every chapter title uses Title Case in both languages ("Basic Table Operations", "Create Your First Tables", "Import and Export Data", "Time and Calendars", "Wrapping Up"). Changing this one page would make it the only chapter that doesn't match. If we want sentence-case titles, we should change them across the whole Crash Course in a separate PR.

> **Reply:** Every heading in the body is sentence case. The front-matter title follows the Crash Course's convention: its chapter titles are consistently Title Case ("Basic Table Operations", "Time and Calendars", "Import and Export Data"). Changing only this one would make it inconsistent with the rest of the course, so I'm leaving it. A course-wide title change would be a separate PR.

### C5: name `PeriodicUpdateGraph.updateThreads` (line 40): decline as written

The fact is correct. `PeriodicUpdateGraph.java:55` reads `updateThreads` with a default of `-1`, and lines 140-141 map `<= 0` to `Runtime.getRuntime().availableProcessors()`. Setting it to 1 does give you a single update thread.

The concept guide's "Query phases and thread pools" section already names this property and its default. Adding it here would put configuration detail in a tutorial's narrative. The parenthetical that's there now is already a small version of that problem. Replace it with a link.

**Change (line 40):**

> When new data arrives in `trades`, Deephaven's update graph makes `high_value`, `by_symbol`, and `recent` eligible to update concurrently. Whether they actually run at the same time depends on how many threads the server gives the update graph — see [thread pools](../../conceptual/query-engine/parallelization.md#query-phases-and-thread-pools).

> **Reply:** Confirmed: `updateThreads` defaults to `-1`, which means all available processors, and `1` removes this concurrency. Because this is the Crash Course, I'm keeping property names out of the narrative. I replaced the parenthetical with a link to the concept guide's "Query phases and thread pools" section, which documents both thread-pool properties and their defaults.

### C6: chunk count and `OperationInitializationThreadPool.threads` (line 55): decline

The note already says this in plain language: "This assumes the default operation-initialization thread pool, which uses one thread per core; a differently-sized pool changes the chunk count."

Copilot's version is also incomplete. In `SelectColumnLayer.java:203-208`:
- The division size is `Math.max(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS, ceil(totalSize / jobScheduler.threadCount()))`, so the chunk count isn't just the thread count. The minimum chunk size puts a floor on it.
- The operation-initialization pool supplies `jobScheduler` only when a table is first created (`QueryTable.java:1825-1829`). Later updates use the update graph's threads.

Naming one property would be precise but incomplete. The worked numbers in the note are correct: max(4,194,304, 5,000,000) gives 4 chunks of 5M rows.

**Optional simplification** of the note's last sentence:

> Exact chunk counts depend on server settings, and scheduling overhead means the speedup is rarely a perfectly linear 4x.

> **Reply:** The note already qualifies this ("assumes the default operation-initialization thread pool... a differently-sized pool changes the chunk count"). Naming only `OperationInitializationThreadPool.threads` would also be incomplete: that pool drives the initial computation, but later updates use the update graph's threads, and the minimum chunk size puts a floor on the chunk count. The concept guide covers both pools, so I've kept this at the Crash Course level rather than naming properties here.

### C7: "at least a few million rows" (line 57): take the correction, not the placement

I checked this in `SelectColumnLayer.java:190-205`:

```java
final long totalSize = upstream.added().size() + upstream.modified().size();
...
if (canParallelizeThisColumn && jobScheduler.threadCount() > 1 && !hasShifts &&
        ((resultTypeIsTableOrRowSet && totalSize > 0)
                || totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS)) {
```

The default is `1L << 22` (`QueryTable.java:336-337`).

Copilot is right that "once a table is large enough" is wrong for ticking tables. A 100M-row live table that adds 1,000 rows per cycle never splits its updates. "A few million" is fine at tutorial resolution. The exact value and the `Table`/`RowSet` exception (which Crash Course readers won't meet) belong in the configuration reference.

**Change (line 57):**

> Deephaven only splits a single column's work across cores when one operation or update has enough rows to be worth dividing — millions of rows with the [default settings](../../conceptual/query-table-configuration.md#parallel-processing-with-select). Below that, the column's own computation runs on a single core, though independent columns and other downstream tables can still run concurrently.

> **Reply:** Good point on the unit. The check is against the rows added plus modified in each update, not the table's size, so I reworded the sentence to "when one operation or update has enough rows" and linked the configuration reference for the exact property and default. I left out the `Table`/`RowSet` exception and the exact number on purpose: this is the Crash Course, and the configuration reference is where those details live.

### C8: make the fixed example larger than 4,194,304 rows (line 180): decline

- **It wouldn't demonstrate the fix for most readers.** `FormulaColumnPython.isParallelizable()` returns `PythonFreeThreadUtil.isPythonFreeThreaded()`. `DhFormulaColumn.isParallelizable()` returns `false` for any formula whose parameters include a `PyObject` or `PyCallableWrapper`, unless the Python build is free-threaded. On a standard Python build, `get_next_id` never runs in parallel, even at 5M rows, so `with_serial` isn't what makes the output correct.
- **One correct run can't prove a race is fixed.** A single correct output could have happened without `with_serial` too.
- **Cost.** The block is `test-set=serial order=result`, so the doc build runs it and snapshots it. Four million-plus Python calls would slow the build and bloat the snapshot.

The note at line 198 already tells readers the 100-row example shows the syntax and doesn't reproduce the race.

> **Reply:** I'm leaving this at 100 rows. On standard (non-free-threaded) Python builds, Python-backed formulas aren't parallelized at any size (`FormulaColumnPython.isParallelizable` depends on `PythonFreeThreadUtil.isPythonFreeThreaded()`), so a 5M-row version wouldn't exercise the race for most readers. A single correct run can't prove the fix anyway. The block is also executed and snapshotted by the doc tests. The note under the example already says it shows syntax, not the race.

### C9: "Deephaven runs formulas in parallel by default" (line 204): take in part

Every condition Copilot lists checks out:
- threshold, `threadCount() > 1`, and `!hasShifts` (`SelectColumnLayer.java:203-205`)
- free-threaded Python (`isParallelizable`, above)
- not redirected, supports parallel population, and stateless (`SelectColumnLayer.java:115-117`)

Listing them all in Key takeaways would repeat that list in the page's most-read section. The accurate short version is that Deephaven assumes formulas are stateless (`QueryTable.statelessSelectByDefault`, default `true`), which is what allows it to parallelize them.

**Change (line 204):**

> - Deephaven assumes formulas are stateless, so it can run them in parallel — this is fast but requires stateless code.

> **Reply:** Agreed that "by default" read as unconditional. I rewrote the takeaway around what the default actually is: Deephaven assumes formulas are stateless, so it can run them in parallel. The body and the linked concept guide cover when splitting actually happens; repeating every condition in the takeaways would bury the point.

### C10: "parallelization isn't the only way execution order can vary" (line 198): take

Nothing on the page supports this clause. The closest real mechanism is that live tables evaluate new and modified rows in later updates. `with_serial` doesn't change that, because it orders rows only within one evaluation. So the clause is unsupported, and it points readers toward a guarantee `with_serial` doesn't give. The same sentence ends with "`with_serial` is the only thing that guarantees rows are processed one at a time, in order". That's also absolute: non-free-threaded Python formulas, and `statelessSelectByDefault=false`, also keep a formula from being split. Remove both clauses and give the real reason to use `with_serial` at any table size.

**Combined rewrite of the NOTE at line 198 (covers C3 and C10):**

> [!NOTE]
> This example uses only 100 rows, well below the size where Deephaven would split the formula across cores, so it wouldn't show the race from the broken version above even without `with_serial`. Use `with_serial` whenever a formula depends on shared state or row order, regardless of table size — whether Deephaven parallelizes a formula depends on how many rows each update processes and on server settings, not on your code. `with_serial` protects one formula from running concurrently with itself; if more than one column shares the same state, you also need [barriers](../../conceptual/query-engine/parallelization.md#barriers).

> **Reply:** Agreed. I removed the clause, along with the "only thing that guarantees" absolute in the same sentence. The note now gives the actual reason to use `with_serial` regardless of size: whether a formula gets parallelized depends on each update's row count and server settings, which your code doesn't control.

## Same issues elsewhere on the page (not flagged by Copilot)

- **The threshold is repeated five times** (lines 55, 57, 158, 171, 198). Line 158 also names the property inline ("larger than the default `QueryTable.minimumParallelSelectRows` (about 4.2 million rows)") and uses "table larger than", when the check is `>=` against each update's row count. Suggested change: "On free-threaded Python builds, when an update has enough rows to split (see [Across rows](#across-rows)), Deephaven may run Python-backed formulas on several cores at once, so multiple cores can call `get_next_id` at the same time." For line 171, drop "(about 4.2 million rows)" and say "above the parallelization threshold". Once C7 is fixed, this cleanup keeps the next Copilot round from flagging the leftovers.
- **Line 177** paraphrases `with_serial` accurately against the javadoc. No change.
- **Line 206** ("Use `with_serial` ... when your formula needs it") refers to a single formula, so it's consistent with C3. No change.

## Groovy version (`docs/groovy/getting-started/crash-course/parallelization.md`)

Apply the same fixes to its matching lines:
- **C3 + C10:** line 162, same NOTE text with `withSerial`.
- **C9:** line 168.
- **C7:** line 55.
- **C5:** line 38.
- **Line 144** names `QueryTable.minimumParallelSelectRows` inline, like Python line 158. Apply the same cleanup.
- **C1 doesn't apply.** Groovy uses `emptyTable` without an import throughout the file.
- **C2 doesn't apply.** Groovy has no illustrative table, so its "read 5, both return 6" prose (line 141) doesn't contradict anything. Consider aligning it with the Python wording anyway.

**Author query for Groovy (AQ1, "The fix" section, code block at line 150):** the fixed example changes two things at once. It replaces the closure with `AtomicInteger` and adds `withSerial`. `AtomicInteger.incrementAndGet` already makes IDs unique without `withSerial`, so what `withSerial` adds here is only row order. The C8 argument doesn't carry over, because a Groovy closure is not gated by free-threading (`DhFormulaColumn.isParallelizable` returns `true` when no Python parameters are involved). The example should either keep the broken example's plain closure and add `withSerial`, or say explicitly that `withSerial` is there for row order. I didn't confirm that a Groovy closure is valid inside `Selectable.parse`, so please check that before switching to it.

## Separate follow-up (outside this PR)

`conceptual/query-table-configuration.md`, section "Parallel processing with `select`", says parallelism starts once "the parent's size exceeds `QueryTable.minimumParallelSelectRows`". The code compares `>=` against the rows added plus modified in the update, not the parent's size. It's worth a small fix, especially since C7 now links to that section.

No files were edited.