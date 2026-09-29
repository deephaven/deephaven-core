Skills used: `deephaven-core-accuracy-check`, with `deephaven-writing-style` for C4 and `ref-deephaven-doc-categories` to classify the page.

# Copilot round 7 on the Crash Course parallelization page

I checked each comment against the source and against the docs tree at `f2ef483084`. The eval file is byte-identical to `docs/python/getting-started/crash-course/parallelization.md` at that commit.

**Page category:** This is a **Tutorial** (Crash Course). The reader has no prior context, so property names, defaults and thresholds don't belong in the narrative. They go in the concept guide (`conceptual/query-engine/parallelization.md`) or the configuration reference, reached by a link.

That matters here. C5, C6, C7 and C9 are each true, but taking them as written would put four more configuration details into a page that already has two.

## Summary

| # | Line | Verdict | Change |
|---|---|---|---|
| C1 | 155 | **Take** | Add the missing import |
| C2 | 173 | **Take** | Rewrite the prose so it matches the table |
| C3 | 198 | **Take, at lower resolution** | Say that shared state across columns also needs barriers, and link to them. Apply the same fix to the takeaway on line 206 |
| C4 | 2 | **Decline** | Crash Course titles use Title Case |
| C5 | 40 | **Decline** | The claim is already accurate. At most, link to the concept guide's thread-pool section |
| C6 | 55 | **Decline** | The note already has this qualification |
| C7 | 57 | **Take the substance, not the numbers** | Say "update", not "table". Keep the property out of the prose |
| C8 | 180 | **Decline** | A bigger table would make the example slow and still wouldn't show the race on standard Python |
| C9 | 204 | **Take, at lower resolution** | Soften "by default" without listing the conditions |
| C10 | 198 | **Take** | Remove the unsupported clause, and also the "only thing" absolute in the same sentence |

---

## C1 (line 155): missing `empty_table` import. Take it.

The block is tagged `syntax`, so the docs tests never run it. A reader who copies it still gets a `NameError`. Every other Python block on the page imports what it uses.

**Change:** Add `from deephaven import empty_table` as the first line of the block (lines 144–156).

**Reply:**
> Good catch — fixed. The block is `syntax`-only so tests didn't catch it, but a copy-paste would fail. Added `from deephaven import empty_table`.

## C2 (line 173): prose and table disagree. Take it.

The table (lines 160–168) shows `1, 2, 2, 4, 5, 5, 7`. The prose says both cores "return 6", but 6 isn't in the table.

The prose is also a little off on the mechanism. When two cores read 5 and both write 6, one increment is lost, which gives a duplicate but not a skipped number. A number gets skipped when a core reads `counter` back after another core has already incremented it again. Increment and return are separate steps in `get_next_id`.

**Change:** Replace line 173 with:

> Incrementing `counter` and returning it are separate steps, so two cores calling `get_next_id` at the same moment can interfere with each other. Both can end up returning the same ID (the repeated 2s and 5s above), and some IDs are never returned at all (3 and 6).

The Groovy sibling has the same sentence (line 141) but no table, so it has no mismatch. You could leave it as is. Changing it to the same wording would keep the two pages aligned.

**Reply:**
> Agreed — the prose named a value the table doesn't show. Rewrote it to describe the race in terms of what the table actually shows (duplicate 2s/5s, missing 3 and 6), which also explains why numbers get skipped, not just duplicated.

## C3 (line 198): `with_serial` isn't enough for state shared across columns. Take it, with a link instead of the property.

The comment is correct. `table-api/src/main/java/io/deephaven/api/ConcurrencyControl.java` says the serial expression "will never be invoked concurrently with itself." It also says that when `SERIAL_SELECT_IMPLICIT_BARRIERS` is false, "no additional ordering between selectable expressions is imposed … To impose further ordering constraints, use barriers."

That flag defaults to `!statelessSelectByDefault`, which is false (`QueryTable.java:400–402`, `:387–388`). So two serial columns sharing one counter can still run concurrently with each other.

The page's own example has only one column, so it is correct. The general advice ("any time your formula depends on shared state") is the overstatement. The takeaway on line 206 repeats it in shorter form.

`SERIAL_SELECT_IMPLICIT_BARRIERS` doesn't belong in a Crash Course. Barriers are covered in the concept guide (`#barriers`, including "Example: extending the counter with a barrier").

**Change:** Rewrite the note on line 198 (this also covers C10):

> This example uses only 100 rows, well below the size where Deephaven would split the work across cores, so it wouldn't show the race even without `with_serial`. Use `with_serial` whenever a formula depends on shared state or row order, regardless of table size: it guarantees the formula never runs concurrently with itself and processes rows in order. If more than one column reads or updates the same state, `with_serial` alone isn't enough. You also need [barriers](../../conceptual/query-engine/parallelization.md#barriers).

Change the line 206 takeaway to:

> - Use `with_serial` to force sequential execution when a formula needs it. If several columns share state, add barriers too.

Apply both changes to the Groovy sibling (lines 162 and 170).

**Reply:**
> Correct — `withSerial` only guarantees the expression "will never be invoked concurrently with itself," and with implicit barriers off by default, two serial columns can still interleave. Added a sentence saying shared state across columns also needs barriers, with a link to the barriers section of the concept guide. I left the property name out on purpose: this is a Crash Course chapter, and the concept guide covers the configuration. Also fixed the same claim in the Key takeaways.

## C4 (line 2): title case. Decline.

The style rule is "Sentence case in headings." Every heading in the page body already follows it.

The front-matter `title` follows a different, established convention. 12 of the 14 other Python Crash Course chapter titles are Title Case, for example "Crash Course Overview", "Basic Table Operations", "Create Your First Tables" and "Real-time Plots". The two exceptions are "Recipes, not loops!" and "deephaven.ui". Retitling this chapter alone would make it the odd one out.

If you'd rather move the whole Crash Course to sentence case, do it in a separate PR. This is a judgment call; if you prefer to follow the literal rule, the change is one line.

**Reply:**
> Keeping this one as-is: the Crash Course chapter titles consistently use Title Case ("Crash Course Overview", "Basic Table Operations", "Create Your First Tables", …), and changing only this chapter would make it inconsistent with its siblings. Headings within the page already use sentence case. Retitling the whole Crash Course would be a separate change.

## C5 (line 40): name `PeriodicUpdateGraph.updateThreads`. Decline.

The existing claim is accurate:
- `PeriodicUpdateGraph.java:55` reads `PeriodicUpdateGraph.updateThreads` with default `-1`.
- Lines 140–141: `if (numUpdateThreads <= 0) { this.updateThreads = Runtime.getRuntime().availableProcessors(); }`

So "sized to your CPU cores by default" is correct. The sentence already says concurrency "depends on the update graph's thread pool", which covers the `updateThreads=1` case.

Naming the property and its default in the prose would be configuration injection in a tutorial. The concept guide already gives both, in "Query phases and thread pools".

**Optional change:** Link "the update graph's thread pool" to `../../conceptual/query-engine/parallelization.md#query-phases-and-thread-pools`. Do the same on Groovy line 38.

**Reply:**
> The sentence is accurate as written (`updateThreads` defaults to `-1`, which resolves to `availableProcessors()`), and it already says concurrency depends on the thread pool. Since this is a Crash Course chapter, I'd rather not put property names and defaults in the narrative. I linked "the update graph's thread pool" to the concept guide's thread-pools section, which documents `PeriodicUpdateGraph.updateThreads` and its default.

## C6 (line 55): name `OperationInitializationThreadPool.threads`. Decline.

The note already says: "This assumes the default operation-initialization thread pool, which uses one thread per core; a differently-sized pool changes the chunk count."

That is the qualification Copilot is asking for, minus the property name. The example is correct for a static `empty_table`, which runs during initialization. The default is `-1`, and `ThreadHelpers.getOrComputeThreadCountProperty` turns a value of 0 or less into `availableProcessors()`.

I also checked the numbers in `SelectColumnLayer.java:206–208`: `divisionSize = max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(totalSize / threadCount))`. For 20M rows on 4 threads that gives 5M-row chunks, so the "four chunks of roughly 5 million" example holds.

The concept guide names the property ("Query phases and thread pools").

**Reply:**
> The note already qualifies this ("assumes the default operation-initialization thread pool … a differently-sized pool changes the chunk count"), and the 20M-rows/4-threads arithmetic matches `SelectColumnLayer`'s division logic. I'd rather not add the property name to a Crash Course note; `OperationInitializationThreadPool.threads` is documented in the concept guide's thread-pools section.

## C7 (line 57): exact threshold, `>=`, per-update size, Table/RowSet results. Take the substance, not the numbers.

Everything in the comment checks out in `SelectColumnLayer.java:191–205`:
- `totalSize = upstream.added().size() + upstream.modified().size()`
- The condition is `(resultTypeIsTableOrRowSet && totalSize > 0) || totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS`
- `QueryTable.java:337` sets the default to `1L << 22`.

The part a reader needs is "update, not table". Line 57 says "once a table is large enough", which is only right when a table is first created. A large ticking table that adds a few rows per cycle never gets split. The exact number, the `>=`, and the Table/RowSet exception are configuration and reference detail.

**Change:** Line 57 (and Groovy line 55):

> Deephaven only splits a single column's row-wise computation across cores when an update touches enough rows at once, at least a few million. That typically happens when a large table is first created, not when a ticking table adds a few rows each cycle. Smaller updates run that column's computation on a single core, though independent columns and other downstream tables can still run concurrently.

**Reply:**
> Good point about the threshold applying to each update (added + modified), not the table size — reworded to say that, and to note that a ticking table adding a few rows per cycle won't be split. I kept "a few million" rather than the exact value and property: this is a Crash Course chapter, and the exact threshold (plus the Table/RowSet special case) is in the concept guide and the query table configuration reference.

## C8 (line 180): make the corrected example exceed 4,194,304 rows. Decline.

A larger table wouldn't show the race on the Python builds most readers use.

`FormulaColumnPython.isParallelizable()` returns `PythonFreeThreadUtil.isPythonFreeThreaded()`. `DhFormulaColumn.isParallelizable()` returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`. `SelectColumnLayer` only parallelizes when `sc.isParallelizable()` is true.

So on a standard (GIL) build, `get_next_id()` never splits across rows at any size. The docs test runner would also call a Python function 4.2 million-plus times for a Crash Course example.

The note on line 198 already says openly that 100 rows won't show the race. With C3 applied, it gives the real reason to use `with_serial`: its guarantee, not one run's output.

**Reply:**
> Deliberately keeping 100 rows. On standard (non-free-threaded) Python builds, formulas that call Python are never split across rows (`isParallelizable()` is false), so a 4.2M-row table wouldn't show the race for most readers — it would just make a Crash Course example very slow. The note below the example says openly that it won't show the race, and it now explains that the reason to use `with_serial` is its guarantee, not the output of one run.

## C9 (line 204): "runs formulas in parallel by default" is too absolute. Take it, without listing conditions.

Copilot's conditions are correct but incomplete. The full gate in `SelectColumnLayer` is `canParallelizeThisColumn` (not redirected, destination supports parallel population, `isStateless()`, `isParallelizable()`) `&& jobScheduler.threadCount() > 1 && !hasShifts &&` the size test.

Five or more conditions don't belong in a Key takeaways bullet. What does belong there is the correct idea: Deephaven assumes formulas are stateless and may run them concurrently.

**Change:** Line 204 (and Groovy line 168):

> - Deephaven assumes formulas are stateless and runs them in parallel when it can, so write formulas that depend only on their own row.

**Reply:**
> Agreed that "by default" overstated it. Reworded the takeaway to "assumes formulas are stateless and runs them in parallel when it can," which is accurate without listing every eligibility condition in a summary bullet. The detailed conditions belong in the concept guide.

## C10 (line 198): "parallelization isn't the only way execution order can vary". Take it.

I found nothing in source or on the page that backs this clause, so remove it. There is no fact here to check with an SME.

The rest of that sentence has its own problem: "`with_serial` is the only thing that guarantees rows are processed one at a time, in order." Setting `QueryTable.statelessSelectByDefault=false` also makes formulas non-stateless and therefore not parallelized (`DhFormulaColumn.isStateless()`, `FormulaColumnPython.isStateless()`). So "only thing" is also an absolute.

The C3 rewrite above drops both. Apply it to Groovy line 162 too.

**Reply:**
> Removed — the page doesn't explain any other mechanism, so the clause was unsupported. I also dropped "the only thing that guarantees…" from the same sentence, since it was another absolute; the note now just states what `with_serial` guarantees.

---

## Found while checking for the same issues elsewhere (not raised by Copilot)

1. **Configuration injection already on the page.** Lines 158 and 171 put `QueryTable.minimumParallelSelectRows` "(about 4.2 million rows)" in the narrative, and line 158 says "larger than" when the check is `>=`. If you decline C5, C6 and C7 on placement grounds, these two lines contradict that reasoning. Proposed wording:
   - Line 158: "On free-threaded Python builds, once an update is large enough for Deephaven to split it across cores, multiple cores can call `get_next_id` at the same time."
   - Line 171: "…the duplicate IDs shown above would appear only when an update is above the [parallelization threshold](../../conceptual/query-table-configuration.md#parallel-processing-with-select)."
   - Groovy line 144 needs the same change.
2. **The Groovy fix example doesn't really need `withSerial` for unique IDs.** Groovy lines 150–159 switch to `AtomicInteger.incrementAndGet()`, which is already atomic. IDs would be unique even when run in parallel. `withSerial` only adds row-order assignment. The Python version keeps the plain counter, so the two pages now teach slightly different lessons. Either return Groovy to a plain counter, or say that `withSerial` is what makes the IDs follow row order.
3. **A separate follow-up page has the same inaccuracy as C7.** `conceptual/query-table-configuration.md` (both languages, "Parallel processing with `select`") says parallelism is enabled when "the parent's size exceeds" the threshold. The code compares the update's added + modified count with `>=`.

Links I checked at `f2ef483084`: all existing links resolve, including `Selectable.md#with_serial` and Groovy `#withserial`. The proposed link targets also exist: the concept guide's `#barriers` and `#query-phases-and-thread-pools` anchors, and `query-table-configuration.md#parallel-processing-with-select`.

No files were edited.