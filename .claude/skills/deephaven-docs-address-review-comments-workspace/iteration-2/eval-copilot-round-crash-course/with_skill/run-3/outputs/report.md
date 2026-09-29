Skills used: `deephaven-docs-address-review-comments`, plus the skills it references: `ref-deephaven-doc-categories`, `deephaven-core-accuracy-spot-check`, `deephaven-core-accuracy-check` and `deephaven-writing-style`.

# Copilot round 7 on the Crash Course parallelization page

**Recommendation:** Take C1, C2, C3 and C9, and decline C4 through C8 and C10. C3 and C9 need the same fix in the Groovy sibling. This is round 7, so I applied the late-round bar: only changes that fix something flatly wrong. I haven't edited any file.

The page is a Crash Course chapter, which makes it a **Tutorial**. Property names, defaults and thresholds stay out of the narrative there. I checked it against `f2ef483084`. The Groovy sibling, `docs/groovy/getting-started/crash-course/parallelization.md`, exists at that commit and I checked it as well.

## Page purpose

A first-time reader should come away knowing three things:
- Deephaven parallelizes work automatically.
- Formulas that use shared state or depend on row order break silently under parallel execution.
- `with_serial` is the fix, and the concept guide covers the details, such as barriers.

The page works at the level of ideas, not configuration.

## Triage

| # | Location | Comment (short) | Premise | Decision | Reason | Proposed text / destination |
|---|---|---|---|---|---|---|
| C1 | L155 | Broken-counter block uses `empty_table` without importing it | **true** | **Apply** | Code a reader copies won't run. The `syntax` tag means CI never catches it. | Add `from deephaven import empty_table` |
| C2 | L173 | Prose says "both return 6", but the table has no 6 | **true** (the table shows duplicate 2s and 5s, and no 3 or 6) | **Apply** | The prose contradicts the example right above it | Change the prose to match the table's 5s |
| C3 | L198 | "Use `with_serial` any time … shared state" overstates what it does | **true**. See source quotes below. | **Apply** (even in a late round) | An overstated prescription is flatly wrong. Fix it with one plain sentence and a link, and sweep the takeaways and the Groovy sibling. | Rewrite the note; also fix takeaway L206 and Groovy L162/L170 |
| C4 | L2 | Title should be sentence case | **true** as a style rule, **but** it conflicts with Crash Course convention | **Decline** (for this PR) | 13 of 14 Python Crash Course titles use title case ("Create Your First Tables", "Basic Table Operations", "Time and Calendars", and so on). The sidebar labels match, and so does the Groovy sibling. Changing one title makes the set inconsistent. | See AQ2: a separate pass across the whole Crash Course |
| C5 | L40 | Name `PeriodicUpdateGraph.updateThreads` and its default | **true-but-handled** | **Decline** | The sentence is already correct at tutorial level. It says "by default", and it says concurrency "depends on the update graph's thread pool". The property is documented in the concept guide the page already links to. | Already in the concept guide's **Query phases and thread pools** section |
| C6 | L55 | Qualify the chunk count by `OperationInitializationThreadPool.threads` | **true-but-handled** | **Decline** | The note already says "assumes the default operation-initialization thread pool… a differently-sized pool changes the chunk count." Adding the property name would be configuration injection. | Same concept-guide section |
| C7 | L57 | Replace "a few million rows" with the exact threshold, `>=`, added+modified, and the Table/RowSet exception | **true** (all verified), but not a defect at this level | **Decline** | "A few million rows" is correct in a tutorial. The precise rule is reference material. | `conceptual/query-table-configuration.md#parallel-processing-with-select` |
| C8 | L180 | Grow the corrected example past 4,194,304 rows | **true-but-handled** | **Decline** | The note at L197–198 already says the example is too small to race. A bigger table makes the build slower and teaches nothing more. (Reasons below.) | none |
| C9 | L204 | Qualify "runs formulas in parallel by default" with four conditions | **true** | **Apply, with a different fix** | The takeaway contradicts the page itself (L57, L171 and L158 say small tables and GIL-build Python don't parallelize). The fix is to make the claim *less* specific, not to add the four conditions. | Takeaway rewrite below; same for Groovy L168 |
| C10 | L198 | "parallelization isn't the only way execution order can vary" is unsupported; remove it | **false**. The source and the concept guide support it. | **Decline** removal | A true statement stays even when the page doesn't explain it. The C3 rewrite keeps the idea and adds a plain-language reason. | Folded into the C3 rewrite |

### Source checks behind the triage

- **C3.** `table-api/.../api/ConcurrencyControl.java`, in `withSerial`, promises "The expression will never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order." For selectables, it adds: "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed… To impose further ordering constraints, use barriers."
  - The default is false. `QueryTable.java:400-401` sets `serialSelectImplicitBarriers` to `!STATELESS_SELECT_BY_DEFAULT`, and `statelessSelectByDefault` defaults to `true` (`:387-388`). So two serial columns sharing one counter can still run at the same time.
  - "`with_serial` is the only thing that guarantees…" is also too strong. Setting `statelessSelectByDefault=false` makes columns serial as well.
- **C5.** `PeriodicUpdateGraph.java:55` reads `updateThreads` with a default of `-1`, and `:140-141` turns `<= 0` into `Runtime.availableProcessors()`.
- **C6.** In `SelectColumnLayer.java:203-208`, the split uses `jobScheduler.threadCount()`. That is the initialization pool for initial results and the update-graph pool for updates. Each chunk is `max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(total/threads))`, so 20M rows on 4 threads gives 4 chunks of 5M. That matches the note, so the example is correct as written.
- **C7.** `QueryTable.java:336-337` has `minimumParallelSelectRows` with a default of `1L << 22`. `SelectColumnLayer.java:191` sets `totalSize = added + modified`, the check is `>= MINIMUM_PARALLEL_SELECT_ROWS`, and Table/RowSet results split whenever `totalSize > 0`.
- **C9.** Beyond the size threshold, `SelectColumnLayer.java:115-117, 203` requires `isStateless() && isParallelizable()`, `threadCount() > 1` and `!hasShifts`. `FormulaColumnPython.java:63-65` and `DhFormulaColumn.java:903-905` only parallelize Python-backed formulas when `PythonFreeThreadUtil.isPythonFreeThreaded()`.
- **C10.** The concept guide at `f2ef483084` says: "Not running concurrently isn't the same as running in row-set order — the engine may still evaluate a non-parallelizable column out of order." In the serial path of `SelectColumnLayer.java` (around lines 395-420), modified rows are applied before added rows. Execution order therefore varies with no parallelism involved.

## Proposed edits

**C1: Python, broken-counter block (L144–156).** Add an import at the top:

```python syntax
from deephaven import empty_table

counter = 0
...
```

The Groovy version needs nothing, because `emptyTable` is available by default.

**C2: Python L173.**

> Two cores might simultaneously read `counter = 4`, both add 1 to get 5, and both return 5. The result: duplicate IDs and skipped numbers.

Groovy L141 has the same "5 → 6" prose but no output table, so there's nothing to contradict. Leave it.

**C3 and C10: replace the note at Python L197–198.**

> [!NOTE]
> This example uses only 100 rows, well below the size where Deephaven splits a formula across cores, so it wouldn't show the race from the broken version above even without `with_serial`. Use `with_serial` whenever a formula depends on shared state or row order, regardless of table size: a formula that isn't running on several cores at once still isn't guaranteed to run in order, and `with_serial` makes its rows run one at a time, in order. It only protects a formula from itself, though. If two columns share the same state, they also need a [barrier](../../conceptual/query-engine/parallelization.md#barriers) so one column finishes before the other starts.

- Groovy L162 gets the same text with `withSerial`. The `#barriers` heading exists in both concept guides at `f2ef483084`.
- The text adds no property name. It drops "the only thing", which wasn't true.

**C3 sweep: takeaway at Python L206 and Groovy L170.**

> - Use `with_serial` to force a formula to run sequentially. If several columns share state, add barriers too.

**C9: takeaway at Python L204 and Groovy L168.**

> - Deephaven assumes formulas are safe to run in parallel by default — this is fast but requires stateless code.

This matches the concept guide's own takeaway ("Deephaven assumes all formulas can run in parallel by default") and `QueryTable.statelessSelectByDefault=true`.

**Optional tidy-up after C3.** L210 ("For more depth — including barriers and other concurrency-control tools — see …") now points at barriers a second time. It could become "For more depth, see [query parallelization](…)."

## Draft replies

**C1:** Good catch, thanks. The block is tagged `syntax`, so CI never ran it. I added `from deephaven import empty_table` so the copied code runs.

**C2:** Agreed, the prose and the table didn't match. The prose now describes two cores both reading 4 and both returning 5, which lines up with the duplicate 5s in the table.

**C3:** Agreed, this overstated what `with_serial` does. Per `ConcurrencyControl.withSerial`, it only stops the expression running concurrently with itself, and with the default `serialSelectImplicitBarriers=false`, nothing orders separate serial columns. I rewrote the note to say `with_serial` protects a formula from itself and that columns sharing state also need a barrier, with a link to the barriers section of the concept guide. I made the same fix in the key takeaway and in the Groovy page. I left the property name out on purpose: this is a Crash Course chapter, and the concept guide covers the configuration.

**C4:** I'll keep the title for this PR. The Crash Course titles and sidebar labels all use title case ("Create Your First Tables", "Basic Table Operations", "Time and Calendars", …), so changing just this one would make the chapter the odd one out. Switching the whole Crash Course to sentence case is a reasonable follow-up, and I've raised it with the docs team.

**C5:** The property and default are correct, thanks. This is a Crash Course chapter, though, and we keep configuration names out of its narrative. The sentence already says the pool is sized to CPU cores "by default" and that concurrency depends on it. `PeriodicUpdateGraph.updateThreads` is documented in the concept guide's "Query phases and thread pools" section, which the page links to. No change.

**C6:** Correct. The note already says it assumes the default operation-initialization pool and that a different pool size changes the chunk count. Naming `OperationInitializationThreadPool.threads` here would be configuration injection in a tutorial, and the property is covered in the concept guide's thread-pool section. No change.

**C7:** All of this is right; I checked `QueryTable.minimumParallelSelectRows` and `SelectColumnLayer`. At Crash Course level, "at least a few million rows" is accurate. The exact threshold, the added+modified rule and the Table/RowSet exception belong in the configuration reference ("Parallel processing with `select`" in `query-table-configuration.md`) rather than here. No change.

**C8:** I'm going to keep 100 rows, and the note right below the example already says it's too small to race. A 4.2M+ row example has three problems. On the standard GIL-enabled Python build, Python-backed formulas are never parallelized, so it still wouldn't race for most readers. It adds millions of Python calls to the docs build. And a race is nondeterministic, so the output can't be snapshot-tested. The example is there to show the `with_serial` syntax.

**C9:** Fair point: the page itself says small tables and GIL-build Python formulas don't run in parallel, so "runs formulas in parallel by default" contradicted it. Rather than listing the engine's conditions in a takeaway, I reworded it to what's true at this level: Deephaven *assumes* formulas are safe to run in parallel by default, which is `statelessSelectByDefault=true`. The size and threading conditions stay in the concept guide. Applied to the Groovy page too.

**C10:** The clause is backed by the engine. The concept guide notes that a column that isn't parallelized can still be evaluated out of order, and in `SelectColumnLayer`'s serial path, modified rows are processed before added rows. So I kept the idea and, as part of the C3 rewrite, stated it plainly: a formula that isn't running on several cores still isn't guaranteed to run in order.

## Author queries

- **AQ1 [The fix, note; also L177].** For an update that has both modified and added rows, `SelectColumnLayer`'s serial path applies modified rows before added ones. The `withSerial` javadoc promises "row set order", and the page repeats that as "in row-set order". Does that promise hold across a whole update on a live table, or only within each set? It's a question for an engine SME. It doesn't block this PR, because the page's examples are static.
- **AQ2 [title].** Should the Crash Course titles and sidebar labels move to sentence case as a set, in a separate PR, so this chapter doesn't become the odd one out?
- **AQ3 [broken counter, both languages].** The broken example is 100 rows in Python but `emptyTable(5_000_000)` in Groovy, and the Groovy note wording differs to match. Which one is intended? The two versions should agree.

## Section-level notes

- **Pile-ups from earlier rounds.** Several comments land on "Across rows" (C6, C7) and on the counter and fix sections (C1, C2, C3, C8, C10), and three separate "this example uses 100 rows" callouts have built up (L55, L171, L198). Earlier rounds also put configuration straight into the narrative at L158 and L171 ("the default `QueryTable.minimumParallelSelectRows` (about 4.2 million rows)", "free-threaded Python builds"). None of this round's comments flag them, but they break the tutorial's level of detail and repeat L57. Rather than patching further, I'd run `deephaven-doc-structure-review` on those sections, collapse the 100-row caveats into one, and drop the property name from the narrative.
- **Conflict signal.** C8 (make the example big enough to parallelize) pulls against the earlier rounds' decision to keep examples small for the docs build. Neither literal fix helps the reader. The note that already explains the gap does.
- **Out-of-PR follow-up.** The `ConcurrencyControl.withSerial` javadoc says `serialSelectImplicitBarriers` defaults "to the value of `QueryTable.statelessSelectByDefault`". The code in `QueryTable.java:400-401` uses the inverse, `!STATELESS_SELECT_BY_DEFAULT`, which gives false by default. Copilot's value in C3 is the correct one, and the javadoc should be fixed separately.
