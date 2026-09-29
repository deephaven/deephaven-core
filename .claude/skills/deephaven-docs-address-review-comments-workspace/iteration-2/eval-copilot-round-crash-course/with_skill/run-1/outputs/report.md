I used the deephaven-docs-address-review-comments skill, which pulls in ref-deephaven-doc-categories, deephaven-core-accuracy-spot-check, deephaven-core-accuracy-check and deephaven-writing-style.

# Copilot round 7 on the Crash Course parallelization page

**Short version:** take C1, C2, C3 and C9, point C5, C6 and C7 to the reference pages instead of adding the detail inline, decline C8 and C10, and decide C4 yourself (I'd leave it for a separate PR). All ten comments' facts check out against source except C10's (C6 and C8 are true but already handled or not the right fix). The trouble is that most of them ask for detail that doesn't belong in a Crash Course chapter.

I treated the page as `docs/python/getting-started/crash-course/parallelization.md` at `f2ef483084`; the eval file matches that commit exactly. Links and anchors were checked in that tree, and engine claims against the current source.

## What the page is for
This is a Tutorial (Crash Course) page. After reading it, a first-time user should know three things: Deephaven parallelizes automatically, formulas must be stateless (each row computed on its own) for that to be safe, and `with_serial` is the fix when they aren't. It works at a plain level with no configuration. This is round 7, so the late-round rule applies: only take what is flatly wrong.

## Triage

| # | Location | Comment (short) | Premise | Decision | Reason | Proposed text / destination |
|---|---|---|---|---|---|---|
| C1 | L155 | Broken-counter snippet is missing the `empty_table` import | true | **Apply** | Copied code fails with `NameError`. The block is tagged `syntax`, so the docs build never ran it. | Add `from deephaven import empty_table` |
| C2 | L173 | "Both return 6" but the table has no 6 | true (table: 1,2,2,4,5,5,7) | **Apply** | The text contradicts its own table | Rewrite the text to match the table (below) |
| C3 | L198 | "Use `with_serial` any time…shared state" promises too much | true | **Apply** | It prescribes a fix that isn't enough in every case. This kind of error gets fixed even in late rounds, in one plain sentence plus a link. | Add a sentence that columns sharing state also need barriers, linked to `#barriers` |
| C4 | L2 | Title should be sentence case | true against the style rule, but conflicts with the series | **Ask** | All 15 Python and 13 Groovy Crash Course titles use Title Case ("Create Your First Tables", "Basic Table Operations"…). Changing only this one breaks the series. | AQ1 |
| C5 | L40 | Name `PeriodicUpdateGraph.updateThreads` and its default | true | **Redirect** | The sentence is already correct at its level ("sized to your CPU cores by default"). Property names don't go in Tutorial text. | Link to the thread-pool section from the closing sentence |
| C6 | L55 | Describe chunk count in terms of `OperationInitializationThreadPool.threads` | true but already handled | **Decline** | The note already says it assumes the default initialization pool and that a different pool size changes the chunk count | — |
| C7 | L57 | State the exact threshold, `>=`, "added+modified", and the Table/RowSet exception | true | **Redirect** | 4,194,304 is "a few million". The exact figures belong on the configuration reference page. | Link to `query-table-configuration.md#parallel-processing-with-select`; optional wording tweak below |
| C8 | L180 | Make the fixed example larger than 4,194,304 rows | true (100 rows never parallelizes) but not the right fix | **Decline** | This formula calls a Python function, so on standard (non-free-threaded) Python it never parallelizes at any size. A 4M+ row table would demonstrate nothing for most readers and would slow the docs build. The note already says this. | — |
| C9 | L204 | "Runs formulas in parallel by default" is too absolute | true | **Apply** (with a different fix) | The bullet contradicts the page's own L171 ("evaluated serially by default"). Fix it by saying less, not by adding the four conditions Copilot lists. | "Treats formulas as stateless by default, so it can run them in parallel" |
| C10 | L198 | "Parallelization isn't the only way execution order can vary" is unsupported | false (the point is supported) | **Decline** the removal and reword in the C3 edit | The source supports the point. The ordering promise ("Rows are evaluated sequentially in row set order") is part of the serial contract only; without `with_serial` the engine promises no order. The current wording just hints at an unnamed mechanism, so the C3 rewrite states the real reason. | Covered by the C3 edit |

### Evidence
- **C3:** In `ConcurrencyControl.withSerial` (`table-api/.../ConcurrencyControl.java`, lines 32–50), the contract is "The expression will never be invoked concurrently with itself" and "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed… To impose further ordering constraints, use barriers." The default is `!STATELESS_SELECT_BY_DEFAULT`, which is false (`QueryTable.java`, lines 400–402).
- **C5:** In `PeriodicUpdateGraph.java`, `updateThreads` defaults to -1 (line 55), and a value of 0 or less means `Runtime.getRuntime().availableProcessors()` (lines 140–141).
- **C6 / C7:** In `SelectColumnLayer.java` (lines 203–208), work is split when `canParallelizeThisColumn && jobScheduler.threadCount() > 1 && !hasShifts && ((resultTypeIsTableOrRowSet && totalSize > 0) || totalSize >= MINIMUM_PARALLEL_SELECT_ROWS)`. Here `totalSize = added + modified`, and `divisionSize = max(MIN_ROWS, ceil(total/threads))`. With 20M rows and 4 threads that gives 4 chunks of 5M, so the L55 example is correct. `MINIMUM_PARALLEL_SELECT_ROWS` defaults to `1L << 22` (`QueryTable.java`, line 337).
- **C8 / C9:** In `DhFormulaColumn.isParallelizable()` (lines 899–905) and `FormulaColumnPython` (lines 63–65), any formula that uses Python can only run in parallel on free-threaded Python. `isStateless()` is still true by default (`STATELESS_SELECT_BY_DEFAULT = true`).

## Proposed edits

**C1** (L144–156). Add the import as the first line of the block:
```python syntax
from deephaven import empty_table

counter = 0
...
```

**C2** (L173). Replace with the text below, and move it up directly under the table, above the note, so the explanation sits next to what it explains:
> Two cores might simultaneously read `counter = 4`, both add 1 to get 5, and both return 5. The result: duplicate IDs and skipped numbers, like the repeated 5 and missing 6 above.

**C3 + C10** (L197–198). Replace the note and add one paragraph after it, before **Trade-off**:
> [!NOTE]
> This example uses only 100 rows, well below the threshold where Deephaven would actually parallelize it, so it wouldn't show the race from the broken version above even without `with_serial`. Don't rely on that: use `with_serial` any time your formula depends on shared state or row order, regardless of table size. It's the only thing that guarantees a formula's rows are evaluated one at a time, in order — without it, Deephaven makes no promise about evaluation order.

> `with_serial` protects one formula from itself. If two or more columns share the same state — for example, both call `get_next_id` — `with_serial` on each isn't enough, because Deephaven can still run the two columns at the same time. They also need [barriers](../../conceptual/query-engine/parallelization.md#barriers) to control which column goes first.

The `### Barriers` heading exists at L236 of the concept guide at that commit. In Groovy, use `withSerial` and "both increment `counter`".

**C9** (L204):
> - Deephaven treats formulas as stateless by default, so it can run them in parallel — this is fast, but it means your code must actually be stateless.

**C5 / C6 / C7** (redirect). Replace L210, dropping "barriers" here because the C3 paragraph now links to them:
> For more depth — including the other concurrency-control tools and the [thread pools](../../conceptual/query-engine/parallelization.md#query-phases-and-thread-pools) that run parallel work — see [query parallelization](../../conceptual/query-engine/parallelization.md). The row threshold for splitting a formula across cores is listed in [Query table configuration](../../conceptual/query-table-configuration.md#parallel-processing-with-select).

Both anchors exist at that commit in Python and Groovy.

*Optional (C7):* L57 "once a table is large enough (at least a few million rows)" could become "once there are enough rows to process at once (at least a few million)". That stays true for live updates, where only added and modified rows count, and adds no property name.

**Groovy sibling** (`docs/groovy/getting-started/crash-course/parallelization.md`): the C3 note (L162) and the C9 bullet (L168) have the identical text and need the same fixes. C1 and C2 don't apply there: `emptyTable` needs no import, and the Groovy page has no output table.

## Draft replies

- **C1:** Good catch, thanks. Added `from deephaven import empty_table`; the block is `syntax`-tagged, so the build never caught it.
- **C2:** Agreed, the prose and table disagreed. Reworded it to match the table (two 5s, no 6) and moved it directly under the table.
- **C3:** Agreed, that promised too much. `with_serial` only guarantees a formula never runs concurrently with itself. I added one sentence saying columns that share state also need barriers, with a link to the Barriers section of the concept guide. The details of implicit barriers stay there rather than in the Crash Course.
- **C4:** The style rule does call for sentence case, but every other Crash Course chapter title in both languages uses Title Case, so changing only this one would make the series inconsistent. I'd rather handle all of them in a separate sweep; leaving it as is for this PR.
- **C5:** The sentence is accurate at this level ("sized to your CPU cores by default"). In the Crash Course we keep property names out of the text, so I added a link to the thread-pool section of the concept guide, where `PeriodicUpdateGraph.updateThreads` and its default are documented.
- **C6:** The note already covers this. It says the example assumes the default operation-initialization pool and that a different pool size changes the chunk count. Leaving it as is.
- **C7:** The default threshold (4,194,304) is "a few million", and this chapter deliberately avoids exact thresholds. I linked the end of the page to the `minimumParallelSelectRows` entry in Query table configuration, which has the exact value and conditions.
- **C8:** I'd rather keep 100 rows. This formula calls a Python function, which only runs in parallel on free-threaded Python builds, so a 4M+ row table wouldn't show the race for most readers and would slow the docs build. The note already says the small example doesn't show the race and why `with_serial` still matters.
- **C9:** Agreed, it contradicted the note above ("evaluated serially by default"). Rather than listing every condition, I reworded it to what's always true: formulas are treated as stateless by default, so Deephaven *can* run them in parallel.
- **C10:** The underlying point is supported: only the `with_serial` contract promises row-set order, and without it the engine promises no ordering. I agree the wording hinted at a mechanism the page never names, so the reworked note now says that directly instead of removing it.

## Author queries
- **AQ1 [title, L2]:** Should all Crash Course titles in both languages move to sentence case in a separate PR, or stay Title Case as a series convention?
- **AQ2 [Example: a broken counter]:** Groovy's broken example uses `emptyTable(5_000_000)` and Python's uses 100 rows plus a note. Is that difference intentional? It's defensible for Python (no parallel Python formulas without free threading), but confirm it was an earlier-round decision.

## Section-level notes
- **Comments are piling up in two places.** C1, C2, C3, C8 and C10 all land on "When it breaks" and "The fix". C5, C6 and C7 all land on "How parallelization works". Earlier rounds have already left three "this example uses 100 rows" disclaimers (L55, L171, L198). They also put `QueryTable.minimumParallelSelectRows (about 4.2 million rows)` into the Tutorial text twice (L158 and L171 repeat each other), and the last sentence of the L55 note is also accumulated caveats. I'd run `deephaven-doc-structure-review` on both sections instead of patching further, starting with removing the property name from L158.
- **C8 would conflict with an earlier round's decision** to keep examples small. Neither enlarging the example nor adding more notes helps the reader.
- **Separate follow-up (not docs):** the `ConcurrencyControl.withSerial` javadoc says `serialSelectImplicitBarriers` defaults "to the value of" `statelessSelectByDefault`. The code (`QueryTable.java`, lines 400–402) and `QueryTable`'s own javadoc say it's the inverse, so the `ConcurrencyControl` javadoc is wrong.