Skill used: deephaven-docs-address-review-comments (plus ref-deephaven-doc-categories, deephaven-core-accuracy-spot-check, and the placement rules in deephaven-core-accuracy-check and deephaven-writing-style that it references)

## Page purpose

This is a **Tutorial** page (Crash Course). After reading it, a first-time user should know three things: Deephaven parallelizes on its own, stateful or order-dependent formulas break under parallelism, and `with_serial` fixes that. It works at the level of ideas, so property names, thresholds and exact scheduling conditions belong on the linked concept guide, not here.

This is **round 7**, so the late-round bar applies: I only recommend changes for things that are actually wrong. Everything else is redirect, decline or ask.

## Triage

| # | Location | Comment (short) | Premise | Decision | Reason | Proposed text / destination |
|---|---|---|---|---|---|---|
| C1 | L155 | Broken-counter snippet is missing `from deephaven import empty_table` | True | **Apply** | Code a reader copies raises `NameError`. Python only; Groovy's `emptyTable` needs no import. | Add the import at the top of the block |
| C2 | L173 | Prose says "both return 6", but the table has no 6 | True: the table shows duplicate 2s and 5s, and 3 and 6 missing | **Apply** | The prose and the table disagree. Fix the prose so it describes the table without claiming one exact interleaving. | See Proposed edits |
| C3 | L198 | `with_serial` alone isn't enough when two columns share state; barriers are needed | True (`ConcurrencyControl.withSerial` Javadoc; `SERIAL_SELECT_IMPLICIT_BARRIERS` defaults to `!statelessSelectByDefault`, which is `false`) | **Redirect** | The tutorial covers one formula. Multi-column coordination is covered in the concept guide's Barriers section, which L210 already links ("including barriers and other concurrency-control tools"). The overclaim in the note ("the only thing that guarantees…") goes away with the C10 rewrite. | Destination: `conceptual/query-engine/parallelization.md#barriers` (already linked at L210). No property name in the narrative. |
| C4 | L2 | Title should be sentence case | True as a style rule, but every Crash Course title in both languages is Title Case ("Create Your First Tables", "Basic Table Operations", "Wrapping Up") | **Ask** | Changing only this page makes the Crash Course inconsistent. It's a series-wide decision. | AQ1 |
| C5 | L40 | Name `PeriodicUpdateGraph.updateThreads` and its default | True (default `-1`, which resolves to `availableProcessors()` in `PeriodicUpdateGraph`) | **Decline** | "sized to your CPU cores by default" is already correct at tutorial level. The property and default are in the concept guide's "Query phases and thread pools" section and in `query-table-configuration.md`. Adding them here is configuration injection. | None (already covered in `conceptual/query-engine/parallelization.md#query-phases-and-thread-pools`) |
| C6 | L55 | Explain the chunk count in terms of `OperationInitializationThreadPool.threads` | True but already handled. The note already says "assumes the default operation-initialization thread pool… a differently-sized pool changes the chunk count." | **Decline** | The note already covers this, and adding the property name makes an already overloaded note worse (see Section-level notes). The worked numbers are correct: `divisionSize = max(minimumParallelSelectRows, ceil(n/threads))` gives max(4.19M, 5M) = 5M, so 4 chunks. | None; optional trim in Section-level notes |
| C7 | L57 | Give the exact threshold, `>=` on added+modified rows, and the Table/RowSet exception | True (`SelectColumnLayer`: `totalSize = added + modified`, `>= MINIMUM_PARALLEL_SELECT_ROWS`, Table/RowSet results ignore the minimum) | **Redirect** (small wording fix) | The exact value and the Table/RowSet case are reference detail. But "once a table is large enough" is slightly wrong for live tables, where the size of each update counts, not the table size. Fix that at the same level of detail, with no property name. | See Proposed edits. Exact detail: `query-table-configuration.md#parallel-processing-with-select` |
| C8 | L180 | Make the fix example larger than 4,194,304 rows so it actually runs in parallel | True that 100 rows is below the threshold | **Decline** | The L198 note already says this was a deliberate choice from earlier rounds. A 4M+-row Python callback slows the docs build. On a standard (GIL) Python build, Python-backed formulas never parallelize anyway (`FormulaColumnPython.isParallelizable()` returns `isPythonFreeThreaded()`), so a bigger table wouldn't show the race or the fix for most readers. Also, a correct result can't prove the race is gone. | None |
| C9 | L204 | "runs formulas in parallel by default" is too absolute | True. Row splitting also needs: update ≥ threshold, `threadCount() > 1`, no shifts, stateless, parallelizable (Python only on free-threaded builds). | **Apply at lower resolution** | The takeaway contradicts the page's own notes, since none of the page's examples run in parallel. Don't list the conditions. Say what is true by default: the engine assumes formulas are stateless. `STATELESS_SELECT_BY_DEFAULT` defaults to `true`. | See Proposed edits. Same fix in the Groovy sibling (L168). |
| C10 | L198 | "parallelization isn't the only way execution order can vary" isn't backed up anywhere on the page | True: nothing on the page explains it | **Apply** (remove the clause) | The clause asks the reader to trust a mechanism the page never describes. The rest of that sentence ("the only thing that guarantees…") also overclaims (see C3). Rewrite the whole note more simply. Same fix in Groovy (L162). | See Proposed edits |

## Proposed edits

**C1, L144–156 (Python only).** Add the import as the first line of the `python syntax` block:

```python
from deephaven import empty_table

counter = 0
...
```

**C2, L173 (Python only).** Replace with:

> When two cores call `get_next_id` at nearly the same moment, their reads and writes of `counter` interleave. Both can return the same ID, and some IDs are never returned at all, like the duplicate 2s and 5s and the missing 3 and 6 above.

This drops the "read 5, both return 6" walk-through. With `counter += 1; return counter`, that interleaving produces a duplicate but no skip, so it can't explain the table anyway. Groovy (L141) has no table and doesn't have this mismatch, so leave it.

**C7, L57 (both languages, wording only).** Replace with:

> Deephaven only splits a single column's row-wise computation across cores when it has enough rows to process at once: a few million or more, whether that's a new table or a large batch of updates. Below that, that column's own computation runs on a single core, though independent columns and other downstream tables can still run concurrently.

No property name. The exact value stays in the concept guide and `query-table-configuration.md`.

**C9, L204 (Python) and L168 (Groovy).** Replace the first bullet with:

> - By default, Deephaven assumes your formulas are stateless, so it's free to run them in parallel.

**C10 (+ C3), L197–198 (Python) and L161–162 (Groovy).** Replace the note with:

> [!NOTE]
> This example uses only 100 rows, so it wouldn't show the race even without `with_serial`. Mark a formula with `with_serial` whenever it depends on shared state or row order, regardless of table size, instead of relying on the table being too small to parallelize.

(Use `withSerial` in Groovy.) This removes the unsupported clause (C10) and the "only thing that guarantees" overclaim (C3). Multi-column coordination stays behind the existing link at L210.

## Draft replies

**C1:** Good catch, thanks. Added `from deephaven import empty_table` to the broken-counter block so it runs as copied. The Groovy version doesn't need an import.

**C2:** Agreed, the prose and the table disagreed. I rewrote the explanation to describe what the table shows (duplicate 2s and 5s, missing 3 and 6) instead of walking through one specific interleaving.

**C3:** You're right that `with_serial` only stops a formula from running concurrently with itself, and that formulas sharing state across columns also need barriers (implicit barriers are off by default). This Crash Course page teaches the single-formula case, and multi-formula coordination is in the concept guide's Barriers section, which the page's closing paragraph already links. I've removed the "only thing that guarantees" wording so the note no longer overclaims, and left the barrier detail on the concept page.

**C4:** The style guide does call for sentence-case headings. But every Crash Course title in both languages currently uses Title Case, so changing just this one would make the series inconsistent. I'm checking whether that's intentional; if not, we'll fix all the Crash Course titles in one follow-up rather than in this PR.

**C5:** The property and default are right (`PeriodicUpdateGraph.updateThreads`, `-1` meaning all available processors). This is a Crash Course page, though, and "sized to your CPU cores by default" is accurate at that level. The property is documented in the concept guide's "Query phases and thread pools" section and in the configuration reference, which is where readers tuning it will look. Leaving the sentence as is.

**C6:** The note already covers this: it says the example assumes the default operation-initialization thread pool and that a differently sized pool changes the chunk count. The 20M-rows-on-4-cores arithmetic matches `SelectColumnLayer`'s split (four chunks of about 5M). I'd rather not add the property name to the tutorial narrative. It's in the concept guide and configuration reference.

**C7:** Thanks, this pointed out a real imprecision. "Once a table is large enough" is misleading for live tables, where the size of each update is what counts. I reworded it to "enough rows to process at once… whether that's a new table or a large batch of updates." The exact threshold, the `>=` comparison, and the Table/RowSet exception are reference-level detail, and they live in the configuration reference and the concept guide rather than in the Crash Course.

**C8:** We picked 100 rows on purpose in an earlier round, and the note under the example says so. A 4M+-row Python callback would slow the docs build noticeably. More importantly, on a standard (GIL) Python build Deephaven never parallelizes Python-backed formulas, so a bigger table still wouldn't show the race or the fix for most readers. Keeping the small example.

**C9:** Agreed that the bullet overstated it; none of the page's own examples actually run in parallel. Rather than listing every scheduling condition in a takeaway, I rewrote it to say what's always true by default: Deephaven assumes formulas are stateless, so it's free to parallelize them. The detailed conditions are in the concept guide. I made the same change on the Groovy page.

**C10:** Fair. Nothing on the page backs that clause up. I removed it and rewrote the note to simply say "use `with_serial` whenever the formula depends on shared state or order, regardless of table size." Same change on the Groovy page.

## Author queries

AQ1 [front matter, L2, both languages]: Is Title Case for Crash Course titles a deliberate series convention? Every Crash Course page uses it, while how-to and concept titles use sentence case. If it isn't deliberate, should all Crash Course titles move to sentence case in a separate PR?

AQ2 [Groovy "Example: a broken counter", L138/L144]: The Groovy broken example uses `emptyTable(5_000_000)` and says it crosses the threshold, while Python uses 100 rows with a note. Is that difference intentional? (Groovy closures can parallelize without free-threading, so it's defensible.) Or should the two pages align in a later pass?

## Section-level notes

- **Pile-up on "The fix" note (L196–198):** C3, C8 and C10 all land on one note that earlier rounds had already filled with caveats. The rewrite above resolves all three at once. Don't patch them one at a time.
- **Pile-up on "Across rows" (L54–57):** C6 and C7 are asking for more configuration detail in a note that has already accumulated caveats: the thread-pool assumption, the "differently-sized pool" clause, and "rarely a perfectly linear 4x." I'd consider trimming it to: "This example uses 100 rows for clarity, far below the size where Deephaven would split it. With 20 million rows on a 4-core machine, Deephaven would split each column's work into four chunks of about 5 million rows and compute them at the same time." Source confirms that's true with default settings.
- **Configuration injection left over from earlier rounds:** L158 and L171 (Python) and L144 (Groovy) name `QueryTable.minimumParallelSelectRows` / "about 4.2 million rows" in the tutorial narrative, and the number is repeated twice in Python. None of the round-7 comments flag it, but it's the same problem C5/C6/C7 would add more of. I recommend replacing it with "a table of several million rows" and keeping the property on the concept page. If you'd rather take a whole pass over the page than patch it, run `deephaven-doc-structure-review` on "Across rows" through "The fix."
- **Cross-language:** Apply C9 and C10 to the Groovy sibling (`docs/groovy/getting-started/crash-course/parallelization.md`, L168 and L162). C1 and C2 are Python-only. Don't copy the declined or redirected suggestions to Groovy.
- **Verified:** L177's description of `with_serial` ("never running concurrently with itself, with rows evaluated one at a time in row-set order") matches the `ConcurrencyControl.withSerial` Javadoc. The `Selectable.md#with_serial` anchor exists at f2ef483084.

Nothing was edited.