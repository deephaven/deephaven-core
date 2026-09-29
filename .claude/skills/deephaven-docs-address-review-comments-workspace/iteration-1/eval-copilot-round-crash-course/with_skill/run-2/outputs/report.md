Skills used: `deephaven-docs-address-review-comments`, which pulls in `ref-deephaven-doc-categories`, `deephaven-core-accuracy-spot-check` (to check each premise), `deephaven-core-accuracy-check` (the rule on where configuration detail goes) and `deephaven-writing-style`.

## Page purpose
This is a Crash Course page, so it's a Tutorial. After reading it, a first-time user should know that Deephaven runs work in parallel on its own, that formulas must not depend on shared state or row order, and that `with_serial` is the fix when they do. It works at a plain-language level: no property names, defaults or engine conditions in the narrative.

This is round 7, so the late-round bar applies: take only what is flatly wrong.

## Triage

| # | Location | Comment (short) | Premise | Decision | Reason | Proposed text / destination |
|---|---|---|---|---|---|---|
| C1 | L155 | Broken-counter snippet is missing the `empty_table` import | True | **Apply** | A copied block fails with `NameError`. The block is tagged `python syntax`, so the docs build never runs it and never catches the error. | Add `from deephaven import empty_table` |
| C2 | L173 | Prose says "both return 6" but the table has no 6 | True | **Apply** | The prose and the table next to it disagree. | Reword the prose to describe the table (below) |
| C3 | L198 | `with_serial` isn't enough when two columns share state | True | **Redirect** | `ConcurrencyControl.withSerial`: "never be invoked concurrently with itself". With `SERIAL_SELECT_IMPLICIT_BARRIERS` false (the default, because `statelessSelectByDefault` defaults to true), "no additional ordering between selectable expressions is imposed". The L198 sentence is about a single formula ("your formula"), so it's correct as written. The two-column case belongs to barriers, which the conceptual guide covers. | One plain-language pointer in the closing line (L210) to `conceptual/query-engine/parallelization.md#barriers`. No property name. |
| C4 | L2 | Title should be sentence case | True-but-handled | **Decline** | The style rule is "Sentence case in headings". The `title:` front matter of every Crash Course chapter at this commit uses title case ("Create Your First Tables", "Basic Table Operations", "Real-time Plots", and so on), and so does the Groovy sibling. Changing only this page would make it the odd one out. | None (see AQ2) |
| C5 | L40 | Name `PeriodicUpdateGraph.updateThreads` and its default | True-but-handled | **Decline** | Source matches the comment: the default is `-1`, which becomes `availableProcessors()`. But the sentence already says concurrency "depends on the update graph's thread pool (sized to your CPU cores by default)". That is correct at tutorial level. Putting the property name in the narrative is configuration injection. | Already covered in the conceptual guide, `#query-phases-and-thread-pools` |
| C6 | L55 | Describe the chunk count in terms of `OperationInitializationThreadPool.threads` | True-but-handled | **Decline** | A static `update` does use `OperationInitializerJobScheduler` (`QueryTable.java:1828`). The note already says "This assumes the default operation-initialization thread pool… a differently-sized pool changes the chunk count." Also, the chunk size is `max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(n/threads))` (`SelectColumnLayer.java:207`), so describing it by thread count alone would still be incomplete. That shows how deep this goes. | Conceptual guide, `#query-phases-and-thread-pools` |
| C7 | L57 | Give the exact threshold, the `>=` against added+modified rows, and the Table/RowSet exception | True | **Redirect** | Source matches the comment: `1L << 22`, the `totalSize >= MINIMUM_PARALLEL_SELECT_ROWS` check on added+modified rows, and Table/RowSet results split whenever `totalSize > 0` (`SelectColumnLayer.java:203-205`). "At least a few million rows" is correct for this page. The exact value is reference material. | Link "large enough" to `conceptual/query-table-configuration.md#parallel-processing-with-select` |
| C8 | L180 | Grow the corrected example past 4,194,304 rows | True, but the fix is wrong | **Decline** | The fixed example returns the same correct output at any size, so more rows don't demonstrate anything about the fix. On a standard (GIL) Python build, Python-backed formulas are never parallelized: `DhFormulaColumn.isParallelizable` returns `!usesPython \|\| PythonFreeThreadUtil.isPythonFreeThreaded()`. So most readers wouldn't see the race even with 4.2M rows, and the docs build would make 4.2M Python calls. It also contradicts the earlier-round decision to keep examples small (the notes at L55, L171 and L198 exist for exactly that reason). | None |
| C9 | L204 | "Runs formulas in parallel by default" is too absolute | True | **Apply** (at tutorial level) | The takeaway contradicts the page itself: L57 and L171 say small tables run on one core. The right fix is to say *less*, not to list the four conditions. What is actually true by default is the stateless assumption (`QueryTable.statelessSelectByDefault`, default `true`). | Reworded takeaway (below), in both languages |
| C10 | L198 | "Parallelization isn't the only way execution order can vary" is unsupported | False | **Decline** | Source backs the clause. From the `SelectColumn.isParallelizable` javadoc: "Even if a column is not parallelizable, the engine may choose to evaluate it out-of-order and may not evaluate all rows individually." A tutorial doesn't need to explain that mechanism. | None. The note itself should be consolidated (see Section-level notes). |

## Proposed edits

**C1: L144–156.** Add the import as the first line of the block:

```python syntax
from deephaven import empty_table

counter = 0
...
```

The Groovy sibling needs no change because `emptyTable` is auto-imported there.

**C2: L173.** Replace the paragraph with:

> When two cores call `get_next_id` at the same moment, their updates to `counter` collide: both can return the same value, and a value can be skipped entirely. That's where the repeated 2s and 5s, and the missing 3 and 6, come from.

This avoids a step-by-step arithmetic trace. The current one can't produce the table's pattern, and a lost update alone gives a duplicate, not a skip. The Groovy sibling has no output table, so its prose isn't contradicted; leave it (see Section-level notes).

**C9: L204.** Replace the bullet with:

> - Deephaven assumes formulas are safe to run in parallel by default — this lets large queries run fast, but it requires stateless code.

Make the same change to the Groovy sibling's matching bullet (its L168). That's the duplicate sweep for this Apply item.

**C3 (redirect): L210.** Replace the line with:

> If more than one formula in the same `update` shares state, `with_serial` on each one isn't enough by itself — they also need [barriers](../../conceptual/query-engine/parallelization.md#barriers) to run one after another. For more depth on these and other concurrency-control tools, see [query parallelization](../../conceptual/query-engine/parallelization.md).

Mirror this in Groovy with `withSerial`. Don't mention `serialSelectImplicitBarriers` anywhere on this page; the conceptual guide's "Implicit barriers" section covers it.

**C7 (redirect): L57.** Link the existing phrase and change nothing else:

> …once a table is [large enough](../../conceptual/query-table-configuration.md#parallel-processing-with-select) (at least a few million rows).

## Draft replies

**C1:** Good catch. Because this block is tagged `syntax`, the docs build never ran it, so nothing flagged the missing import. Added `from deephaven import empty_table`.

**C2:** Agreed, the prose and the table disagreed. I rewrote the paragraph to describe the collision in general terms and point at the repeated 2s and 5s and the missing 3 and 6 in the table, rather than walking through a specific counter value.

**C3:** You're right that `with_serial` only keeps a formula from running concurrently with *itself*, and that by default there's no ordering between separate serial columns. This sentence is about a single formula, so it stays as is. I added a plain-language pointer in the closing paragraph to the barriers section of the conceptual parallelization guide, which covers the multi-column case with a worked counter example. I didn't name the property here because the Crash Course avoids configuration detail in its narrative.

**C4:** The style rule covers headings. Page titles across the whole Crash Course, and on this page's Groovy sibling, use title case, so changing only this one would make it inconsistent with its neighbors. If we want sentence-case titles, that should be one change across all the chapters. I've flagged it for the author.

**C5:** The property and default are correct, but this sentence already says what the reader needs: concurrency depends on the update graph's thread pool, which is sized to the cores by default. The Crash Course keeps property names out of the narrative. `PeriodicUpdateGraph.updateThreads` is covered in the conceptual parallelization guide under "Query phases and thread pools."

**C6:** The note already says it assumes the default operation-initialization pool and that a different pool size changes the chunk count. The exact rule also involves the minimum chunk size (`max(minimumParallelSelectRows, ceil(rows/threads))`), which is more detail than an illustrative Crash Course note should carry. The conceptual guide covers the pool and its property.

**C7:** All of that is accurate, but for a first-time reader "at least a few million rows" is the right level of detail. I linked "large enough" to the configuration reference, where the exact `minimumParallelSelectRows` value is listed. The `>=` against rows per update and the Table/RowSet exception are edge cases this tutorial doesn't need.

**C8:** Keeping the 100 rows. The corrected example gives the same correct output at any size, so more rows wouldn't show anything more about the fix. On a standard (non-free-threaded) Python build, Python-backed formulas are never parallelized, so most readers wouldn't see the race even at 4.2M rows, and the docs build would make millions of Python calls. The note under the example already explains the size choice.

**C9:** Agreed, the takeaway contradicted the page's own point that small tables run on one core. I reworded it to what is actually true by default: Deephaven *assumes* formulas are safe to run in parallel. I didn't list the conditions, since this is a summary bullet in a tutorial. Same fix in the Groovy page.

**C10:** The clause is supported by source. `SelectColumn.isParallelizable` documents that "the engine may choose to evaluate it out-of-order" even when a column isn't parallelized. The tutorial doesn't need to explain that mechanism, only why `with_serial` matters regardless of table size. Leaving it in.

## Author queries

- **AQ1 [Groovy sibling, broken counter]:** The Groovy broken example uses `emptyTable(5_000_000)` and its note says it crosses the threshold, while the Python one uses 100 rows. Is that difference intentional? A reason for it would be that Groovy closures parallelize without free-threaded Python. If so, it's fine. If not, one of them should match the other.
- **AQ2 [L2, all Crash Course titles]:** Should `title:` front matter follow the sentence-case heading rule? If yes, it's a change across the whole Crash Course, not something to do in this PR.

## Section-level notes

- **Pile-up in the broken-counter and `with_serial` sections (L140–210):** five of the ten comments (C1, C2, C3, C8, C10) land here. That signals a structural problem, not five separate patches. The same "this example is too small to actually parallelize" caveat now appears three times, at L158, L171 and L198. The L198 note has also grown a second job (when to use `with_serial` at all) that belongs in the body text. I recommend a `deephaven-doc-structure-review` pass on these sections: state the small-example caveat once, and move "use `with_serial` whenever the formula depends on shared state or order, at any size" into the paragraph introducing the fix.
- **Configuration injection left from an earlier round, not raised by any comment:** L158 names `QueryTable.minimumParallelSelectRows` inline. That's the same problem C5 and C7 would create. When you consolidate, reword it to "a table above the parallelization threshold (a few million rows)" to match L57. The Groovy note at its L144 has the same inline property.
- **L55 note:** the last sentence ("This assumes the default operation-initialization thread pool…") is caveats that built up in earlier rounds. Consider cutting it back to "With 20 million rows and 4 cores, Deephaven would divide each column's work into about four chunks," and let the conceptual guide carry the pool details.
- **Optional, Groovy L141:** the sentence "read 5, both add 1 to get 6, both return 6" then says it causes "skipped numbers." That trace is a lost update, which produces a duplicate but no skip. It's minor and nobody commented on it, but the C2 wording would work there too if you want the two siblings to match.

No edits were made. This is the triage and the draft replies only.