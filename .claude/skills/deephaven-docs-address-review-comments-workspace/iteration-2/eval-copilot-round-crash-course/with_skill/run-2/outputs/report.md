Skills used: `deephaven-docs-address-review-comments`, plus the skills it points to: `ref-deephaven-doc-categories`, `deephaven-core-accuracy-check`, `deephaven-core-accuracy-spot-check` and `deephaven-writing-style`.

Of the 10 comments, take three (C1, C2, C3), ask about one (C4) and decline six (C5–C10). Nothing has been edited. The file you gave me matches `docs/python/getting-started/crash-course/parallelization.md` at `f2ef483084` exactly. I checked every comment against the source code in the repo.

## Page purpose
This page is a Crash Course chapter, so it's a Tutorial. After reading it, a first-time user should know that Deephaven parallelizes formulas on its own, be able to spot a formula that uses shared state or depends on row order, and know to use `with_serial` for it, with a link onward to the concept guide for barriers and tuning. It explains concepts in plain terms. Property names, defaults and threshold conditions belong on the linked concept and configuration pages, not in this page's running text.

This is round 7 of review, so the late-round bar applies: take only what is flatly wrong.

## Triage

| # | Location | Comment (short) | Premise | Decision | Reason | Proposed text / destination |
|---|---|---|---|---|---|---|
| C1 | L155 | Broken-counter snippet is missing `from deephaven import empty_table` | true | **Apply** | Copying the snippet raises `NameError`. Code that doesn't run is flatly wrong. | Add the import at the top of the block. |
| C2 | L173 | Prose says "both return 6", but the table has no 6 | true | **Apply** | The table shows 2 and 5 twice, with 3 and 6 missing. The prose contradicts the example right above it. | Change the prose to match the table. Don't touch the table. |
| C3 | L198 | "Use `with_serial` any time … shared state" overstates what it guarantees | true | **Apply** | This is an overstated prescription. The `ConcurrencyControl.withSerial` javadoc only promises "The expression will never be invoked concurrently with itself," and with `SERIAL_SELECT_IMPLICIT_BARRIERS` false (the default) "no additional ordering between selectable expressions is imposed … use barriers." | One plain sentence plus a link to the concept guide's `#barriers` section. No property name. Same fix in the Groovy sibling. |
| C4 | L2 | Title should be sentence case | true (style rule) | **Ask** | The style guide requires sentence case in headings. But all 14 existing Crash Course titles and sidebar labels use title case ("Create Your First Tables", "Time and Calendars", …), and the sidebar entry for this page is "Query Parallelization". Changing only this page breaks the series. | AQ1 |
| C5 | L40 | Name `PeriodicUpdateGraph.updateThreads` and its default | true | **Decline** | The source confirms it (default `-1`, which resolves to `availableProcessors()`; `updateThreads=1` uses the serial `QueueNotificationProcessor`). But the sentence is already correct: "sized to your CPU cores by default" and "depends on the update graph's thread pool" already cover the one-thread case. A property name in Tutorial narrative is configuration injection. | Already covered in the concept guide's "Query phases and thread pools" section, which the page's closing link leads to. |
| C6 | L55 | Qualify the chunk count in terms of `OperationInitializationThreadPool.threads` | true but already handled | **Decline** | The note already says "This assumes the default operation-initialization thread pool, which uses one thread per core; a differently-sized pool changes the chunk count." | None |
| C7 | L57 | Give the exact threshold, `>=`, "added plus modified", and the Table/RowSet exception | true | **Decline** | The source confirms each part (`SelectColumnLayer`: `totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS`, default `1L << 22`; Table/RowSet results split when `totalSize > 0`). But 4,194,304 is "a few million", so the sentence is correct at the page's level. The per-update and Table/RowSet details serve edge cases this page isn't about. | The exact figure is in `conceptual/query-table-configuration.md`, section "Parallel processing with `select`". |
| C8 | L180 | Make the `with_serial` example larger than 4,194,304 rows | true (100 rows doesn't need `with_serial`) | **Decline** | The note directly under the example already says so. The example was deliberately shrunk in round 94a2e9e467 to avoid the build cost. Also, a bigger table wouldn't show the race on a standard Python build: `DhFormulaColumn.isParallelizable()` returns `!usesPython \|\| PythonFreeThreadUtil.isPythonFreeThreaded()`. | None |
| C9 | L204 | Qualify "runs formulas in parallel by default" with four conditions | true | **Decline** | A Key takeaways bullet is the wrong place for four conditions. The page body already covers the size condition. The bullet is a fair summary of the default (`QueryTable.statelessSelectByDefault=true`: "Stateless formulas are allowed to be processed in parallel"). | Optional lower-resolution reword is below. It adds no conditions. |
| C10 | L198 | "parallelization isn't the only way execution order can vary" is unsupported, so remove it | false | **Decline** | The source supports the clause. The `SelectColumn.isParallelizable()` javadoc says "Even if a column is not parallelizable, the engine may choose to evaluate it out-of-order and may not evaluate all rows individually." A true statement stays. At most it gets a plain-language reason. | The C3 rewrite includes that reason. |

## Proposed edits

**C1 (L144–156).** Add the import to the broken-counter block:

```python syntax
from deephaven import empty_table

counter = 0
...
```

Groovy sibling: nothing to do, because `emptyTable` is built into the Groovy session.

**C2 (L173).** Change the prose to match the table:

> Two cores might simultaneously read `counter = 4`, both add 1 to get 5, and both return 5. The result: duplicate IDs and skipped numbers.

Groovy sibling: nothing to do. It has no output table, so its "counter = 5 … both return 6" prose doesn't contradict anything.

**C3 (and the reason for C10), L197–198.** Take the prescription out of the NOTE and make it body text, so the note covers only the example's size:

> [!NOTE]
> This example uses only 100 rows, well below the threshold where Deephaven would actually parallelize it, so it wouldn't show the race from the broken version above even without `with_serial`.

Use `with_serial` whenever a formula depends on shared state or row order, regardless of table size: even on a single core, Deephaven doesn't promise to evaluate an ordinary formula's rows in order. `with_serial` protects one formula. If two or more columns share the same state, such as a single counter, they also need [barriers](../../conceptual/query-engine/parallelization.md#barriers).

- **Checked against source:** the `withSerial` javadoc and the `isParallelizable` javadoc quoted above.
- **Link target:** the `### Barriers` heading exists in `docs/python/conceptual/query-engine/parallelization.md` at `f2ef483084`, and that section has an "Example: extending the counter with a barrier" subsection.
- **Removed sentence:** this drops "`with_serial` is the only thing that guarantees…". That absolute sits awkwardly right before a barriers pointer, and the new first sentence says the same thing more safely.
- **Groovy sibling:** apply the same change at L161–162, using `withSerial`.
- **Duplicate sweep:** "Use `with_serial` to force sequential execution when your formula needs it" (Key takeaways, L206) is a per-formula statement, and `withSerial` does cover a single formula. L210 already points to barriers. No change needed there.

**C9 (optional, only if you want to soften the absolute; not required).** Reword L204 and Groovy L168:

> By default, Deephaven treats formulas as safe to run in parallel — this is fast, but it requires stateless code.

## Draft replies

**C1:** Good catch, thanks. The snippet now imports `empty_table`, so it runs if copied. (The Groovy sibling uses the built-in `emptyTable`, so it didn't need the fix.)

**C2:** Agreed, the prose and the table disagreed. I changed the prose to describe the duplicate 5 that the table shows, and left the illustrative table as is.

**C3:** You're right. `withSerial` only guarantees the formula is never run concurrently with itself, and with the default settings it adds no ordering between serial columns. I pulled the prescription out of the note and added one sentence saying `with_serial` protects a single formula and that columns sharing state also need barriers, with a link to the Barriers section of the parallelization guide. I didn't name `serialSelectImplicitBarriers` here, since this is a Crash Course chapter. Same fix in the Groovy page.

**C4:** The style guide does call for sentence case. The catch is that every other Crash Course chapter and its sidebar label is currently in title case, so changing only this one would make it the odd one out in the sequence. I've asked whether to sentence-case the whole Crash Course in a follow-up; I'll leave this title matching its siblings until then.

**C5:** The property and default are correct, thanks for confirming. I'm leaving them out of this sentence on purpose. This is a Crash Course chapter, and the sentence is already accurate at that level: the pool is sized to your cores by default, and actual concurrency depends on it, which covers `updateThreads=1`. `PeriodicUpdateGraph.updateThreads` and its default are covered in the parallelization guide's "Query phases and thread pools" section, which this page links to at the end.

**C6:** The note already covers this: "This assumes the default operation-initialization thread pool, which uses one thread per core; a differently-sized pool changes the chunk count." I'd rather not add the property name to a tutorial note that has already built up several caveats.

**C7:** All accurate, including the `>=` comparison against rows added plus modified and the Table/RowSet exception. For a Crash Course reader, "large enough (at least a few million rows)" is correct at the level the page works at, and 4,194,304 is a few million. The exact property and default are in the configuration reference under "Parallel processing with `select`". The per-update and Table/RowSet details are edge cases I'd keep out of the tutorial narrative.

**C8:** I'm keeping the 100-row example. It was shrunk in an earlier round to avoid the build cost, and the note right under it already says it wouldn't race without `with_serial`. Also, a larger table wouldn't show the race here: a Python-backed formula is only parallelizable on a free-threaded build (`DhFormulaColumn.isParallelizable`), so on the standard build the bigger example would still come out correct. The example's job is to show the syntax, and the broken example above it shows the failure.

**C9:** The conditions are real, but a Key takeaways bullet is the wrong place for four of them. The page body already explains the size threshold, and the configuration details live in the linked guide. The bullet summarizes the default behavior, which the source supports: `statelessSelectByDefault` is `true`, meaning formulas are treated as stateless and allowed to run in parallel. (If the absolute still reads too strongly, I can reword it to "treats formulas as safe to run in parallel" without adding conditions.)

**C10:** The clause is supported by the engine contract. `SelectColumn.isParallelizable()` says that even when a column isn't parallelizable, "the engine may choose to evaluate it out-of-order." So I've kept it, and the revised text now gives the plain-language reason: Deephaven doesn't promise in-order evaluation for an ordinary formula even on a single core.

## Author queries

**AQ1 [front matter, title]:** Should the Crash Course move to sentence-case titles and sidebar labels? If yes, do all 14 chapters (both languages, plus `sidebar.json`) in one follow-up PR. If no, keep "Query Parallelization" to match the series. My recommendation is to keep it for this PR and open a ticket for the series-wide change.

## Section-level notes

- **The comments pile up in one place.** C1, C2, C3, C8 and C10 all land on "When it breaks" through "The fix" (L133–200). That stretch now explains the example size or the threshold three times: the L158 paragraph (which names `QueryTable.minimumParallelSelectRows` in the narrative, carried over from earlier rounds), the L171 note, and the L198 note. That's the configuration injection this page should avoid. Instead of patching it further, I'd run `deephaven-doc-structure-review` on those sections. The likely result is one short statement that the examples are small for clarity, with the property name gone from L158.
- **Python and Groovy diverge.** The Python broken example uses `empty_table(100)` plus a note. The Groovy one uses `emptyTable(5_000_000)` with a different note. Both are `syntax` blocks, so the build cost doesn't apply to either. Pick one approach for both pages when you do the structural pass.
- **The requests conflict with each other and with past rounds.** C8 asks to grow the example, while round 94a2e9e467 shrank the neighboring "Across rows" example for build cost. C5, C6, C7 and C9 all push more configuration detail into a Tutorial after six earlier rounds. That pattern is the reason they're declined.
- **Your queue for the Groovy page:** only the C3 fix, at L161–162. C1 and C2 don't apply there.

Everything is at `/Users/margaretkennedy/dhc-skills-chip` (`git show f2ef483084:<path>` for the reviewed version):

- The page: `docs/python/getting-started/crash-course/parallelization.md` and `docs/groovy/getting-started/crash-course/parallelization.md`
- The concept guide sections I cite: `docs/python/conceptual/query-engine/parallelization.md` (`#barriers`, `#query-phases-and-thread-pools`)
- The configuration reference: `docs/python/conceptual/query-table-configuration.md`
- Source I verified against:
  - `table-api/src/main/java/io/deephaven/api/ConcurrencyControl.java`
  - `engine/table/src/main/java/io/deephaven/engine/table/impl/QueryTable.java`
  - `engine/table/src/main/java/io/deephaven/engine/table/impl/select/analyzers/SelectColumnLayer.java`
  - `engine/table/src/main/java/io/deephaven/engine/table/impl/select/SelectColumn.java`
  - `engine/table/src/main/java/io/deephaven/engine/table/impl/select/DhFormulaColumn.java`
  - `engine/table/src/main/java/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.java`