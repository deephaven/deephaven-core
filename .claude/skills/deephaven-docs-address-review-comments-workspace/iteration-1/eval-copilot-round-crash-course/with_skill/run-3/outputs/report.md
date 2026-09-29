Skill used: deephaven-docs-address-review-comments, plus the files it references: ref-deephaven-doc-categories, deephaven-core-accuracy-spot-check, deephaven-core-accuracy-check and deephaven-writing-style.

I checked every comment against source in `/Users/margaretkennedy/dhc-skills-chip` and edited nothing. The eval file is byte-identical to `docs/python/getting-started/crash-course/parallelization.md` at f2ef483084. This is round 7, so I only applied the late-round bar: fix what's flatly wrong, and redirect, decline or ask about the rest.

**In short:** take C1, C2, C3, C9 and C10, each rewritten at the page's own level rather than as Copilot worded it. Redirect C5 and C7. Decline C4, C6 and C8.

## Page purpose
This is a Crash Course page, so it's a Tutorial. After reading it, a first-time user should understand that Deephaven spreads work across cores automatically, see why formulas with shared state or row-order dependencies break under that, and know to reach for `with_serial`. It works at the level of a mental model: no property names, thresholds or scheduler internals in the narrative.

## Triage

| # | Location | Comment (short) | Premise | Decision | Reason | Proposed text / destination |
|---|---|---|---|---|---|---|
| C1 | L144–156 | Broken-counter snippet is missing its `empty_table` import | True | **Apply** | Code that fails when copied. The fixed version (L180) imports it. | Add `from deephaven import empty_table` |
| C2 | L173 | Prose says "both return 6" but the table has no 6 | True: the table (L160–168) has two 2s, two 5s, and no 3 or 6 | **Apply** | The prose contradicts the table right above it | Rewrite the prose to match the table (below) |
| C3 | L198 | "`with_serial` any time … shared state" overstates the guarantee | True | **Apply** (at page level) | A remedy that claims to be enough when it isn't is flatly wrong. Fix without naming the property. | Say `with_serial` alone isn't enough when several columns share state, and link to barriers |
| C4 | L2 | Make the title sentence case | Partly true: the rule covers headings; titles are a convention question | **Decline** | All 14 other Python Crash Course titles use Title Case | Leave as is; a site-wide title-case change is a separate decision |
| C5 | L40 | Name `PeriodicUpdateGraph.updateThreads` and its default | True | **Redirect** | "Sized to your CPU cores by default" is correct at this level. The property is already documented. | The closing "For more depth" line (below) |
| C6 | L55 | Qualify the chunk count with `OperationInitializationThreadPool.threads` | True, and the page already covers it | **Decline** | The note already says it assumes the default pool and that a different size changes the chunk count | None |
| C7 | L57 | Give the exact threshold, the `>=` against added+modified rows, and the Table/RowSet exception | True | **Redirect** (with a small wording fix) | "A few million" is right at this level. "Once a table is large enough" is slightly off for ticking updates. | Reword the sentence; exact values go to the config reference |
| C8 | L180–194 | Grow the fixed example past 4,194,304 rows | True that 100 rows won't parallelize, but the proposed fix doesn't work | **Decline** | On a standard (GIL) Python build a Python-backed formula never parallelizes at any size. The notes already cover the size choice. | None (see AQ1) |
| C9 | L204 | "Runs formulas in parallel by default" is too absolute | True | **Apply** (at page level) | The page's own notes (L55, L171, L198) contradict it | Reword without Copilot's list of four conditions |
| C10 | L198 | "Parallelization isn't the only way execution order can vary" is unsupported | True | **Apply** | Nothing on the page supports the clause | Remove it; merge with C3 into one rewritten note |

### Evidence
- **C1:** L155 calls `empty_table(100)` and the block (L144–156) has no import. `syntax` blocks don't run in the docs build, so only readers hit the error.
- **C3:** `table-api/.../ConcurrencyControl.java` says of `withSerial` that the expression "will never be invoked concurrently with itself." It also says that when `SERIAL_SELECT_IMPLICIT_BARRIERS` is false, "no additional ordering between selectable expressions is imposed … To impose further ordering constraints, use barriers." That flag's default is `!STATELESS_SELECT_BY_DEFAULT`, i.e. false (`QueryTable.java:400–401`).
- **C4:** Every Python Crash Course title is Title Case, e.g. "Create Your First Tables", "Basic Table Operations", "Real-time Plots". So is the Groovy sibling ("Query Parallelization"). The style rule says "Sentence case in headings."
- **C5:** `PeriodicUpdateGraph.updateThreads` defaults to `-1`, which resolves to `Runtime.getRuntime().availableProcessors()`. At 1 thread it uses `QueueNotificationProcessor`. `conceptual/query-engine/parallelization.md` already documents this under "Managing thread pool sizes."
- **C6:** The note's numbers are right. `SelectColumnLayer.java:206–208` gives `divisionSize = max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(total/threads))`. For 20M rows on 4 threads that is 5M, so 4 chunks.
- **C7:** `SelectColumnLayer.java:194, 203–205` gates on `totalSize = added + modified`, `totalSize >= MINIMUM_PARALLEL_SELECT_ROWS`, and splits `Table`/`RowSet` results whenever `totalSize > 0`. The default is `1L << 22` (`QueryTable.java:336–337`). The exact values are in `conceptual/query-table-configuration.md#parallel-processing-with-select`.
- **C8:** `DhFormulaColumn.isParallelizable()` returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`. This also conflicts with earlier rounds, which kept 100 rows and added notes explaining why.
- **C9:** Formulas only parallelize under several conditions (`SelectColumnLayer.java:115–117, 203`; `DhFormulaColumn.isParallelizable`), but the page promises nothing about those conditions and already links to the conceptual guide.
- **C10:** The remainder of the clause, "`with_serial` is the only thing that guarantees rows are processed one at a time, in order," is also an overstatement. For example, if a formula isn't parallelizable, its column is computed on one thread anyway (`SelectColumnLayer.java:115–117`).

## Proposed edits

**C1 (L144–156):** add the import as the block's first line:
```python syntax
from deephaven import empty_table

counter = 0
...
```

**C2 (L173):** replace with:
> Two cores can read `counter` at the same moment and hand out the same ID, while other IDs are skipped entirely. In the output above, 2 and 5 each appear twice, and 3 and 6 never appear.

**C3 + C10 (L198 note):** replace the note's second and third sentences, giving:
> [!NOTE]
> This example uses only 100 rows, well below the size where Deephaven would parallelize it, so the broken version above would also produce correct IDs here. Use `with_serial` whenever a formula depends on shared state or row order, regardless of table size. If more than one column shares the same state, `with_serial` alone isn't enough — you also need barriers, covered in [query parallelization](../../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where).

Check the anchor when you apply this. It is generated from the heading "Controlling Concurrency for `select`, `update` and `where`".

**C7 (L57):** reword the first sentence and leave the property out:
> Deephaven only splits a single column's row-wise computation across cores when there are enough rows to process at once (a few million). Below that, that column's own computation runs on a single core, though independent columns and other downstream tables can still run concurrently.

**C9 (L204):**
> - Deephaven assumes formulas are safe to run in parallel unless you tell it otherwise — this is fast but requires stateless code.

**Redirect destination for C5 and C7 (L210):** extend the existing closing line instead of adding pointers in the narrative:
> For more depth — including barriers, thread pool sizes, and the settings that control when work is split across cores — see [query parallelization](../../conceptual/query-engine/parallelization.md) and [query table configuration](../../conceptual/query-table-configuration.md#parallel-processing-with-select).

**Groovy sibling** (`docs/groovy/getting-started/crash-course/parallelization.md` at the same commit): make the same C3+C10 change to the note at L162 and the same C9 change to L168. C1 doesn't apply there, because the Groovy session statically imports `TableTools` (`GroovyDeephavenSession.java:283–284`). C2 doesn't apply either: the Groovy page has no output table.

## Draft replies

- **C1:** Good catch, thanks. I added `from deephaven import empty_table` so the broken snippet runs on its own, like the fixed one below it.
- **C2:** Agreed, the prose and the table disagreed. I rewrote the sentence to describe what the table shows: 2 and 5 appear twice, and 3 and 6 are missing.
- **C3:** Right. Per `ConcurrencyControl.withSerial`, it only keeps the expression from running concurrently with itself. When several columns share state, you also need barriers. The note now says `with_serial` alone isn't enough in that case and links to the barriers section of the concept guide. I left the `serialSelectImplicitBarriers` property out of this tutorial on purpose; the concept guide covers it.
- **C4:** Thanks, but I'm keeping this one. The style rule covers headings, and every other Crash Course chapter title (and the Groovy version of this page) uses Title Case. Changing only this one would make the chapter list inconsistent. If we move titles to sentence case, it should happen across the whole Crash Course in its own PR.
- **C5:** The property and its default are correct, but this is a first-time-user tutorial, and "sized to your CPU cores by default" is accurate at that level. `PeriodicUpdateGraph.updateThreads` is documented under "Managing thread pool sizes" in the concept guide. I extended the page's closing "For more depth" link to point readers there for thread pool sizes rather than adding the property inline.
- **C6:** This is already covered: the note says it assumes the default operation-initialization pool and that a different pool size changes the chunk count. I'd rather not name the property in a Crash Course chapter; it's documented in the concept guide's thread-pool section.
- **C7:** All true, but the exact threshold and gating rules belong in the configuration reference, not the tutorial. I reworded the sentence to "when there are enough rows to process at once (a few million)" so it also holds for ticking updates. The closing line now links to "Parallel processing with `select`" in the query table configuration page, which has the property and default.
- **C8:** I'm declining this one. The 100-row size is deliberate: it keeps the example cheap, and the notes around it say it's below the threshold. More rows also wouldn't show the race for most readers: a Python-backed formula is only eligible for parallel evaluation on a free-threaded build (`DhFormulaColumn.isParallelizable`), so on a standard build it stays serial at any size.
- **C9:** Agreed, the page's own notes contradict "runs in parallel by default." I reworded it to "Deephaven assumes formulas are safe to run in parallel unless you tell it otherwise." That's the point of the takeaway, and it doesn't need the full list of conditions, which live in the concept guide linked at the end.
- **C10:** Agreed, nothing on the page supports that clause. I removed it, along with "the only thing that guarantees…", which overstated things too, and merged the note with the C3 fix.

## Author queries
- **AQ1 [Example: a broken counter]:** The Groovy broken counter uses `emptyTable(5_000_000)` with a note that it crosses the threshold. The Python one uses 100 rows with a note that it doesn't. Was this difference deliberate (because Python formulas only parallelize on free-threaded builds), or should the two pages tell the same story? Either way, C8's fix doesn't help Python readers.

## Section-level notes
- **Too many comments on one note:** C3, C10 and (indirectly) C8 all land on the note under "The fix" (L197–198). That's a sign the section needs rethinking, not patching.
- **Clutter from earlier rounds:**
  - Three notes say nearly the same "this example is too small to parallelize" (L55, L171, L198).
  - L158 puts `QueryTable.minimumParallelSelectRows` into the narrative, which is exactly the property-name-in-narrative problem the skills warn against.
  - The L55 note carries thread-pool caveats.

  I'd recommend a `deephaven-doc-structure-review` pass over "Across rows" through "The fix." It could say once, near the top, that the examples are kept small, drop the property name from L158, and trim the L55 note, rather than patching each note in round 8.
- **Conflicts:** C8 (make the example large) cuts against earlier rounds' choice to keep examples small and explain it in notes. I kept the earlier decision.