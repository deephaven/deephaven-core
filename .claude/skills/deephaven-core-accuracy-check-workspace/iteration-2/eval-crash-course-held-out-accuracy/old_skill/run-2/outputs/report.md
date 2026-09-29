# Accuracy review: Crash Course, "Query Parallelization" (Python)

**File reviewed:** `.claude/skills/deephaven-core-accuracy-check/evals/files/crash-course-parallelization-pre-review.md`, treated as `docs/python/getting-started/crash-course/parallelization.md` at DOC-857 commit `f2ef483084`.
**Category:** Tutorial. It lives under `crash-course/`, so it is written for a first-time reader, and the configuration-placement rule applies to it the same way it applies to a Concept guide.
**Scope:** Python file only; I did not diff it against the Groovy sibling, as you asked. Findings 1–5 are about engine or Python behavior shared by both languages, so the Groovy page likely has the same problems and should get its own check.
**Source of truth:** the working tree at `/Users/margaretkennedy/dhc-skills-chip`. For link targets I also checked commit `f2ef483084`.

Overall, the core numbers and APIs are correct. The problems are overstated absolutes and one note that contradicts the page's own earlier text.

---

## Verified accurate (with source)

- **Row-splitting threshold "about 4.2 million":** `QueryTable.java:336-337` has `getLongWithDefault("QueryTable.minimumParallelSelectRows", 1L << 22)`, which is 4,194,304.
- **"20 million rows / 4 cores → four chunks of roughly 5 million":** `SelectColumnLayer.java:206-208` sets `divisionSize = Math.max(MINIMUM_PARALLEL_SELECT_ROWS, (totalSize + threadCount - 1) / threadCount)`. For these inputs that is max(4.19M, 5M), so 5M rows per chunk and four chunks. Correct.
- **Thread pools default to the processor count:**
  - The operation-initialization pool uses `getOrComputeThreadCountProperty("OperationInitializationThreadPool.threads", -1)` (`OperationInitializationThreadPool.java:30`). Values of 0 or less fall back to `Runtime.getRuntime().availableProcessors()` (`ThreadHelpers.java:18-24`).
  - The update graph uses `PeriodicUpdateGraph.updateThreads` with default -1, which also becomes `availableProcessors()` (`PeriodicUpdateGraph.java:55, 140-141`).
  - A static `update` starts on `OperationInitializerJobScheduler` (`QueryTable.java:1825-1828`). So the note's "default operation-initialization thread pool" is the right pool for the static example.
- **Across tables:** when `updateThreads > 1`, `PeriodicUpdateGraph.java:154-157` uses a `ConcurrentNotificationProcessor`. The hedged wording ("eligible to update concurrently") is accurate.
- **Across columns:** `SelectAndViewAnalyzer.UpdateScheduler.doKickOffWork` starts any layer whose `getLayerDependencySet()` doesn't overlap the layers still running. So `A` and `B` in the example can run concurrently, and no row-count threshold applies to this.
- **Python formulas parallelize only on free-threaded builds:**
  - `DhFormulaColumn.java:899-905` has `return !usesPython || PythonFreeThreadUtil.isPythonFreeThreaded();`.
  - `FormulaColumnPython.isParallelizable()` returns `PythonFreeThreadUtil.isPythonFreeThreaded()`.
  - `SelectColumnLayer.java:115-117` requires `sc.isStateless() && sc.isParallelizable()`.
- **`with_serial` contract** (`ConcurrencyControl.java` javadoc): *"The expression will never be invoked concurrently with itself"* and *"Rows are evaluated sequentially in row set order."* The doc's paraphrase ("never running concurrently with itself, with rows evaluated one at a time in row-set order") matches both clauses.
- **APIs used in the code:**
  - `Table.update` accepts `Union[str, Sequence[str], Selectable, Sequence[Selectable]]` (`table.py:1364-1366`).
  - `Selectable.parse` and `.with_serial()` exist (`table.py:157, 222`).
  - `agg.sum_` exists (`agg.py:75`), as do `Table.tail` and `time_table("PT1s")`.
- **Built-in functions:**
  - `DateTimeUtils` has `hourOfDay(Instant, ZoneId, boolean)` (line 3278), `dayOfMonth(Instant, ZoneId)` (3509), `multiply(Duration, long)` (2202) and `plus(Instant, Period)` (1790).
  - `'ET'` becomes a time zone through `TimeLiteralReplacedExpression` (`parseTimeZoneQuiet` branch).
  - A one-character `' '` is left alone as a char literal (`s.length() <= 1`).
- **`i` in formulas:** the static `empty_table` tables and the append-only `time_table` both pass the `usesI` safety check (`AbstractFormulaColumn.java:125`).
- **Links** (relative to `docs/python/getting-started/crash-course/`): `where.md`, `aggBy.md`, `tail.md`, `conceptual/query-engine/parallelization.md` and `reference/query-language/types/Selectable.md` all exist at `f2ef483084`. `Selectable.md` has a `` ### `with_serial` `` heading, so the `#with_serial` anchor resolves.
  - `Selectable.md` is **not** in the current working tree at the repo root. It exists only in the PR commit, so the link is valid only if that file merges with this page.

---

## Findings

### 1. Note under the broken counter contradicts the page's own free-threaded caveat (Medium)
- **Location:** lines 170-171.
- **The claim:** "the race and duplicate IDs shown above would appear only on a table above the default parallelization threshold ... or with a lowered threshold."
- **Why it's wrong:** line 158, two paragraphs earlier, correctly says this only applies on free-threaded Python builds. The source agrees: on a standard GIL build, `isParallelizable()` is false for Python-backed formulas (`DhFormulaColumn.java:905`), so `canParallelizeThisColumn` is false at any table size. On the default build, the race never appears, however large the table or low the threshold.
- **Second problem:** "would appear" overstates it. A race is possible, not guaranteed on every run.
- **Fix:** add the free-threaded condition, and change "would appear" to "can appear".

### 2. "Use `with_serial` any time your formula depends on shared state" is too broad (Medium)
- **Location:** line 198, repeated in the takeaway at line 206.
- **Why it's too broad:** `with_serial` only stops a formula from running concurrently *with itself*. By default, two different serial columns can still run concurrently with each other:
  - `SERIAL_SELECT_IMPLICIT_BARRIERS` defaults to `!STATELESS_SELECT_BY_DEFAULT`, which is false (`QueryTable.java:400-402`).
  - The javadoc says: *"If ... SERIAL_SELECT_IMPLICIT_BARRIERS is false, then no additional ordering between selectable expressions is imposed."*
  - So if the shared state is used by two columns (for example, two columns both calling `get_next_id()`), `with_serial` alone doesn't protect it. That case needs barriers.
- **Fix:** scope the statement to state used by one formula. The link to barriers at line 210 can cover the multi-column case. Don't add a config caveat here.

### 3. Absolute "only" claim, plus an unsupported rationale (Low, with an author query)
- **Location:** line 198.
- **The claim:** "`with_serial` is the only thing that guarantees rows are processed one at a time, in order."
- **Why "only" is wrong:** setting `QueryTable.statelessSelectByDefault=false` globally makes every formula stateful. Per the `QueryTable.java:381-387` javadoc, that means "formulas may not be parallelized within a column". `SelectColumnLayer` then takes `doSerialApplyUpdate`, which processes rows in order. `with_serial` is the per-formula guarantee, not the only one.
- **Fix:** say "the per-formula way to guarantee...". Don't add the property name to this tutorial (see finding 6).
- **AQ1 [The fix, NOTE para]:** "parallelization isn't the only way execution order can vary". What other mechanism does this refer to? For a single-column static `update` below the threshold, the non-parallel path (`doSerialApplyUpdate`) evaluates rows in row-set order. I found no other reordering path in source, so please name it or drop the clause.

### 4. "Deephaven runs formulas in parallel by default" is overstated (Low-Medium)
- **Location:** line 204, Key takeaways.
- **Why it's overstated:** the page itself says a column's rows are split only above about 4.2M rows (line 57), and Python-backed formulas only on free-threaded builds (line 158). The source adds more conditions. `SelectColumnLayer.java:203-205` also requires:
  - `jobScheduler.threadCount() > 1`
  - no row shifts in the update
  - rows added plus modified in *that update* ≥ threshold. This counts rows in the update, not the table's size.
- **Fix:** something like "Deephaven parallelizes formulas when it's worthwhile, and assumes they're stateless by default." The TIP at line 8 ("benefit ... automatically") is acceptable as it stands.

### 5. The threshold is stated for all row-splitting, but it applies only to `update`/`select` (Low)
- **Location:** line 57, "only splits a single column's row-wise computation across cores once a table is large enough (at least a few million rows)".
- **Why it's too narrow:** the "Across rows" section frames splitting as general ("Within a single table, Deephaven splits the data into chunks"), but the few-million figure is only the `update`/`select` gate. Other operations use different gates:
  - **`where`:** parallelizes once `numberOfRows / 2 > QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT` (default `1 << 16`), which is about 131K rows (`AbstractFilterExecution.java:732`, `QueryTable.java:275-276`).
  - **Sort:** parallelizes from `1 << 20` rows (`QueryTable.java:349-350`).
  - **Table- or RowSet-valued formula columns:** split at any size above 0 (`SelectColumnLayer.java:204-206`). This is a minor exception.
- **Fix:** scope the sentence to "When you add columns with `update` or `select`...", with no numbers for the other operations.
- **Completeness:** "three ways" (tables, rows, columns) is a fair tutorial simplification. Parallel `where`, sort, snapshot, `update_by` and range-join initialization all fit under "across rows" or "across tables", so I don't count them as a missing category.

### 6. Configuration property name in tutorial narrative (Low, placement)
- **Location:** line 158, "larger than the default `QueryTable.minimumParallelSelectRows` (about 4.2 million rows)".
- **Why it's a problem:** a property name dropped into Crash Course prose is configuration detail in the narrative.
- **Two smaller errors in the same sentence:**
  - The check is `>=`, not "larger than".
  - It's measured on the rows in the update, not the table size.
- **Proposed destination:** say "a very large table (millions of rows)" in the prose, and leave the property to the linked concept guide or `docs/python/conceptual/query-table-configuration.md`. Don't add it inline.

### 7. The broken-counter explanation doesn't produce the sample output (Low)
- **Location:** line 173, compared with the table at lines 160-168.
- **The mismatch:** the prose describes a lost update: both cores read `counter = 5`, both write 6, both return 6. That produces a duplicate 6 but **no skipped number**, because the counter ends at 6. The table instead shows 5 repeated and 6 missing.
- **Where skips come from:** `return counter` re-reads the global after another thread's increment. For example, A increments to 6, B increments to 7, and both return 7.
- **Fix:** either describe that interleaving, or make the table match the lost-update explanation (for example, "...5, 6, 6, 7").

### 8. Snippet can't run as written (Low)
- **Location:** lines 144-156, the `python syntax` block.
- **The problem:** it calls `empty_table` without `from deephaven import empty_table`, so a reader who pastes it gets a `NameError`.
- **Fix:** add the import. It's tagged `syntax`, so the build won't catch this.

---

## Links
All existing links resolve at `f2ef483084` (details above). The only dependency is that `reference/query-language/types/Selectable.md` must merge along with this page, because it isn't in the current tree.

## Completeness sweep (Python file)
- **Free-threaded or threshold condition:** appears at lines 158, 171 and 198. Line 171 is the one missing the free-threaded condition (finding 1).
- **"Parallel by default":** appears at lines 8 and 204 (finding 4).
- **"`with_serial` for any shared state":** appears at lines 198 and 206 (finding 2).
- **Threshold wording:** appears at lines 55, 57, 158, 171 and 198. Only lines 57 and 158 need rewording (findings 5 and 6).
- **Groovy sibling:** not swept, per your scope. Findings 1–5 are about engine behavior, so its equivalent passages likely need the same changes.
