# Accuracy review: Crash Course, "Query Parallelization" (Python)

**File reviewed:** `.claude/skills/deephaven-core-accuracy-check/evals/files/crash-course-parallelization-pre-review.md`, treated as `docs/python/getting-started/crash-course/parallelization.md` at DOC-857 commit `f2ef483084`.
**Category:** Tutorial, because it sits under `getting-started/crash-course/`. The configuration-placement rule for Concept guides and Tutorials therefore applies.
**Source of truth:** the source code at `/Users/margaretkennedy/dhc-skills-chip` HEAD. For link targets, I also checked the `f2ef483084` tree.
**Scope:** Python only, as you asked. `docs/groovy/getting-started/crash-course/parallelization.md` exists in `f2ef483084`. I did not check it, so every finding below still needs the same sweep on the Groovy page.

Overall, most of the page is accurate: the threshold, the chunk math, the thread-pool defaults, the Python free-threading gate, the example APIs and the `with_serial` contract. There are two substantive problems and a few smaller ones.

---

## 1. Substantive inaccuracies

### 1a. "`with_serial` is the only thing that guarantees…" overstates it, and the prescription isn't enough for shared state in general (line 198)

> Use `with_serial` any time your formula depends on shared state or row order, regardless of table size — … `with_serial` is the only thing that guarantees rows are processed one at a time, in order.

- **The contract** (`table-api/src/main/java/io/deephaven/api/ConcurrencyControl.java`, `withSerial()` javadoc): "The expression will never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order." That is about **one expression**. For other selectables, the same javadoc says ordering "is controlled by the value of the `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS`." That value defaults to false when `statelessSelectByDefault=true`, which is the default (`QueryTable.java` ~line 388–397).
- **Consequence:** if two columns (or two tables updating concurrently on update-graph threads) touch the same `counter`, `with_serial` on each one does **not** stop them running at the same time. That also takes barriers. The linked concept guide says the same thing (`docs/python/conceptual/query-engine/parallelization.md` lines 76–79, 116).
  - As written, "any time your formula depends on shared state" tells readers `with_serial` alone is enough, which it isn't.
  - "The only thing" is also too absolute. Setting `QueryTable.statelessSelectByDefault=false` makes formulas stateful and serial by default (`DhFormulaColumn.isStateless()`), and adds implicit barriers.
- **Suggested fix at Tutorial resolution:** "Use `with_serial` when a formula depends on row order or on state that only this formula touches. If several formulas or tables share the same state, you also need barriers — see [query parallelization](../../conceptual/query-engine/parallelization.md)." Drop "the only thing."
- **Duplicate sweep:**
  - Line 177: "tells Deephaven to process this formula serially" is correct because it is scoped to one formula.
  - Line 206, the Key takeaways bullet "Use `with_serial` to force sequential execution when your formula needs it", is acceptable.
  - Line 205, "Shared state … cause silent errors with parallelization", pairs with 206 to suggest `with_serial` is the whole remedy. The final link to barriers softens this. It is worth one clause ("…or barriers when several formulas share state") so the summary doesn't drop the qualification.

### 1b. "Deephaven runs formulas in parallel by default" contradicts the page's own counter section (line 204)

- **Source:**
  - `SelectColumnLayer.java` ~line 203 splits a column across threads only when `canParallelizeThisColumn && jobScheduler.threadCount() > 1 && !hasShifts && totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS`.
  - `DhFormulaColumn.isParallelizable()` and `FormulaColumnPython.isParallelizable()` return false for Python-backed formulas unless `PythonFreeThreadUtil.isPythonFreeThreaded()`.
- **The problem:** row-level parallelism within a column is **not** the default for small tables, for Python-backed formulas on standard GIL builds, or for updates that include shifts. Line 158 of the same page names the free-threaded and threshold conditions, so the takeaway contradicts text two screens above it.
- **Suggested fix:** "Deephaven may run formulas in parallel — across tables, across columns, and (for large tables) across rows — so formulas should be stateless."

---

## 2. Imprecise claims (true in the common case)

### 2a. The size threshold is per update, not per table, and has a type exception (line 57; restated at lines 158 and 171)

> Deephaven only splits a single column's row-wise computation across cores once a table is large enough (at least a few million rows).

- **Per update:** `SelectColumnLayer.createUpdateHandler` measures `totalSize = upstream.added().size() + upstream.modified().size()`. On a ticking table, what matters is the size of each update cycle, not the table. A 50M-row live table that adds 1,000 rows per cycle does not split those rows. For the static `empty_table` examples, "table size" is the same thing, so the examples are fine.
- **Type exception:** columns whose result type is `Table` or `RowSet` parallelize at any size (`resultTypeIsTableOrRowSet && totalSize > 0`, with `divisionSize = 1`). That is minor for a Crash Course. I'd leave it out rather than add a caveat, but "only" is technically too strong.
- **Suggested fix:** "…once the work is large enough (a few million rows at once)…". Avoid adding a parenthetical caveat.

### 2b. The note under the counter example leaves out the free-threading condition (line 171)

> …the race and duplicate IDs shown above would appear only on a table above the default parallelization threshold (about 4.2 million rows) or with a lowered threshold.

- On a standard GIL build, a Python-backed formula is never split across threads at any size (`FormulaColumnPython.isParallelizable()`: "If we are not free-threaded, then we cannot be parallelized"). Line 158 says so, but this note restates the condition without it.
- Read on its own, the note suggests a 5M-row table would show the race on standard Python. It would not, at least not through within-column parallelism.
- **Suggested fix:** add "on a free-threaded Python build" to the note, or cut the note back to "With 100 rows, the formula is evaluated serially."

### 2c. `Total` in the 20M-row note (line 55)

> …divide each column's computation into four chunks … independently for `Price`, `Quantity`, and `Total`. All four cores would compute their chunks simultaneously…

- **The chunk math is correct.** `divisionSize = max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(total/threads))`, which is max(4,194,304, 5,000,000) = 5M, giving 4 chunks. The thread count comes from `OperationInitializationThreadPool.threads` = -1, which resolves to `availableProcessors()` (`ThreadHelpers.getOrComputeThreadCountProperty`).
- **The problem:** `Total` depends on `Price` and `Quantity`. Its layer does not start until both have finished (`SelectAndViewAnalyzer.UpdateScheduler.doKickOffWork`: `readyToFire = !layers[nextLayer].getLayerDependencySet().intersects(remainingLayers)`). "Independently" and "simultaneously" read as if all three columns run at once.
- **Suggested fix:** "…independently for `Price` and `Quantity`, and then for `Total`, which needs their results."

---

## 3. Code example issues

- **Broken-counter snippet (lines 144–156):** it uses `empty_table` with no `from deephaven import empty_table`. It is tagged `syntax`, so the doc test won't catch it, but a reader who copies it gets a `NameError`. Add the import to match the `with_serial` example.
- **Every other snippet checked out against source:**
  - `time_table`, `empty_table` are exported from `deephaven/__init__.py`.
  - `agg.sum_` exists (`py/server/deephaven/agg.py:75`), as do `Table.where`, `agg_by` and `tail`.
  - `randomGaussian(double, double)` and `randomInt(int, int)` exist (`engine/function/.../Random.java`).
  - `hourOfDay(Instant, ZoneId, boolean)` exists (`DateTimeUtils.java:3278`), as does `dayOfMonth(Instant, ZoneId)` (`:3509`).
  - The literals `'ET'`, `'PT1m'`, `'P1D'` and `'2024-01-01T00:00:00 ET'` are parsed as timezone, Duration, Period and Instant (`TimeLiteralReplacedExpression.java`). The `' '` literal is left alone as a char because length ≤ 1 is skipped.
  - Duration × int is handled by `DateTimeUtils.multiply(Duration, long)`. Instant + Duration and Instant + Period are handled by `DateTimeUtils.plus(...)`.
  - `Selectable.parse(...).with_serial()` exists (`py/server/deephaven/table.py:157, 222`), and `Table.update` accepts a `Selectable` (`table.py:1364`).

---

## 4. Configuration detail in Tutorial narrative (placement, not truth)

- **Line 158** has `the default QueryTable.minimumParallelSelectRows (about 4.2 million rows)` in the middle of a sentence.
  - The value is correct: `1L << 22` is 4,194,304 (`QueryTable.java:336–337`), and the check is `>=`, so "larger than" is really "at least."
  - The property name doesn't belong in Crash Course prose. Use "about 4.2 million rows," or link to `docs/python/conceptual/query-table-configuration.md` (the file exists). Don't inline the name.
- **Line 55** has "This assumes the default operation-initialization thread pool, which uses one thread per core; a differently-sized pool changes the chunk count…". This is accurate, but it is the same kind of configuration aside. Consider cutting it to "on a 4-core machine."

---

## 5. Verified as accurate (no change needed)

- **Across tables (lines 14, 40):** `PeriodicUpdateGraph.updateThreads` defaults to -1, which becomes `availableProcessors()` (`PeriodicUpdateGraph.java:55, 141`). `ConcurrentNotificationProcessor` is used when `updateThreads > 1` (line 154–157). "Eligible to update concurrently … depends on the thread pool" is correctly hedged.
- **Across columns (lines 16, 61–69):** independent layers are fired as soon as their dependencies clear (`UpdateScheduler`), and `allowCrossColumnParallelization()` is `isStateless()`. This happens at any row count, so the 10-row example is valid. It is gated on `anyParallelColumns()` and `canParallelize()` for initialization (`QueryTable.java` ~line 1825), and on `parallelismFactor() > 1` for updates (`SelectOrUpdateListener.java:67–71`).
- **Free-threaded Python gate (line 158):** confirmed by `DhFormulaColumn.isParallelizable()` and `FormulaColumnPython.isParallelizable()`.
- **Serial below the threshold (line 171, first clause) and the line 198 note that the 100-row example wouldn't race:** confirmed by the `SelectColumnLayer` gate.
- **`with_serial` paraphrase (line 177):** "never running concurrently with itself, with rows evaluated one at a time in row-set order" matches "never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order." "One at a time" is equivalent to "sequentially," so no extra guarantee is added.
- **Line 198, "parallelization isn't the only way execution order can vary":** supported by `SelectColumn.isParallelizable()` javadoc: "Even if a column is not parallelizable, the engine may choose to evaluate it out-of-order and may not evaluate all rows individually."
- **Counter failure mode (lines 158–173):** this is a lost-update race. Because `return counter` re-reads the global, both duplicates and skipped values are possible, so the sample table is plausible.
- **"Always safe" examples (lines 75–131):** all are stateless, same-row formulas.

---

## 6. Links

| Link | Status |
|---|---|
| `../../reference/table-operations/filter/where.md` | Exists |
| `../../reference/table-operations/group-and-aggregate/aggBy.md` | Exists |
| `../../reference/table-operations/filter/tail.md` | Exists |
| `../../conceptual/query-engine/parallelization.md` | Exists |
| `../../reference/query-language/types/Selectable.md#with_serial` | **Exists in `f2ef483084`** (heading `### \`with_serial\`` at line 23, so the anchor resolves). **Missing on the current `dhc-skills-chip` HEAD**, which is not a descendant of that commit. Confirm the PR that adds `Selectable.md` lands before or with this page. |

**Suggested new link:** `update` (lines 177 and 194) → `../../reference/table-operations/select/update.md`, which exists.

---

## 7. Author queries

None. Every claim could be resolved from source.

## Completeness sweep

I re-grepped the Python file for each finding:
- Threshold and free-threaded wording appear at lines 57, 158, 171 and 198.
- The "parallel by default" claim appears at line 204.
- `with_serial` sufficiency appears at lines 177, 198, 205 and 206.
- The configuration property name appears only at line 158.

The Groovy page has not been swept, because it was out of scope.