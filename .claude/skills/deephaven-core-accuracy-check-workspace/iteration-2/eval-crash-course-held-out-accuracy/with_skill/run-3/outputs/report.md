# Accuracy review: `docs/python/getting-started/crash-course/parallelization.md` (DOC-857 snapshot, f2ef483084)

**Category:** Tutorial. It sits under `getting-started/crash-course/`, so I judged it on the Tutorial profile. That affects how configuration detail should be placed (see finding 7).

**Scope:** Python file only, as you asked. A Groovy sibling exists at `docs/groovy/getting-started/crash-course/parallelization.md` in the PR tree, and the skill normally requires checking it and fixing both files together. I did not check it. Findings 1, 2, 4 and 7 very likely apply there too.

**Source of truth:** I checked source at the `dhc-skills-chip` HEAD (373cece093). I also checked the key points against f2ef483084 itself: the threshold, `isParallelizable`, and the `ConcurrencyControl` contract are the same in both.

---

## Findings

### 1. Shared-state guidance misses cross-column concurrency, so `with_serial` alone is described as enough when it isn't (Medium-High)
**Lines 137, 158, 198, 206**

The page says the counter race needs two conditions: a free-threaded Python build and a table above the row threshold. It then says to use `with_serial` "any time your formula depends on shared state", and adds that it "is the only thing that guarantees" correct order. That holds for a **single** column. It fails when two columns in the same `update` share state:

- **Each column runs as its own job.** Every column becomes its own layer, and `UpdateScheduler.doKickOffWork` starts any layer whose dependencies are done (`SelectAndViewAnalyzer.java`).
- **Nothing blocks that by default.** `SelectColumnLayer.allowCrossColumnParallelization()` returns `selectColumn.isStateless()`. `DhFormulaColumn.isStateless()` returns `true` whenever `QueryTable.STATELESS_SELECT_BY_DEFAULT` is set, and that defaults to `true`.
- **No size or Python-build gate applies.** Cross-column concurrency happens at any table size and on GIL Python builds. The `isParallelizable` Python/free-threaded check only affects splitting rows *within* a column.
- **`with_serial` only covers its own column.** The `ConcurrencyControl.withSerial` javadoc says: "The expression will never be invoked concurrently with itself." On ordering between columns, it adds: "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed … To impose further ordering constraints, use barriers." `SERIAL_SELECT_IMPLICIT_BARRIERS` defaults to `!STATELESS_SELECT_BY_DEFAULT`, which is `false` (`QueryTable.java`).

So two `with_serial` columns calling the same counter can still run at the same time and race.

The PR's own conceptual guide makes this point ("When shared state is involved, you often need both…"), but the crash course drops it.

- **Fix:** Add one sentence to the `with_serial` section: if more than one column touches the same state, `with_serial` on each column isn't enough, and you also need a barrier (link to the conceptual guide's Barriers section).
- **Fix:** Scope line 198's "the only thing that guarantees…" to a single formula.
- **Fix:** Match the takeaway at line 206.

### 2. "Deephaven runs formulas in parallel by default" contradicts the page's own caveats (Medium)
**Line 204**

The page itself says two things that conflict with this takeaway:
- Lines 57 and 171: a column is only split across cores above the threshold (4,194,304 rows by default).
- Line 158: Python-backed formulas are split only on free-threaded builds (`DhFormulaColumn.isParallelizable`: `return !usesPython || PythonFreeThreadUtil.isPythonFreeThreaded();`).

For most Crash Course readers (GIL Python, small tables), formulas are not split across rows at all.

- **Fix:** Say "Deephaven *may* run formulas in parallel by default, so formulas must be safe to run that way."

### 3. The "few million rows" threshold applies only to `select`/`update` (Low)
**Lines 15, 44, 57**

Line 57 is scoped to "a single column's row-wise computation", so it is literally correct. The "Across rows" framing is general, though, and the "Across tables" example uses `where`, which splits at far smaller sizes:
- `where`: `AbstractFilterExecution.shouldParallelizeFilter` requires `numberOfRows / 2 > QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT`, with a default of `1 << 16`. That is about 131K rows.
- Sort: `MINIMUM_PARALLEL_SORT_ROWS` defaults to `1 << 20`.

- **Fix:** Keep the sentence general, with no new numbers: "when computing a column's values (`update`/`select`)…".

### 4. The paraphrase of the `with_serial` contract adds "one at a time" (Low)
**Lines 177, 198**

The source contract says: "Rows are evaluated sequentially in row set order" and "never be invoked concurrently with itself" (`ConcurrencyControl.java`). It does not promise per-row calls ("one at a time"). The serial path fills whole chunks.

- **Fix:** Use "sequentially, in row-set order".

### 5. "Reads or modifies a variable that other rows also use" is too broad (Low)
**Line 137**

Reading a shared value that nothing modifies is safe. The engine makes the same distinction: the non-default path of `DhFormulaColumn.isStateless()` treats immutable query-scope parameters (`isImmutableType`) as stateless.

- **Fix:** "modifies a variable, or reads one that other rows modify".

### 6. The broken-counter example fails with `NameError` if copied (Low)
**Lines 144–156**

It calls `empty_table` without `from deephaven import empty_table`. The block is tagged `python syntax`, so doc validation won't catch it.

### 7. A configuration property is named in the middle of Tutorial prose (placement)
**Line 158**

The value is correct: `QueryTable.minimumParallelSelectRows` defaults to `1L << 22`, which is 4,194,304, so "about 4.2 million" is right. The property name itself doesn't belong in Crash Course narrative, though.

- **Fix:** Reword to "a table larger than the parallelization threshold (about 4.2 million rows by default)". Put the property name behind a link to `../../conceptual/query-table-configuration.md#parallel-processing-with-select`, which exists in the PR tree under the heading "Parallel processing with `select`".

---

## Checked and correct

**Across rows (line 55):** 20M rows on 4 threads splits into four jobs of about 5M rows. The chunk size is `divisionSize = max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(total/threads))`, which gives `max(4.19M, 5M)`. "A differently-sized pool changes the chunk count" is also true: at 8 threads each chunk stays at 4.19M, so you get 5 chunks, not 8 (`SelectColumnLayer.java:203–208`).

**Thread pools:**
- The operation-initialization pool defaults to one thread per core: `OperationInitializationThreadPool.threads` is `-1`, which maps to `availableProcessors()` via `ThreadHelpers`.
- The update graph pool is "sized to your CPU cores by default": `PeriodicUpdateGraph.updateThreads` is `-1`, so it uses `availableProcessors()`, and `ConcurrentNotificationProcessor` is used when there is more than one thread.
- Neither property is overridden in `props/configs`.

**Across columns (lines 61–69):** Independent columns can run concurrently, gated by `anyParallelColumns()` and `canParallelize()`. Dependent columns wait for their inputs through the layer dependency bitset.

**Python on free-threaded builds (line 158):** The claim matches `DhFormulaColumn.isParallelizable`.

**Line 198, "parallelization isn't the only way execution order can vary":** Supported by the `SelectColumn.isParallelizable` javadoc: "Even if a column is not parallelizable, the engine may choose to evaluate it out-of-order."

**Code examples:** All Python APIs exist with the signatures used:
- `time_table`
- `agg.sum_(cols)`
- `Table.tail(num_rows)`
- `Selectable.parse` / `with_serial` (`py/server/deephaven/table.py`)
- `randomGaussian(double, double)` and `randomInt(int, int)` (`Random.java`)
- `hourOfDay(Instant, ZoneId, boolean)` and `dayOfMonth(Instant, ZoneId)`
- `DateTimeUtils.multiply(Duration, long)` for `'PT1m' * i`

`' '` is a valid char literal: `TimeLiteralReplacedExpression` skips single-quoted strings of length 1 or less.

**Links:**
- `where.md`, `aggBy.md`, `tail.md` and `conceptual/query-engine/parallelization.md` all exist.
- `reference/query-language/types/Selectable.md#with_serial` exists at f2ef483084, with a `### \`with_serial\`` heading. It is **not** in the current `dhc-skills-chip` checkout, so the link only resolves once that PR's file lands.

**Missing links:** `empty_table`, `time_table` and `update` appear without links. Their targets exist in the PR tree:
- `reference/table-operations/create/emptyTable.md`
- `reference/table-operations/create/timeTable.md`
- `reference/table-operations/select/update.md`

## Separate follow-up (not in this file)
The current checkout's `docs/python/conceptual/query-engine/parallelization.md` still says "Deephaven's default behavior is to treat both `A` and `B` statefully". That contradicts `STATELESS_SELECT_BY_DEFAULT=true` and the page's own 41.0 note. The PR-tree version no longer has this sentence, so check that it is removed when the PR merges.