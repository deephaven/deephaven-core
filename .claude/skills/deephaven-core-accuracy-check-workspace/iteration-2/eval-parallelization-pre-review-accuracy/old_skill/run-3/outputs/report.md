# Accuracy review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857 snapshot, commit f2ef483084)

**What I checked:** the snapshot file `.claude/skills/deephaven-core-accuracy-check/evals/files/parallelization-pre-review.md`. It is byte-identical to `git show f2ef483084:docs/python/conceptual/query-engine/parallelization.md`. I verified claims against the source in `/Users/margaretkennedy/dhc-skills-chip`. I edited nothing.

**Category:** Concept guide, because the file is under `conceptual/`. Its sidebar spot under Best practices → Performance is only for discoverability. So configuration detail belongs in a Configuration section or a link, not in the narrative.

**Groovy sibling:** `docs/groovy/conceptual/query-engine/parallelization.md` exists, but you put it out of scope, so I didn't diff it. Most findings below are about engine behavior, not about Python, so the same claims probably need fixing in the Groovy page.

---

## 1. Inaccurate or overstated claims

### 1.1 The GIL caution: "never run concurrently" is false for selectables (L75)
The doc says: "Deephaven only considers Python-backed filters and selectables for parallel execution on a free-threaded Python build — on a standard (GIL-enabled) build, they're never run concurrently."

- **What the source shows:** `isParallelizable()` returns false for Python-backed formulas on a GIL build.
  - `DhFormulaColumn.java:905`: `return !usesPython || PythonFreeThreadUtil.isPythonFreeThreaded();`
  - `FormulaColumnPython.java:65`: same idea.
- **What that flag controls:** only splitting one column's rows across threads (`SelectColumnLayer.java:115-117`: `canParallelizeThisColumn = ... && sc.isStateless() && sc.isParallelizable();`).
- **What it doesn't control:** running columns side by side. That depends only on statelessness.
  - `SelectColumnLayer.java:657-658`: `allowCrossColumnParallelization() { return selectColumn.isStateless(); }`
  - `FormulaColumnPython.isStateless()` returns `QueryTable.STATELESS_SELECT_BY_DEFAULT`, which is `true`.
- **Result:** two Python formulas in one `update` run as separate layers that can execute at the same time (`SelectAndViewAnalyzer.UpdateScheduler`). The same Python function can also run concurrently from different tables' updates (the "across tables" mechanism).
- **Fix:** on a GIL build, the engine doesn't split a Python formula's rows across threads. It can still run it alongside other columns or tables.
- **Same claim repeated:** "Forces single-threaded access" (Quick reference, L22). Selectable.md at the same commit also says "it is never invoked concurrently". Selectable.md is a separate page, so treat it as a follow-up.

### 1.2 The counter example can't produce the output it shows (L162–190)
- **The row-order symptom:** "row 4 has `A=4` after row 3 has `A=5`" means values out of order within column A. That needs row splitting, which a Python formula never gets on a GIL build (see 1.1). What can actually happen there is A and B interleaving, because they run concurrently as separate columns. Within each column, values would still increase.
- **"Gaps (no 10-19 visible)":** the sample table contains every value 0–9 exactly once, so it shows no gap.
- **"`B` not following `A + 1`":** B = A + 1 is never the correct result, even when fully serial. The engine evaluates whole columns one layer at a time. The barrier section itself says A gets 0–9 and B gets 10–19 (L280). So the example implies a "correct" outcome that the doc later contradicts.
- **The prose (L180):** "multiple threads read and update `counter` simultaneously" is only true across columns on a GIL build.
- **Fix:** redo the sample table and the explanation so they match what the engine can actually do.

### 1.3 The barrier example's "without X" claims don't hold (L244, L280)
- **"Without a barrier, both columns would start simultaneously, both read `counter = 0`":**
  - Both columns use `with_serial`, which wraps them in `StatefulSelectColumn`, so `isStateless()` is false.
  - That makes `anyParallelColumns()` false (`SelectAndViewAnalyzer.java:1047-1050`).
  - So `QueryTable.java:1825-1830` picks `ImmediateJobScheduler`, and the layers run one after the other on the calling thread.
  - With default settings, removing the barrier doesn't cause the described race.
  - The barrier is still what *guarantees* the order (ConcurrencyControl javadoc: "the respecting filter/selectable will be executed entirely after the filter/selectable declaring the barrier"). Keep the barrier, but describe what's lost as the guarantee, not a race you can see.
- **"Without `with_serial`, rows within each column would also race":** false for this example.
  - The table has 10 rows. Row splitting needs `totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS`, which is `1L << 22` (`SelectColumnLayer.java:205`).
  - The formula is Python, so it isn't row-parallelizable on a GIL build anyway.
  - What removing `with_serial` actually changes here: the columns become stateless, so they can run concurrently. The barrier still orders them.

### 1.4 `with_serial` "on a single thread" overstates the contract (L148)
- **The contract:** `ConcurrencyControl.java` (table-api) says "The expression will never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order." It doesn't promise one thread.
- **L213** ("only one thread processes the column at a time") is accurate.
- **L148** ("on a single thread") and **L20** ("Serialize access to shared resource") overstate it. `with_serial` doesn't serialize access from other columns, filters, or tables that touch the same file, logger, or library. L22 ("Forces single-threaded access") has the same problem.

### 1.5 The "Breaking change in Deephaven 41+" box overstates the old behavior (L9)
- **When it changed:** the default flipped in `8ea55b6a1d` (DH-20714, #7331). The first tag containing that commit is `v41.0.0`, so the version number is right.
- **What was wrong:** "Deephaven 40 and earlier assumed all formulas required sequential processing" is false. Before the change, `DhFormulaColumn.isStateless()` still returned true when every parameter was an immutable type and every column it used was stateless. So many formulas were already parallelized. Only `ConditionFilter.permitParallelization()` depended purely on the flag, which was then false.
- **"will now produce incorrect results":** this should be "can". Small tables, Python formulas on GIL builds, and runs that happen to schedule serially all still give correct results.

### 1.6 The "What does NOT get parallelized" list misclassifies `view`, `update_view`, and `lazy_update` (L70)
- **What's true:** these operations do no work when the table is created, as the doc says.
- **What's wrong:** their formulas run later, on whatever thread reads the column. That includes parallel `where` segments, parallel `update`/`select` layers that reference the column, and reads from several update threads at once. So they can run concurrently.
- **The engine's own guard:** `QueryTable.java` (`viewOrUpdateView`) throws `"view and updateView cannot respect barriers"`. With `statelessSelectByDefault=true` it also throws `"A stateful column cannot safely be used in a view or updateView."`.
- **Fix:** don't list these as "not parallelized". Say they compute nothing up front, and that their formulas can still run concurrently on the threads that read them.

### 1.7 The "What gets parallelized" list is incomplete, and one entry is missing a condition (L62–66)
- **`sort`:** only parallelized during initialization. During update cycles, a sort listener sorts serially. `SortHelpers.java:1133-1135`: "Update graph refresh threads have the poisoned ExecutionContext ... a sort listener running there sorts serially."
- **Missing operations:** these also parallelize, but aren't listed:
  - `update_by`: `UpdateBy.java:307-339` uses `OperationInitializerJobScheduler` or `UpdateGraphJobScheduler`.
  - `range_join`: `RangeJoinOperation.java:257-261`.
  - Snapshots: `ConstructSnapshot.java:1386`, gated by `QueryTable.enableParallelSnapshot` and `minimumParallelSnapshotRows`. `query-table-configuration.md` already has a "Parallel snapshotting" section.

### 1.8 The "single core below thresholds" note contradicts the across-columns section (L124)
- **The claim:** "Below those thresholds, Deephaven evaluates the formula on a single core."
- **What the source shows:** the size threshold only controls row splitting. Running columns side by side has no size threshold. It depends only on `anyParallelColumns()` and whether the operation initializer can parallelize (`QueryTable.java:1825-1827`).
- **Why it matters:** the doc's own `update(["A = X * 2", "B = Y + 1"])` example (L60) can use two cores on 10 rows.
- **Related absolute (L83):** "dividing the rows among CPU cores" during initialization states unconditionally what the L124 note later limits to large tables.

### 1.9 Implicit barriers: wrong names, and the default link is missing (L320–323)
- **Wrong identifier:** `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is the Java field. The property users can set is `QueryTable.serialSelectImplicitBarriers`.
- **Made-up mode names:** "Stateless mode (default)" and "Stateful mode" don't appear anywhere in the source.
- **Missing dependency:** the default isn't fixed. `QueryTable.java:400-402`: `getBooleanWithDefault("QueryTable.serialSelectImplicitBarriers", !STATELESS_SELECT_BY_DEFAULT)`. Setting `statelessSelectByDefault=false` turns implicit barriers on automatically.
- **Wider scope than stated:** implicit barriers apply to every non-stateless column (`SelectAndViewAnalyzer.java:176`: `if (QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS && !sc.isStateless())`), not just `with_serial` ones. They apply only to selectables, not filters.
- **Source bug (separate follow-up):** the `ConcurrencyControl.withSerial()` javadoc says the property defaults "to the value of `QueryTable.statelessSelectByDefault`". The code uses the opposite value.

### 1.10 "Concurrency control works the same way for `Filter` as it does for `Selectable`" (L135)
This is overstated. The contract treats them differently:
- For a filter, "serial acts as an absolute reordering barrier".
- For selectables, extra ordering depends on `SERIAL_SELECT_IMPLICIT_BARRIERS`.

L234 describes the filter behavior correctly, so L135 contradicts it.

### 1.11 Update-thread default (L50)
The doc says: "This depends on `PeriodicUpdateGraph.updateThreads` being greater than 1, which is the default."

The default is actually `-1`, which means `Runtime.availableProcessors()` (`PeriodicUpdateGraph.java:55, 140-141`). On a one-core host that gives 1, and notifications then use `QueueNotificationProcessor`. So "greater than 1" isn't itself the default. (This is also configuration detail in the narrative; see section 2.)

### 1.12 Minor
- **L99, "Produces the same output for the same input":** by this definition, the page's own `randomGaussian`/`randomInt` example (L39–40) isn't stateless, yet it runs in parallel by default. The source's definition is narrower: `SelectColumn.isStateless()`: "one row does not depend on the order of evaluation for another row."
- **L93, "Deephaven parallelizes operations that are stateless":** the engine doesn't detect statelessness. It assumes it when `statelessSelectByDefault`/`statelessFiltersByDefault` is true (`DhFormulaColumn.isStateless()` returns true right away). L126 gets this right.

---

## 2. Configuration detail in the narrative (move it, don't delete it)
Property names and defaults appear inline at:
- L50: `PeriodicUpdateGraph.updateThreads`
- L66: `minimumParallelSortRows`, `parallelSort=false`
- L83, L87: thread-pool properties
- L124: `minimumParallelSelectRows`, `parallelWhereRowsPerSegment`, with numbers
- L320–323: implicit-barrier property

Every value I checked is correct:

| Property | Default | Source |
|---|---|---|
| `QueryTable.minimumParallelSelectRows` | `1L << 22` (4,194,304) | `QueryTable.java:337` |
| `QueryTable.parallelWhereRowsPerSegment` | `1 << 16` | `QueryTable.java:276` |
| `where` parallel threshold | `numberOfRows / 2 > PARALLEL_WHERE_ROWS_PER_SEGMENT` | `AbstractFilterExecution.java:732` |
| `QueryTable.minimumParallelSortRows` | `1L << 20` | `QueryTable.java:353` |
| `QueryTable.parallelSort` | `true` | `QueryTable.java:344` |
| `OperationInitializationThreadPool.threads` | `-1` | `OperationInitializationThreadPool.java:30` |
| `PeriodicUpdateGraph.updateThreads` | `-1` | `PeriodicUpdateGraph.java:55` |

Because this is a Concept guide, put these in one Configuration section at the end, or link to `../query-table-configuration.md`. At f2ef483084 that page already covers each of them: `#parallel-processing-with-where`, `#parallel-processing-with-select`, `#parallel-sorting`, `#stateless-by-default`.

It also adds a detail L124 leaves out: for updates to a refreshing table, the `where` threshold applies to each cycle's added and modified rows. So a large table that gets small ticks may not parallelize its per-update filtering.

---

## 3. Verified accurate (with source)
- **`Selectable.parse`, `with_serial`, `with_declared_barriers`, `with_respected_barriers`:** `py/server/deephaven/table.py:140-238`. They accept one `Barrier` or a sequence.
- **`update`/`select` accept `Selectable`s:** `table.py:1364-1366` and `1446-1450`.
- **`view`/`update_view`/`lazy_update` accept only strings:** `table.py:1389, 1408, 1427`. So the L156 box ("cannot be used with `view` or `update_view`") holds.
- **`deephaven.filters`:** `is_null`, `not_`, and `Filter.with_serial` exist (`filters.py:95, 175, 187`). `Barrier` and `ConcurrencyControl` are in `deephaven.concurrency_control`.
- **Query-language functions:** `randomGaussian(double, double)` and `randomInt(int, int)` are in `engine/function/.../Random.java:57, 173`. `sqrt` is in `Numeric.ftl`.
- **"Each barrier can only be declared by one operation":** `SelectAndViewAnalyzer.java:343-346` throws "Duplicate barrier".
- **Multiple-barriers execution order (L314):** consistent with layer dependencies in `SelectAndViewAnalyzer.java:194-202`. A and B are stateless, so they *can* run in parallel.
- **"the engine may still evaluate a non-parallelizable column out of order" (L77):** `SelectColumn.isParallelizable()` javadoc: "the engine may choose to evaluate it out-of-order and may not evaluate all rows individually."
- **Partition filters (L329–331):** `PartitionAwareSourceTable.isPrioritizablePartitioningFilter` never checks statelessness or `permitParallelization`. A serial filter sends it and every later filter into the per-row set (`whereImpl`, around L332).
- **Free-threaded detection:** `PythonFreeThreadUtil.java:43` checks for "free-threading" in the Python version string, so no other configuration is needed.

---

## 4. Links
All relative links resolve at f2ef483084. Three of them (`types/Selectable.md`, `types/Filter.md`, `crash-course/parallelization.md`) come from the PR itself and don't exist at the checkout's current HEAD, so they depend on the PR merging with them. The anchors `#with_serial`, `update.md#serial-execution`, `where.md#serial-execution`, and all internal `#...` anchors exist.

- **Suggested internal links:** `Barrier` (L137, L240) and `ConcurrencyControl` (L153) link to external Pydoc. Internal reference pages exist at the commit: `../../reference/query-language/types/Barrier.md` and `../../reference/query-language/types/ConcurrencyControl.md` (confirmed with `git ls-tree`).
- **L153 `with_serial` link:** it points to `update.md#serial-execution`. The `Selectable.md#with_serial` target used elsewhere is the method's own reference.
- **External links to check by hand:** docs.python.org free-threading, Oracle `Runtime.availableProcessors` (Java 11), and the deephaven.io Pydoc URLs.

---

## 5. Author queries
- **AQ1 [Serialization, counter example]:** what output do you want to show? A free-threaded build with ≥4.2M rows can produce out-of-order values within a column. A GIL build only interleaves A and B. Pick one scenario and make the table match it.
- **AQ2 [Multiple barriers, L316]:** "most filters are stateless and don't need barriers" is a usage claim with nothing in the source behind it. SME to confirm or drop it.

## 6. Follow-ups outside this file
- `ConcurrencyControl.withSerial()` javadoc has the implicit-barrier default backwards compared with `QueryTable.java:400-402`.
- `docs/python/reference/query-language/types/Selectable.md` repeats the "never invoked concurrently" claim from 1.1.
- The Groovy sibling needs the same review for 1.3, 1.4, 1.6–1.10, and section 2.
