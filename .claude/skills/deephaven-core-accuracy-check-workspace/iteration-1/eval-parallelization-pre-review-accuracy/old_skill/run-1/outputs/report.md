<!-- Saved by the orchestrating session from the subagent's returned text; the subagent's own Write was refused. -->

Summary: most of what the page says about engine config properties, defaults, the Python API and barrier semantics checks out against the source, and every relative link and anchor resolves at f2ef483084. However, the counter and barrier examples show failures that can't happen as written on a standard (GIL) Python build, the "GIL means never concurrent" caveat is overstated, the Deephaven 40 behavior and the Query table configuration link are wrong, and three more claims are missing important qualifiers.

---

# Accuracy review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857, commit f2ef483084)

This review covers only the Python file and makes no edits. I compared the snapshot with `git show f2ef483084:docs/python/conceptual/query-engine/parallelization.md` and the two are identical. I checked claims against the engine source at the repo HEAD and checked links against the tree at f2ef483084.

**Category:** Concept guide. The file is under `conceptual/`, and `ref-deephaven-doc-categories` names this exact page as a concept guide, even though the sidebar places it under Performance. Because of that, I weighed gaps in its enumerated lists as a narrative concept guide, not as reference-guide defects.

**Cross-language sibling:** `docs/groovy/conceptual/query-engine/parallelization.md` exists. It is out of scope for this review, but most findings below are about shared engine behavior, so the same claims probably need fixing there too.

---

## High: examples that don't show what they claim

### 1. The counter race example (L175–190) can't produce the output shown on a standard (GIL) Python build

- **What the doc claims:** the sample output shows column `A` going backwards: "row 4 has `A=4` after row 3 has `A=5`".
- **Why that can't happen on a standard build:**
  - The formula calls a Python function, so its column reports `isParallelizable() == false` unless Python is free-threaded (`DhFormulaColumn.java:899-906`): "If we are not free-threaded, then we must be stateful for performance reasons … `return !usesPython || PythonFreeThreadUtil.isPythonFreeThreaded();`"
  - `SelectColumnLayer.java:115-117` only splits a column into row ranges when `canParallelizeThisColumn = !isRedirected && … && sc.isStateless() && sc.isParallelizable()`.
  - So on a standard build, each column is filled in order on one thread, and `A` rises steadily.
- **What does happen:** `A` and `B` can still run at the same time on different threads, because `allowCrossColumnParallelization()` returns `selectColumn.isStateless()` (`SelectColumnLayer.java:657-659`). That produces interleaved values with gaps, which covers the doc's "gaps" and "`B` not following `A + 1`" points. The backwards values in `A` can only appear on a free-threaded build. That build meets the size threshold here: 5,000,000 is at least `minimumParallelSelectRows` = `1L << 22`.
- **Fix:** change the sample output and explanation so they match a standard build (interleaving between columns, `A` still increasing), or say the within-column disorder needs free-threaded Python.

### 2. The "fix" (L192–211) doesn't fix the two-column problem it follows

- The broken example updates two columns, `A` and `B`. The fix switches to one column, `ID`.
- A single serial column says nothing about whether serial `A` and `B` stay in order relative to each other. With the default `serialSelectImplicitBarriers=false`, the `ConcurrencyControl.withSerial` javadoc says: "If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, then no additional ordering between selectable expressions is imposed … To impose further ordering constraints, use barriers."
- **Fix:** either keep the fix to one column and say so, or show that two columns also need a barrier (which the Barriers section covers later).

### 3. The barrier example overstates what goes wrong without the barrier (L244, L280)

- **What the doc claims:** "Without a barrier, both columns would start simultaneously, both read `counter = 0`, and produce overlapping, incorrect results," and "Without `with_serial`, rows within each column would also race."
- **Without the barrier:**
  - When every column is serial, `SelectAndViewAnalyzer.anyParallelColumns()` is false (`SelectAndViewAnalyzer.java:1047-1051`).
  - `QueryTable.java:1825-1831` then uses `ImmediateJobScheduler`, so the columns run one after another on the calling thread. They don't "start simultaneously."
  - The barrier still matters as a guarantee. With implicit barriers off, nothing guarantees `A` runs before `B`, and adding one stateless column would switch to the parallel scheduler. But "both read 0 / overlapping" isn't what actually happens.
- **Without `with_serial`:**
  - The table has 10 rows, far below `minimumParallelSelectRows` (`SelectColumnLayer.java:203-205`).
  - The formulas are Python-backed and therefore not parallelizable on a GIL build.
  - So rows within a column won't race here. What's accurate is that in-order evaluation isn't guaranteed: the `SelectColumn.isParallelizable` javadoc says "the engine may choose to evaluate it out-of-order".
- **Fix:** describe what the barrier and `with_serial` guarantee, not failures this code can't reproduce as written.

---

## Medium: wrong or overstated claims

### 4. "On a standard (GIL-enabled) build, they're never run concurrently" (L75) is overstated

- For filters it's correct: `ConditionFilter.permitParallelization()` returns `false` when the filter uses Python and Python isn't free-threaded (`ConditionFilter.java:807-819`).
- For selectables, the GIL build only stops a Python-backed column from being split into row ranges. The column is still stateless by default (`FormulaColumnPython.isStateless()` returns `STATELESS_SELECT_BY_DEFAULT`), so it can run at the same time as other columns (`allowCrossColumnParallelization()` = `isStateless()`). That concurrency is exactly what drives the counter race in finding 1.
- **Fix:** say a Python-backed column isn't split across threads on a GIL build, but can still run alongside other columns in the same `select`/`update`.
- The same simplification appears in L9 and L345 ("assumes all formulas can run in parallel by default"). They are fine as the high-level rule, but should stay consistent with the corrected wording.

### 5. The Deephaven 40 behavior is overstated (L9)

- **What the doc claims:** "Deephaven 40 and earlier assumed all formulas required sequential processing by default."
- **What the source shows:**
  - The default flip happened in `8ea55b6a1d` (DH-20714), which first appears in the v41.0.0 tag. So the "41+" boundary is correct.
  - Before that change, `DhFormulaColumn.isStateless()` with `STATELESS_SELECT_BY_DEFAULT=false` still returned true for formulas with only immutable query-scope parameters and stateless input columns (`Arrays.stream(params).allMatch(DhFormulaColumn::isImmutableType) && usedColumns...isUsedColumnStateless`). So pure column math was already parallelizable in 40.
  - Before 41, only condition filters used `STATELESS_FILTERS_BY_DEFAULT` (then `false`). Other `WhereFilter`s default to `permitParallelization() == true`.
- **Fix:** something like "Deephaven 40 and earlier treated formulas as stateful unless the engine could prove them stateless, and treated condition filters as stateful."

### 6. The "Query table configuration" link doesn't cover these properties (L126)

- **What the doc claims:** "See Query table configuration for details on these and other engine configuration properties," referring to `statelessSelectByDefault` and `statelessFiltersByDefault`.
- **What the page has:** at f2ef483084, `docs/python/conceptual/query-table-configuration.md` lists parallel-where, parallel-select and parallel-sort properties. It mentions neither of these nor `serialSelectImplicitBarriers`.
- **Fix:** document them there, or reword so the link only promises the thresholds and parallel toggles.

### 7. The implicit-barriers default depends on another setting, and the doc doesn't say so (L320–325)

- `QueryTable.java:400-402`: `getBooleanWithDefault("QueryTable.serialSelectImplicitBarriers", !STATELESS_SELECT_BY_DEFAULT)`.
- So the property is `false` by default only while `statelessSelectByDefault=true`. A user who follows L126 and sets `statelessSelectByDefault=false` silently turns implicit barriers on.
- The doc also presents "Stateless mode" and "Stateful mode" as if they were setting values. The property is a boolean.
- The `ConcurrencyControl.withSerial` javadoc says the property is "defaulting to the value of `QueryTable.statelessSelectByDefault`". That contradicts the code, which defaults to the negation (and `QueryTableSelectBarrierTest.testPropertyDefaults` asserts `false`). This is a source javadoc bug to report separately, not something to copy into the doc.
- Implicit barriers apply only to selectables. For filters, `withSerial` is always "an absolute reordering barrier" (same javadoc), so "serial operations" at L320 should say "serial selectables."

### 8. Sort is only parallelized during initialization; the doc implies update cycles too (L66, L87)

- **What the doc claims:** L87 says updates parallelize "across rows and across columns just like during initialization."
- **What the source shows:** `SortHelpers.parallelizableOperationInitializer()` (`SortHelpers.java:1131-1143`) says: "Update graph refresh threads have the poisoned ExecutionContext … a sort listener running there sorts serially."
- Row splitting in `select`/`update` during updates is also skipped when the update has shifts (`SelectColumnLayer.java:203`, `!hasShifts`).
- **Fix:** qualify the sort bullet as initial computation only, and soften "just like during initialization."

### 9. "Serialization processes rows one at a time, in order, on a single thread" (L148) goes beyond the contract

- The contract (`ConcurrencyControl.withSerial`) promises only "never be invoked concurrently with itself" and "Rows are evaluated sequentially in row set order."
- It doesn't promise a single thread. L213's "only one thread processes the column at a time" is the accurate paraphrase, so use that wording at L148 as well.
- "Without it, parallel execution produces incorrect results" should be "can produce."

---

## Low: small gaps and qualifiers

10. **"What gets parallelized" leaves some operations out (L62–66).**
    - `update_by` uses `OperationInitializerJobScheduler` and `UpdateGraphJobScheduler` (`UpdateBy.java:309, 337-338`).
    - `range_join` initialization does too (`RangeJoinOperation.java:259`).
    - Snapshots read columns in parallel (`ConstructSnapshot.java:1456`, `QueryTable.enableParallelSnapshot`).
    - `update_by` is the most user-relevant of these; consider adding it or saying the list isn't complete.
11. **Statelessness is assumed, not detected (L93).**
    - "By default, Deephaven parallelizes operations that are stateless" reads as if the engine checks. With the defaults, it simply assumes every formula and condition filter is stateless.
    - The doc also defines stateless more strictly than the source. `SelectColumn.isStateless()` javadoc: "one row does not depend on the order of evaluation for another row."
12. **`statelessFiltersByDefault` only affects condition (formula) filters (L126).** The `WhereFilter.permitParallelization()` javadoc and default say other filters default to `true`.
13. **The `updateThreads` default is a core count (L50).** "Greater than 1, which is the default": the default is `-1`, which becomes `availableProcessors()` (`PeriodicUpdateGraph.java:140-144`). Say "on a multi-core machine, which the default ensures."
14. **Barrier rules left out (L240).**
    - A respected barrier must already have been declared by an earlier column or filter, reading left to right. Otherwise the engine throws "Respected barrier, …, is not defined" (`SelectAndViewAnalyzer.java:198-200`).
    - Constant columns can't declare or respect barriers (`SelectAndViewAnalyzer.java:224-230`).
    - The examples happen to follow these rules, but the rules aren't stated.
15. **"A and B run in parallel" (L314) should be "can run in parallel."** Running them together needs a thread pool with more than one thread (`OperationInitializationThreadPool.canParallelize()`).
16. **Partition filters have more conditions (L329–331).**
    - To be prioritized, a filter must also not use `i`/`ii`/`k`, must not be a reindexing filter, and must not be refreshing (`PartitionAwareSourceTable.isPrioritizablePartitioningFilter`, L422-427).
    - A serial filter also sends every filter after it to row-level evaluation (L144-157).
17. **The `select`/`update` threshold has an exception (L124).** The note is correct for ordinary columns, but columns whose result type is `Table` or `RowSet` ignore the minimum (`SelectColumnLayer.java:126-131, 204`).

---

## Checked and correct

- **Property names and defaults:**

  | Property | Default | Source |
  | --- | --- | --- |
  | `QueryTable.minimumParallelSortRows` | `1L << 20` | QueryTable.java:352-353 |
  | `QueryTable.parallelSort` | `true` | QueryTable.java:343-344 |
  | `QueryTable.minimumParallelSelectRows` | `1L << 22` | QueryTable.java:336-337 |
  | `QueryTable.parallelWhereRowsPerSegment` | `1 << 16` | QueryTable.java:275-276 |
  | `QueryTable.statelessSelectByDefault` | `true` | QueryTable.java:387-388 |
  | `QueryTable.statelessFiltersByDefault` | `true` | QueryTable.java:378-379 |
  | `OperationInitializationThreadPool.threads` | `-1` (becomes the core count) | `ThreadHelpers.getOrComputeThreadCountProperty` |
  | `PeriodicUpdateGraph.updateThreads` | `-1` (becomes the core count) | PeriodicUpdateGraph.java |

  The `where` threshold math (`numberOfRows / 2 > PARALLEL_WHERE_ROWS_PER_SEGMENT`) is also correct.
- **`with_serial` can't be used with `view`/`update_view` (L156):** correct under the default setting. `QueryTable.java:2055-2063` throws "A stateful column cannot safely be used in a view or updateView." when `STATELESS_SELECT_BY_DEFAULT` is true. Respected barriers are rejected in views regardless of settings (L2035-2037).
- **The Python GIL caveat's second paragraph (L77):** matches the `SelectColumn.isParallelizable` javadoc.
- **Python API:** `Selectable.parse`, `with_serial`, `with_declared_barriers` and `with_respected_barriers` exist (`table.py:140-235`), as do the same methods on `Filter` (`filters.py:55-106`), `is_null`/`not_`, and `deephaven.concurrency_control.Barrier`. `update`, `select` and `where` accept `Selectable`/`Filter` objects or sequences of them.
- **Barrier and filter-serial semantics:** match the javadoc and the Python `Barrier` docstring. Duplicate declarations throw.
- **Query-language functions:** `randomGaussian(double, double)` and `randomInt(int, int)` exist in `io.deephaven.function.Random`. `' '` stays a `char` literal and isn't treated as a time literal (`TimeLiteralReplacedExpression`).
- **The 41 breaking change:** confirmed by `8ea55b6a1d`, first tagged in v41.0.0.
- **No Enterprise-only features and no invented performance numbers.** Every number in the doc traces to a source constant.

## Links

- **Relative links:** every one resolves at f2ef483084, including `Selectable.md`, `Filter.md` and `crash-course/parallelization.md`. Those three are added on the PR branch and don't exist at the current HEAD, so merge them together with this page.
- **Anchors:** `Selectable.md#with_serial`, `update.md#serial-execution`, `where.md#serial-execution` and the in-page anchors all exist.
- **Suggested change:** at L153 the method name `with_serial` links to `update.md#serial-execution`. `Selectable.md#with_serial` is the better target.
- **L126:** see finding 6; the link works, but the page doesn't have what the sentence promises.
- **External links to verify by hand:**
  - The Pydoc `concurrency_control` anchors. Both classes exist in `py/server/deephaven/concurrency_control.py`, and that module's docstring links back to this page's path.
  - The python.org free-threading how-to.
  - The Oracle `Runtime.availableProcessors()` javadoc.

## Duplicate-claim sweep (Python file)

| Claim | Where in this file |
| --- | --- |
| GIL "never concurrently" | L75; related L9, L345 |
| Race symptoms | L180–190, L244, L280 |
| "Single thread" | L148 only; L213 is correct |
| Sort parallelism | L66, L87 |
| Implicit-barrier default | L320–323 only |
| Config-page link | L126 only |

The Groovy sibling wasn't swept, per your scope. Findings 1–9 are about engine behavior, not the Python API, so expect the same issues there.
