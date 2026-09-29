# Accuracy review: `docs/python/conceptual/query-engine/parallelization.md`

I reviewed the snapshot at `.claude/skills/deephaven-core-accuracy-check/evals/files/parallelization-pre-review.md`. It is byte-identical to that path at `f2ef483084`. I checked it against the current source checkout at `c68a9a9be8`.

**Category:** Concept guide (`conceptual/`). Its sidebar spot under Best practices → Performance is there for discoverability and doesn't change the category. The Concept-guide rules on where configuration detail belongs therefore apply.

**Scope:** Python file only. A Groovy sibling exists at `docs/groovy/conceptual/query-engine/parallelization.md`. You ruled it out of scope, so I didn't check it, but it very likely repeats the shared claims flagged below (the version-40 claim, the view/update_view classification, implicit barriers, the counter table).

I made no edits.

---

## 1. Incorrect or overstated claims

### 1.1 The "Deephaven 40 and earlier" callout overstates the old default (line 9)

The doc says: "Deephaven 40 and earlier assumed all formulas required sequential processing by default."

- The default did flip in `8ea55b6a1d` (DH-20714, #7331). The first tag containing that commit is v41.0.0, so the version number is right.
- Before that commit, `QueryTable.statelessSelectByDefault` defaulted to `false`. But `DhFormulaColumn.isStateless()` still returned true for most formulas:

  ```java
  if (QueryTable.STATELESS_SELECT_BY_DEFAULT) { return true; }
  return Arrays.stream(params).allMatch(DhFormulaColumn::isImmutableType)
          && usedColumns.stream().allMatch(this::isUsedColumnStateless) ...
  ```

  So pure column math like `Total = Price * Quantity` was already stateless and parallelizable in 40. Only formulas that used mutable query-scope parameters (Python callables, mutable objects) were treated as stateful.
- Filters followed a similar pattern. Before the change, only `ConditionFilter.permitParallelization()` returned `STATELESS_FILTERS_BY_DEFAULT` (`false`). Match and range filters used the `WhereFilter` default of `true`.
- **Fix:** Say that 40 treated formulas that reference query-scope variables or functions, and string condition filters, as stateful. 41 treats all formulas and filters as stateless unless they are marked serial.

### 1.2 The "What does NOT get parallelized" list gets `view`, `update_view` and `lazy_update` wrong (line 70)

- All three build a `ViewColumnSource` (`AbstractFormulaColumn.getViewColumnSource`). Their formulas run later, on whatever thread reads the column.
- If the reader is a parallel `update`/`select`, a parallel `where`, or a chunked downstream read, the formula runs concurrently on those threads. `view`/`update_view` formulas can also run again on every read of a row.
- The engine itself treats these as unsafe for stateful formulas. `QueryTable.viewOrUpdateView` throws `"A stateful column cannot safely be used in a view or updateView."` when `STATELESS_SELECT_BY_DEFAULT` is on. A few lines earlier it throws `"view and updateView cannot respect barriers"`.
- "Not computed upfront" is true. Listing these under "not parallelized" gives readers the wrong idea, because it implies they are safe for stateful code.
- "Upfront" is also ambiguous: static creation, refreshing-table initialization, or update cycles?
- **Fix:** Take them out of the "not parallelized" list. Say they compute nothing when the table is created, and their formulas may be evaluated concurrently, and repeatedly, by whatever reads them.

### 1.3 `sort` is parallelized only during initialization (line 66)

- `SortHelpers.parallelizableOperationInitializer()` returns null on update-graph refresh threads (`PoisonedOperationInitializer`). Its javadoc says: "a sort listener running there sorts serially."
- It also returns null whenever `canParallelize()` is false.
- The bullet reads as if `sort` is always parallel above the size threshold. It isn't: incremental sort updates are serial.

### 1.4 The GIL callout says Python code is "never run concurrently", which is too strong (line 75)

- For selectables, `isParallelizable()` gates only splitting rows inside one column. See `SelectColumnLayer`: `canParallelizeThisColumn = ... && sc.isStateless() && sc.isParallelizable()`.
- Running columns in parallel is gated only on statelessness. `SelectColumnLayer.allowCrossColumnParallelization()` returns `selectColumn.isStateless()`, and `anyParallelColumns()` picks the parallel job scheduler.
- So on a GIL build, two Python-backed columns in the same `update` can still run on different pool threads at the same time. Under the GIL they interleave rather than run truly in parallel, but a shared `counter += 1` can still race.
- Independent tables whose formulas call Python also update concurrently on the update-graph pool.
- The gating claim for filters matches source: `ConditionFilter.permitParallelization()` returns false when Python is used and the build isn't free-threaded.
- **Fix:** Limit the claim to "a single Python-backed column or filter isn't split across threads." Don't imply Python code is protected from concurrency.

### 1.5 The counter example's sample output contradicts the doc's own model (lines 175–190)

- **"B not following A + 1" assumes the wrong model.** Each column is its own layer and evaluates all of its rows. Even with fully serial execution, B would not be A+1. The doc's own barrier example (line 280) says "Column A gets values 0–9. Column B gets values 10–19."
- **"Out-of-order values (row 4 has A=4 after row 3 has A=5)" needs row splitting inside column A.** On a GIL build, `DhFormulaColumn.isParallelizable()` returns false for formulas that use Python callables, so column A is not split. This clashes with the GIL callout directly above it (see 1.4).
- **Only a free-threaded build reproduces that within-column disorder.** Splitting also requires `totalSize >= MINIMUM_PARALLEL_SELECT_ROWS` (4,194,304). Chunks are `max(MINIMUM_PARALLEL_SELECT_ROWS, ceil(total/threads))`, so 5M rows splits into just two chunks: 4,194,304 rows and 805,696 rows.
- **"Gaps (no 10–19 visible)" doesn't mean anything in a five-row sample.**
- **The "fix" example (line 209) changes the code.** It drops column B and uses a single `ID` column, so it doesn't show how to fix the A/B case it just described.

### 1.6 The barrier example's "would race" statements don't happen as written (lines 244, 280)

- `empty_table(10).update([col_a, col_b])` has two serial columns and no stateless ones. So `anyParallelColumns()` is false and `QueryTable` uses `ImmediateJobScheduler`, which runs the layers one after another in order.
- Removing the barrier would therefore not "both read `counter = 0`" in practice. The barrier matters because nothing *guarantees* A runs before B, per `ConcurrencyControl.withSerial` javadoc: "no additional ordering between selectable expressions is imposed". It is not because a race is observable here.
- "Without `with_serial`, rows within each column would also race" doesn't happen at 10 rows either. That is far below the 4,194,304-row split threshold, and on a GIL build Python formulas aren't split anyway.
- **Fix:** Say what is and isn't guaranteed, not what "would" happen.

### 1.7 "Concurrency control works the same way for `Filter` as it does for `Selectable`" (line 135)

It doesn't. From the `ConcurrencyControl.withSerial` javadoc:

- For a filter, "serial acts as an absolute reordering barrier."
- For selectables, ordering between expressions depends on `SERIAL_SELECT_IMPLICIT_BARRIERS`, which is off by default.

The doc itself says at line 234 that a serial filter "cannot be reordered with respect to other filters", which contradicts line 135.

### 1.8 The implicit-barriers section (lines 320–323)

- It names the Java field (`QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS`) as if it were the setting. The actual property is `QueryTable.serialSelectImplicitBarriers`.
- The default isn't fixed. The code is `getBooleanWithDefault("QueryTable.serialSelectImplicitBarriers", !STATELESS_SELECT_BY_DEFAULT)`. Setting `statelessSelectByDefault=false` turns implicit barriers on unless you override it. The "Stateless mode (default)" and "Stateful mode" labels describe this property alone and leave out that dependency.
- The quick-reference row "Multiple operations sharing state → Barriers or implicit barriers" (line 21) offers implicit barriers without saying they're off by default.
- Separately, the source contradicts itself. The `ConcurrencyControl.java` javadoc says the property defaults "to the value of `QueryTable.statelessSelectByDefault`", but the code uses the negation. The code is what runs, so the doc's "default false" is correct. The javadoc should be fixed as a separate follow-up.

### 1.9 Absolute statements weakened by the doc's own caveats

- **Line 83:** "Deephaven computes that operation's initial result ... dividing the rows among CPU cores." That only happens above the size thresholds, which the doc itself gives at line 124.
- **Line 50:** "`PeriodicUpdateGraph.updateThreads` being greater than 1, which is the default." The default is `-1`, meaning `availableProcessors()`, so it is 1 on a single-core host.
- **Lines 345 and 9:** "Deephaven assumes all formulas can run in parallel by default." On a GIL build, Python-backed formulas are never split across rows (`isParallelizable()` returns false).
- **Line 124:** The threshold is compared against the rows in the current change (added + modified; `SelectColumnLayer`: `totalSize = upstream.added().size() + upstream.modified().size()`), not the table size. It is also skipped when the update has shifts. Columns returning a Table or RowSet split at any size.

## 2. Incomplete lists

The "What gets parallelized" list (lines 62–66) leaves out other engine paths that use the operation-initializer or update-graph job schedulers:

- `update_by` (`UpdateBy.java`)
- range join (`RangeJoinOperation.java`)
- snapshots that read columns in parallel (`ConstructSnapshot`, gated by `QueryTable.enableParallelSnapshot` / `minimumParallelSnapshotRows`)

The doc doesn't need to list them all, but as written it looks exhaustive.

The barrier section also leaves out two rules the engine enforces:

- A respected barrier must be declared by an operation earlier in the same list. Otherwise `SelectAndViewAnalyzer` throws `"Respected barrier, ..., is not defined for ..."`.
- `view`/`update_view` can't respect barriers. The line 156 callout mentions only `with_serial`.

## 3. Configuration detail in the narrative (Concept guide)

Property names and defaults appear in the middle of explanations at:

- Line 50 (`updateThreads`)
- Line 66 (`minimumParallelSortRows`, `parallelSort`)
- Lines 83 and 87 (thread-pool properties)
- Line 124 (three threshold properties with values)
- Lines 320–323

For a Concept guide, rewrite these at a higher level ("once the table is large enough", "uses all cores by default"). Move the property names and values to one Configuration section at the end of the page, or to the configuration reference.

**The link to Query table configuration (line 126) needs checking.** It claims that page covers "these and other" properties. At `f2ef483084`/current, `query-table-configuration.md` has no `statelessSelectByDefault`, `serialSelectImplicitBarriers`, `parallelSort`, `minimumParallelSortRows`, or thread-pool properties. So the configuration section has to be added somewhere; the link alone doesn't cover it.

Separate follow-up: that page gives two defaults for `statelessFiltersByDefault`, `false` in its summary table (line 42) and `true` at line 226. The source default is `true`.

## 4. Claims confirmed against source

- **`with_serial`, `with_declared_barriers`, `with_respected_barriers`:** exist on `Selectable` (`deephaven.table`) and `Filter` (`deephaven.filters`). Both accept a single barrier or a list.
- **`Selectable.parse`:** is a classmethod.
- **`Barrier()`:** takes no arguments (`deephaven.concurrency_control`).
- **`is_null` and `not_`:** exist in `deephaven.filters`.
- **Python `view`, `update_view`, `lazy_update`:** accept only strings.
- **`update`, `select`, `where`:** accept `Selectable` / `Filter` objects.
- **Serial guarantees (lines 136, 213, 234):** match the `ConcurrencyControl.withSerial` javadoc: "never be invoked concurrently with itself", "Rows are evaluated sequentially in row set order", "absolute reordering barrier" for filters.
- **"Each barrier can only be declared by one operation":** javadoc says "declared by at most one filter".
- **Default of `statelessSelectByDefault` and `statelessFiltersByDefault`:** `true`.
- **`minimumParallelSelectRows`:** `1L << 22`, which is 4,194,304 ("about 4.2 million").
- **`minimumParallelSortRows`:** `1L << 20`.
- **`parallelSort`:** default `true`.
- **`parallelWhereRowsPerSegment`:** `1 << 16`. The check `numberOfRows / 2 > PARALLEL_WHERE_ROWS_PER_SEGMENT` matches "more than twice ... about 131,072".
- **Thread pools:** `OperationInitializationThreadPool.threads` and `PeriodicUpdateGraph.updateThreads` both default to `-1`, which maps to `Runtime.availableProcessors()`. `updateThreads > 1` selects `ConcurrentNotificationProcessor`.
- **Implicit barriers:** each serial column respects the earlier serial columns' barriers (`SelectAndViewAnalyzer`), so "execute one after the other" is correct when enabled.
- **Line 77:** "may still evaluate a non-parallelizable column out of order" matches the `SelectColumn.isParallelizable` javadoc.
- **Python free-threading check:** `PythonFreeThreadUtil` (version string contains "free-threading").
- **`with_serial` can't be used with `view`/`update_view` (line 156):** the engine raises an error under the default `statelessSelectByDefault=true`.
- **Partition filters (lines 329–331):**
  - `isPrioritizablePartitioningFilter` doesn't check statelessness.
  - Once a serial filter is seen, all later filters, including partition filters, go to the post-coalesce set.
  - The engine also stops prioritizing partition filters that come after *any* earlier serial filter. The doc doesn't mention this.
- **Example code:**
  - Using `i` on a `time_table` is allowed because the table is append-only (`validateSafeForRefresh`).
  - `' '` in `FirstName + ' ' + LastName` is a single-character literal, which `TimeLiteralReplacedExpression` skips, so the expression is valid.
  - `randomGaussian` and `randomInt` exist in `io.deephaven.function.Random`.

## 5. Links

**Valid internal links.** All of these exist at `f2ef483084`:

- `Selectable.md`, `Filter.md`, and the crash-course `parallelization.md` were added by that PR; they're missing from the current tree.
- `select.md`, `update.md`, `where.md`, `sort.md`, `view.md`, `update-view.md`, `lazy-update.md`, `../dag.md`, `../query-table-configuration.md`, `./engine-locking.md`.

**Anchors that resolve:**

- `Selectable.md#with_serial` (`### with_serial`)
- `update.md#serial-execution`
- `where.md#serial-execution`

**Suggestions:**

- **Barrier and ConcurrencyControl links (lines 137, 153, 240, 356):** these point to external pydoc. Internal reference pages exist in the repo: `docs/python/reference/query-language/types/Barrier.md` and `ConcurrencyControl.md`. Point to those.
- **Line 153:** the `with_serial` *method* links to `update.md#serial-execution`. `ConcurrencyControl.md#with_serial` is the method's own reference.

**External links to check by hand:**

- Python free-threading howto
- JDK 11 `Runtime.availableProcessors`
- The two pydoc URLs

## 6. Author queries

- **AQ1 [Key concepts, line 135]:** Should this say filters and selectables behave the same for barriers but differ for `with_serial` ordering? (See 1.7.)
- **AQ2 [Serialization example]:** Should the counter example be recast as a single column and demonstrated on free-threaded Python? Or drop the sample output table in favor of describing what is *not guaranteed*? (See 1.5.)
- **AQ3 [What gets parallelized]:** Is this list meant to be exhaustive? If it is, `update_by`, range join and snapshots belong in it. (See section 2.)

## 7. Follow-ups outside this file

- The Groovy sibling probably repeats 1.1, 1.2, 1.5, 1.7 and 1.8. It was out of scope here but needs the same pass.
- The `ConcurrencyControl.java` javadoc gives the wrong default for `serialSelectImplicitBarriers` (see 1.8).
- `query-table-configuration.md` gives two different defaults for `statelessFiltersByDefault`, and it doesn't cover the select/sort/implicit-barrier properties.
