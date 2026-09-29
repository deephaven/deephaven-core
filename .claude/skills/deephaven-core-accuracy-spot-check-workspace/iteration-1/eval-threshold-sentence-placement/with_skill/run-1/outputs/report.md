# Spot-check: added sentence in the parallelization concept guide

**File:** `docs/python/conceptual/query-engine/parallelization.md` (Concept guide), section "Within a single table"

**Added sentence:**

> Deephaven only does this for tables with more than 1 million rows; smaller tables are computed on a single core.

**Scope note:** The "Within a single table" section isn't in the checked-out copy of the file (or in its Groovy sibling), so I checked the sentence as written in `evals/files/added-sentence.md`, along with the sentence before it that it depends on.

## Verdict: inaccurate. Rewrite it without the number instead of just fixing the number.

### What source says

The threshold is `QueryTable.minimumParallelSelectRows` in `engine/table/src/main/java/io/deephaven/engine/table/impl/QueryTable.java` (lines 334-337):

```java
/** Minimum select "chunk" size, defaults to 4 million. */
public static long MINIMUM_PARALLEL_SELECT_ROWS =
        Configuration.getInstance().getLongWithDefault("QueryTable.minimumParallelSelectRows", 1L << 22);
```

`props/configs/src/main/resources/dh-defaults.prop` doesn't override it. The only override is in the test config (`dh-tests.prop:90`, `=1024`). The check that uses it is in `engine/table/src/main/java/io/deephaven/engine/table/impl/select/analyzers/SelectColumnLayer.java` (lines 190-207):

```java
final long totalSize = upstream.added().size() + upstream.modified().size();
...
if (canParallelizeThisColumn && jobScheduler.threadCount() > 1 && !hasShifts &&
        ((resultTypeIsTableOrRowSet && totalSize > 0)
                || totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS)) {
```

### Problems with the sentence

1. **Wrong number.** The default is `1L << 22` = 4,194,304 rows (about 4 million), not 1 million. The 1 million figure (`1L << 20`) belongs to other settings, `QueryTable.minimumParallelSortRows` and `QueryTable.minimumParallelSnapshotRows`, not to `select`/`update`.
2. **Wrong comparison.** The code uses `>=`, not "more than".
3. **"Tables" is the wrong unit.** The count is the rows in the current update (`added + modified`), not the size of the table. At initialization the engine passes every row as one fake "added" update (`QueryTable.java` ~1818-1821), so there the count is the table size. On later ticks, a small update to a huge table still runs on one thread.
4. **"Only" is an absolute, and it has exceptions.** Columns whose result type is a `Table` or `RowSet` are split across threads for any non-empty update (`resultTypeIsTableOrRowSet && totalSize > 0`, with a division size of 1). Size isn't the only condition either. The column must also be stateless and parallelizable (`sc.isStateless() && sc.isParallelizable()`, line 117), not redirected, and support parallel population. There must be more than one thread, and the update can't contain shifts. Parallel select/update must also be enabled at all (`QueryTable.enableParallelSelectAndUpdate`, `QueryTable.java:1825` and `SelectOrUpdateListener.java:68-72`).
5. **"A single core" is inaccurate.** Below the threshold the work runs on a single *thread* (`ImmediateJobScheduler` or the unsplit path). The engine doesn't manage CPU cores.

### Placement gate: don't swap in the correct number

Replacing "1 million" with "4,194,304 (`QueryTable.minimumParallelSelectRows`)" would be true, but it would put a property name and a default into Concept-guide narrative. That's the "configuration injection" pattern this guide was already reviewed for. Fixing all five points inline would also turn one sentence into a list of caveats. Write the sentence at the section's level of abstraction and send readers to the configuration reference for the exact value.

**Suggested replacement:**

> Deephaven splits the work this way only when there are enough rows for the split to pay off; smaller calculations run on a single thread. See [Parallel processing with `select`](../query-table-configuration.md#parallel-processing-with-select) for the threshold and how to tune it.

(If you'd rather not add a link here, the page's existing configuration area, the "Managing thread pool sizes" table, is the right place for `QueryTable.minimumParallelSelectRows` and its default of `1L << 22`.) The link target exists: `docs/python/conceptual/query-table-configuration.md` has the `## Parallel processing with \`select\`` heading and already lists this property with the correct default. The Groovy page has the same heading.

## Style (changed line only)

- The semicolon joins two independent clauses correctly, and the quotes and dashes are fine.
- The replacement above keeps method names without leading dots or parentheses and uses active voice.

## Duplicate check

- **In the file:** No other mention of "1 million", "single core", or `minimumParallelSelectRows` in the Python page. The `empty_table(1_000_000)` examples further down are only example data sizes and don't claim anything about the threshold. (Note: with the real 4M default, those 1M-row examples wouldn't be parallelized at all. That doesn't make them wrong, but keep it in mind if the page ever says those examples run in parallel.)
- **Groovy sibling** (`docs/groovy/conceptual/query-engine/parallelization.md`): No "Within a single table" section and no copy of this claim. If you add the section there too, use the same replacement.
- **Enumerated lists:** The sentence isn't part of an "N ways" list.
- **Related, outside the scope of this spot-check:** `query-table-configuration.md:114` (Python and Groovy) says parallelism starts once the parent's size "exceeds" the threshold. Source uses `>=` and counts rows per update, not the parent's size. It's worth fixing in a separate pass, but I didn't check the rest of that page.

No escalation to a full `deephaven-core-accuracy-check` is needed for this change. The claim appears only once.
