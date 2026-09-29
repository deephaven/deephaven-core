# Spot-check: added threshold sentence in `parallelization.md`

**File:** `docs/python/conceptual/query-engine/parallelization.md`, section "Within a single table" (concept guide)

**Sentence you added:**

> Deephaven only does this for tables with more than 1 million rows; smaller tables are computed on a single core.

**Verdict: Inaccurate.** The number is wrong, and so are the comparison and what gets counted. The "single core" half also overstates what the source guarantees.

Note: the working-tree copy of this file has no "Within a single table" section, so I checked the sentence as you supplied it, not a diff.

## What the source says

1. **The threshold is about 4.2 million rows, not 1 million.** In `engine/table/src/main/java/io/deephaven/engine/table/impl/QueryTable.java` (lines 333-337):

   > `Minimum select "chunk" size, defaults to 4 million.`
   > `MINIMUM_PARALLEL_SELECT_ROWS = Configuration.getInstance().getLongWithDefault("QueryTable.minimumParallelSelectRows", 1L << 22);`

   `1L << 22` is 4,194,304. It is a configurable property (`QueryTable.minimumParallelSelectRows`), not a fixed limit. The test config `props/test-configs/src/main/resources/dh-tests.prop:90` sets it to `1024`.

2. **The check is "at least", not "more than", and it counts rows in the update, not the table.** In `engine/table/src/main/java/io/deephaven/engine/table/impl/select/analyzers/SelectColumnLayer.java` (lines 192-205):

   > `final long totalSize = upstream.added().size() + upstream.modified().size();`
   > `... || totalSize >= QueryTable.MINIMUM_PARALLEL_SELECT_ROWS)) {`

   The number compared is the added rows plus the modified rows being processed. On initialization, that is the whole table (`QueryTable.java` builds a fake update with every row as `added`). On a ticking table, each cycle's update is checked separately. A 100-million-row table that ticks 10,000 rows per cycle processes those updates serially.

3. **"Smaller tables are computed on a single core" is too absolute.** Parallelism also depends on other conditions (same `if` at line 203): `canParallelizeThisColumn` (the column is not redirected, its destination supports parallel population, and the expression is stateless and parallelizable), `jobScheduler.threadCount() > 1`, and `!hasShifts`. Columns whose results are `Table` or `RowSet` values parallelize at any size above zero (`resultTypeIsTableOrRowSet && totalSize > 0`). Below the threshold the column is computed serially within that layer. The source does not promise one core for the whole operation, because independent column layers and other DAG nodes can still run concurrently.

4. **Related, affecting the sentence before yours:** above the threshold, each group has at least `MINIMUM_PARALLEL_SELECT_ROWS` rows (`divisionSize = Math.max(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS, ceil(totalSize / threadCount))`, lines 206-208). A 5-million-row table is therefore split into only two groups, not one group per core. Your threshold sentence will be read together with the one before it, so the pair should not suggest that every core gets a group.

## Suggested replacement

> Deephaven only splits the work this way when the rows being processed (the whole table on initialization, or the added and modified rows in an update) number at least `QueryTable.minimumParallelSelectRows`, which defaults to 4,194,304 (`1L << 22`). Smaller batches are computed serially. See [Parallel processing with select](../query-table-configuration.md#parallel-processing-with-select) to change this threshold.

If the concept guide should stay lighter, a shorter version:

> By default, Deephaven parallelizes this work only when at least about 4 million rows (`QueryTable.minimumParallelSelectRows`) are being processed at once; smaller initializations and updates are computed serially.

Check the relative link path from `conceptual/query-engine/` before you merge. The target `docs/python/conceptual/query-table-configuration.md` exists, and it has a "Parallel processing with select" section.

## Style (changed line only)

- The suggested text uses straight quotes, no dotted method names, and active voice. No other style issues in the added sentence.

## Duplicate check

- **Same file:** `parallelization.md` has no other threshold or "million" claim. Its "Parallelizing query initialization" section (line 26) says only "When possible", so nothing conflicts.
- **Cross-language sibling:** `docs/groovy/conceptual/query-engine/parallelization.md` also lacks the "Within a single table" section. If you add the section there too, apply the same fix.
- **Related claim elsewhere:** `docs/python/conceptual/query-table-configuration.md:114` and its Groovy twin (`docs/groovy/conceptual/query-table-configuration.md:114`) say parallelism starts once "the parent's size exceeds" the threshold. The code uses `>=` on the added and modified rows, not the parent's size. This is outside the edit you asked about, but it is the same claim, so a full `deephaven-core-accuracy-check` of `query-table-configuration.md` (both languages) is worth doing.
- The claim is not part of an enumerated list.
