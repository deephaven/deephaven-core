//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.PushdownFilterMatcher;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.StepClock;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.table.vectors.ColumnVectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.merge;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.time.DateTimeUtils.epochNanosToInstant;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Reproducer for <b>PD-034 (P1)</b> — {@code ReindexingFilter}s are not excluded from pushdown, and a fully-resolving
 * pushdown skips {@code ClockFilter.filter()}, which is load-bearing initialization rather than a pure predicate.
 * <p>
 * {@code PushdownFilterMatcher.canPushdownFilter} ({@code :145-150}) excludes {@code NoPredicatePushdown} but not
 * {@code ReindexingFilter}. {@code ClockFilter} ({@code select/ClockFilter.java:32-83}) passes every gate — one column,
 * no column arrays, default (false) virtual row variables, default parallelizable.
 * <p>
 * But {@code ClockFilter.filter()} <i>is</i> its initialization: it assigns {@code nanosColumnSource} from the table
 * being filtered and builds the range state that the per-cycle {@code run()} ({@code :141-147}) consumes. With a cached
 * data index on the clock column, {@code DataIndexPushdownManager} can fully resolve the filter (empty
 * {@code maybeMatch}) and the driver then never calls {@code filter()} at all
 * ({@code AbstractFilterExecution.java:417-422}). Worse, {@code pushdownDataIndex} runs a <i>copy</i> of the filter
 * against the <b>index table</b>, so whatever state does get built is built against the wrong table. The registered
 * update source then refreshes against uninitialized state — no rows are ever released, or {@code run()} throws.
 * <p>
 * This test mirrors {@code TestClockFilters.testUnsorted1} — the same data, clock, and stepping — and adds only a data
 * index on the clock column. Without the index the sequence releases 6, then 12, then 18 rows as the clock advances;
 * that is the oracle.
 * <p>
 * {@code UnsortedClockFilter} is used rather than {@code SortedClockFilter} because a filter that
 * {@code requiresSorting()} makes {@code QueryTable.where} sort and flatten internally
 * ({@code QueryTable.java:1411-1423}), producing a fresh table that carries no data index — which incidentally prevents
 * the pushdown path from being reached at all. The unsorted variant is applied directly to the table the caller
 * indexed.
 * <p>
 * Related contamination noted in the finding: {@code BasePushdownFilterContextImpl.computeFilterNullBehavior}
 * ({@code :183-200}) executes the <b>live</b> filter object against a one-row dummy null table to probe null behavior,
 * which for state-bearing-but-parallelizable filters initializes real state against the dummy table.
 * <p>
 * <b>Expected after a fix:</b> both tests below pass. <b>Today both fail</b>, demonstrating the bug.
 */
public class TestPD034 {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private Table testInput;
    private StepClock clock;
    private boolean savedUseDataIndex;

    /** Set by {@link #runSteps} — whether {@code ClockFilter.filter()} actually ran. */
    private boolean filterInitialized;

    @Before
    public void setUp() {
        final Table range = newTable(
                col("Timestamp",
                        epochNanosToInstant(1000L), epochNanosToInstant(2000L), epochNanosToInstant(3000L),
                        epochNanosToInstant(1000L), epochNanosToInstant(2000L), epochNanosToInstant(3000L)),
                intCol("Int", 1, 2, 3, 1, 2, 3));
        // Static input, exactly as TestClockFilters.testSorted1 builds it.
        testInput = merge(range, range, range);
        clock = new StepClock(1000L, 2000L, 3000L);
        savedUseDataIndex = QueryTable.USE_DATA_INDEX_FOR_WHERE;
    }

    @After
    public void tearDown() {
        QueryTable.USE_DATA_INDEX_FOR_WHERE = savedUseDataIndex;
    }

    /**
     * Root cause: a {@link ReindexingFilter} must not be pushdown-eligible, because pushdown may satisfy it entirely
     * and skip the {@code filter()} call it depends on for initialization.
     */
    @Test
    public void canPushdownFilterRejectsReindexingFilter() {
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);
        filter.init(testInput.getDefinition());

        assertFalse("a ReindexingFilter must not be pushed down: " + filter.getClass().getSimpleName(),
                PushdownFilterMatcher.canPushdownFilter(filter));
    }

    /**
     * End-to-end: the clock filter must release rows as the clock advances, whether or not a data index happens to
     * exist on the clock column.
     */
    @Test
    public void indexedClockFilterReleasesRowsLikeUnindexedOracle() {
        final int[][] oracle = runSteps(false);
        assertTrue("sanity: without an index the clock filter's filter() runs", filterInitialized);

        final int[][] actual = runSteps(true);
        assertTrue("ClockFilter.filter() is load-bearing initialization and must not be skipped by pushdown",
                filterInitialized);

        assertArrayEquals("initial result", oracle[0], actual[0]);
        assertArrayEquals("after clock step 1", oracle[1], actual[1]);
        assertArrayEquals("after clock step 2", oracle[2], actual[2]);
    }

    /**
     * Builds the sorted input, optionally indexes the clock column, applies a refreshing {@link SortedClockFilter}, and
     * captures the {@code Int} column after the initial filter and after each of two clock steps.
     */
    private int[][] runSteps(final boolean withDataIndex) {
        clock.reset();
        QueryTable.USE_DATA_INDEX_FOR_WHERE = withDataIndex;
        try {
            // UnsortedClockFilter does not require sorting, so where() applies it directly to this table -- the one
            // we indexed. SortedClockFilter would instead sort+flatten internally, producing a fresh table that
            // carries no data index, which quietly prevents the pushdown path from ever being reached.
            final Table input = testInput;
            if (withDataIndex) {
                // where() only uses fully-populated indexes (WhereListener.extractFilterDataIndexMap checks
                // tableIsCached()), so materialize it -- this is the finding's "cached data index" precondition.
                DataIndexer.getOrCreateDataIndex(input, "Timestamp").table();
            }

            final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);
            final Table result = input.where(filter);

            final List<int[]> captured = new ArrayList<>();
            // ClockFilter.filter() is its initialization: it assigns nanosColumnSource. If pushdown fully
            // resolved the filter, the driver never called it and the per-cycle run() has nothing to work with.
            filterInitialized = filter.nanosColumnSource != null;

            captured.add(ColumnVectors.ofInt(result, "Int").toArray());

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            for (int step = 0; step < 2; step++) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    clock.run();
                    filter.run();
                });
                captured.add(ColumnVectors.ofInt(result, "Int").toArray());
            }
            return captured.toArray(new int[0][]);
        } finally {
            QueryTable.USE_DATA_INDEX_FOR_WHERE = savedUseDataIndex;
        }
    }
}
