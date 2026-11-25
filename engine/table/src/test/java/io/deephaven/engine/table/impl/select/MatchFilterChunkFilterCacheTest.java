//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * A {@link MatchFilter} builds the chunk filter for its values once, and every {@link MatchFilter#filter} and
 * {@link MatchFilter#filterInverse} call, every copy and every rename of the filter matches with the built chunk
 * filter.
 */
public class MatchFilterChunkFilterCacheTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /** More than three values, so the chunk filter holds a set of the values. */
    private static final Object[] VALUES = {1, 3, 5, 7, 9};

    private static Table data() {
        return newTable(intCol("X", IntStream.range(0, 20).toArray()));
    }

    private static MatchFilter initialized(final Table table, final MatchOptions options) {
        final MatchFilter filter = new MatchFilter(options, "X", VALUES);
        filter.init(table.getDefinition());
        return filter;
    }

    @Test
    public void filterAndFilterInverseReuseTheBuiltChunkFilter() {
        final Table table = data();
        final MatchFilter filter = initialized(table, MatchOptions.REGULAR);
        final ChunkFilter built = filter.chunkFilter().orElseThrow();

        try (final WritableRowSet matched = filter.filter(table.getRowSet(), table.getRowSet(), table, false);
                final WritableRowSet matchedAgain =
                        filter.filter(table.getRowSet(), table.getRowSet(), table, false);
                final WritableRowSet unmatched =
                        filter.filterInverse(table.getRowSet(), table.getRowSet(), table, false)) {
            assertEquals(i(1, 3, 5, 7, 9), matched);
            assertEquals(matched, matchedAgain);
            assertEquals(table.getRowSet().minus(matched), unmatched);
        }
        assertSame(built, filter.chunkFilter().orElseThrow());
    }

    /**
     * A copy or rename made before the chunk filter is first built still shares the one that the original, or any other
     * copy, builds later.
     */
    @Test
    public void copiesAndRenamesShareTheChunkFilterBuiltLater() {
        final Table table = data();
        final MatchFilter filter = initialized(table, MatchOptions.REGULAR);
        final MatchFilter copy = (MatchFilter) filter.copy();
        final MatchFilter renamed = (MatchFilter) filter.renamedFilter(Map.of("X", "Y")).orElseThrow();

        final ChunkFilter builtByCopy = copy.chunkFilter().orElseThrow();
        assertSame(builtByCopy, filter.chunkFilter().orElseThrow());
        assertSame(builtByCopy, renamed.chunkFilter().orElseThrow());
        assertSame(builtByCopy, ((MatchFilter) renamed.copy()).chunkFilter().orElseThrow());
    }

    /**
     * Threads that first use the filter at the same time, as the parallel segments of a where do, all match with one
     * chunk filter and agree on the result.
     */
    @Test
    public void concurrentFirstUseBuildsOneChunkFilter() throws Exception {
        final int threads = 8;
        final Table table = TableTools.emptyTable(100_000).update("X = (int) (ii % 1000)");
        final Object[] values = IntStream.range(0, 500).boxed().toArray();
        final ExecutionContext context = ExecutionContext.getContext();
        for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED}) {
            final MatchFilter filter = new MatchFilter(options, "X", values);
            filter.init(table.getDefinition());
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                final CountDownLatch start = new CountDownLatch(1);
                final List<Future<WritableRowSet>> results = new ArrayList<>();
                for (int ti = 0; ti < threads; ++ti) {
                    final boolean inverse = ti % 2 == 1;
                    results.add(pool.submit(() -> {
                        start.await();
                        try (final SafeCloseable ignored = context.open()) {
                            return inverse
                                    ? filter.filterInverse(table.getRowSet(), table.getRowSet(), table, false)
                                    : filter.filter(table.getRowSet(), table.getRowSet(), table, false);
                        }
                    }));
                }
                start.countDown();
                final String matchFormula = options.inverted() ? "X >= 500" : "X < 500";
                try (final RowSet expected = table.where(matchFormula).getRowSet().copy();
                        final RowSet expectedInverse = table.getRowSet().minus(expected)) {
                    for (int ti = 0; ti < threads; ++ti) {
                        try (final WritableRowSet result = results.get(ti).get()) {
                            assertEquals(ti % 2 == 1 ? expectedInverse : expected, result);
                        }
                    }
                }
            } finally {
                pool.shutdownNow();
            }
            assertSame(filter.chunkFilter().orElseThrow(), ((MatchFilter) filter.copy()).chunkFilter().orElseThrow());
        }
    }

    /**
     * A where large enough to split into several segments, and the inverted match, agree with the equivalent formula.
     */
    @Test
    public void largeWhereMatchesTheFormula() {
        final Table table = TableTools.emptyTable(300_000).update("X = (int) (ii % 1000)");
        final Object[] values = IntStream.range(0, 500).filter(v -> v % 3 == 0).boxed().toArray();
        assertTableEquals(table.where("X < 500 && X % 3 == 0"),
                table.where(new MatchFilter(MatchOptions.REGULAR, "X", values)));
        assertTableEquals(table.where("!(X < 500 && X % 3 == 0)"),
                table.where(new MatchFilter(MatchOptions.INVERTED, "X", values)));
    }

    /**
     * A refreshing where matches added and modified rows on each cycle with the chunk filter built for its first one.
     */
    @Test
    public void refreshingWhereMatchesEachCycle() {
        final QueryTable source = testRefreshingTable(i(2, 4, 6, 8).toTracking(), intCol("X", 1, 2, 3, 4));
        final Table regular = source.where(new MatchFilter(MatchOptions.REGULAR, "X", VALUES));
        final Table inverted = source.where(new MatchFilter(MatchOptions.INVERTED, "X", VALUES));
        final String formula = "X == 1 || X == 3 || X == 5 || X == 7 || X == 9";
        assertTableEquals(source.where(formula), regular);
        assertTableEquals(source.where("!(" + formula + ")"), inverted);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(source, i(4, 10, 12), intCol("X", 5, 9, 10));
            removeRows(source, i(2));
            source.notifyListeners(i(10, 12), i(2), i(4));
        });
        assertTableEquals(source.where(formula), regular);
        assertTableEquals(source.where("!(" + formula + ")"), inverted);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(source, i(6, 8), intCol("X", 0, 7));
            source.notifyListeners(i(), i(), i(6, 8));
        });
        assertTableEquals(source.where(formula), regular);
        assertTableEquals(source.where("!(" + formula + ")"), inverted);
    }
}
