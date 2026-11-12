//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.DelegatingColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.proto.backplane.grpc.ColumnStatisticsRequest;
import io.deephaven.server.session.SessionState;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.instantCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ColumnStatisticsGrpcImplTest {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testDoubleStatsWithRetriedSnapshot() {
        final Table stats = statsWithRetriedSnapshot(
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), doubleCol("Value", 1.0, 2.0, 3.0)));
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertEquals(3L, stats.getColumnSource("SIZE").get(0));
        assertEquals(6.0, stats.getColumnSource("SUM").get(0));
        assertEquals(14.0, stats.getColumnSource("SUM_SQRD").get(0));
        assertEquals(1.0, stats.getColumnSource("MIN").get(0));
        assertEquals(3.0, stats.getColumnSource("MAX").get(0));
        assertEquals(2.0, stats.getColumnSource("AVG").get(0));
    }

    @Test
    public void testLongStatsWithRetriedSnapshot() {
        final Table stats = statsWithRetriedSnapshot(
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), longCol("Value", -1, 5, 2)));
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertEquals(6L, stats.getColumnSource("SUM").get(0));
        assertEquals(8L, stats.getColumnSource("SUM_ABS").get(0));
        assertEquals(30L, stats.getColumnSource("SUM_SQRD").get(0));
        assertEquals(-1L, stats.getColumnSource("MIN").get(0));
        assertEquals(5L, stats.getColumnSource("MAX").get(0));
    }

    /**
     * The sum, absolute sum and sum of squares all overflow {@code long} and fall back to {@code double}; powers of two
     * keep the expected values exact.
     */
    @Test
    public void testLongOverflowStatsWithRetriedSnapshot() {
        final long big = 1L << 62;
        final Table stats = statsWithRetriedSnapshot(
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), longCol("Value", big, big, -big)));
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertSame(double.class, stats.getColumnSource("SUM").getType());
        assertEquals(0x1p62, stats.getColumnSource("SUM").get(0));
        assertSame(double.class, stats.getColumnSource("SUM_ABS").getType());
        assertEquals(3 * 0x1p62, stats.getColumnSource("SUM_ABS").get(0));
        assertSame(double.class, stats.getColumnSource("SUM_SQRD").getType());
        assertEquals(3 * 0x1p124, stats.getColumnSource("SUM_SQRD").get(0));
        assertEquals(-big, stats.getColumnSource("MIN").get(0));
        assertEquals(big, stats.getColumnSource("MAX").get(0));
    }

    /**
     * Each square overflows the {@code int} multiplication, so the sum of squares falls back to {@code double} while
     * the sums stay {@code long}.
     */
    @Test
    public void testIntOverflowStatsWithRetriedSnapshot() {
        final int big = 1 << 16;
        final Table stats = statsWithRetriedSnapshot(
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), intCol("Value", big, big, -big)));
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertEquals((long) big, stats.getColumnSource("SUM").get(0));
        assertEquals(3L * big, stats.getColumnSource("SUM_ABS").get(0));
        assertSame(long.class, stats.getColumnSource("SUM").getType());
        assertSame(double.class, stats.getColumnSource("SUM_SQRD").getType());
        assertEquals(3 * 0x1p32, stats.getColumnSource("SUM_SQRD").get(0));
        assertEquals(-big, stats.getColumnSource("MIN").get(0));
        assertEquals(big, stats.getColumnSource("MAX").get(0));
    }

    @Test
    public void testBigDecimalStatsWithRetriedSnapshot() {
        final Table stats = statsWithRetriedSnapshot(TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("Value", BigDecimal.ONE, BigDecimal.valueOf(2), BigDecimal.valueOf(3))));
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertEquals(0, BigDecimal.valueOf(6).compareTo((BigDecimal) stats.getColumnSource("SUM").get(0)));
        assertEquals(0, BigDecimal.ONE.compareTo((BigDecimal) stats.getColumnSource("MIN").get(0)));
        assertEquals(0, BigDecimal.valueOf(3).compareTo((BigDecimal) stats.getColumnSource("MAX").get(0)));
    }

    @Test
    public void testInstantStatsWithRetriedSnapshot() {
        final Table stats = statsWithRetriedSnapshot(TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                instantCol("Value", Instant.ofEpochSecond(200), Instant.ofEpochSecond(100), null)));
        assertEquals(2L, stats.getColumnSource("COUNT").get(0));
        assertEquals(3L, stats.getColumnSource("SIZE").get(0));
        assertEquals(Instant.ofEpochSecond(100), stats.getColumnSource("MIN").get(0));
        assertEquals(Instant.ofEpochSecond(200), stats.getColumnSource("MAX").get(0));
    }

    @Test
    public void testStringStatsWithRetriedSnapshot() {
        final Table stats = statsWithRetriedSnapshot(
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), stringCol("Value", "a", "b", "a")));
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertEquals(2, stats.getColumnSource("UNIQUE_VALUES").get(0));
    }

    /**
     * A snapshot taken during an update cycle, before the table is satisfied, must use the values from before the
     * cycle.
     */
    @Test
    public void testDoubleStatsFromPreviousValues() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final QueryTable base =
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), doubleCol("Value", 1.0, 2.0, 3.0));
        final AtomicInteger prevReads = new AtomicInteger();
        final AtomicInteger currentReads = new AtomicInteger();
        final QueryTable table = withReadListener(base,
                usePrev -> (usePrev ? prevReads : currentReads).incrementAndGet());

        final Table stats;
        updateGraph.startCycleForUnitTests(false);
        try {
            addToTable(base, i(2, 4, 6), doubleCol("Value", 10.0, 20.0, 30.0));
            stats = computeStats(table);
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }

        assertTrue("previous values were read", prevReads.get() > 0);
        assertEquals(0, currentReads.get());
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertEquals(6.0, stats.getColumnSource("SUM").get(0));
        assertEquals(1.0, stats.getColumnSource("MIN").get(0));
        assertEquals(3.0, stats.getColumnSource("MAX").get(0));
    }

    /**
     * The first attempt reads previous values during an update cycle and is invalidated when that cycle completes, so
     * the retry reads the current values and must not accumulate onto the first attempt's statistics.
     */
    @Test
    public void testDoubleStatsRetriedFromPreviousValues() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final QueryTable base =
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), doubleCol("Value", 1.0, 2.0, 3.0));
        final AtomicInteger prevReads = new AtomicInteger();
        final AtomicInteger currentReads = new AtomicInteger();
        final boolean[] cycleOpen = {false};
        final QueryTable table = withReadListener(base, usePrev -> {
            (usePrev ? prevReads : currentReads).incrementAndGet();
            if (cycleOpen[0]) {
                cycleOpen[0] = false;
                updateGraph.markSourcesRefreshedForUnitTests();
                updateGraph.completeCycleForUnitTests();
            }
        });

        final Table stats;
        updateGraph.startCycleForUnitTests(false);
        cycleOpen[0] = true;
        try {
            addToTable(base, i(2, 4, 6), doubleCol("Value", 10.0, 20.0, 30.0));
            stats = computeStats(table);
        } finally {
            if (cycleOpen[0]) {
                updateGraph.markSourcesRefreshedForUnitTests();
                updateGraph.completeCycleForUnitTests();
            }
        }

        assertTrue("previous values were read", prevReads.get() > 0);
        assertTrue("current values were read on the retry", currentReads.get() > 0);
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertEquals(60.0, stats.getColumnSource("SUM").get(0));
        assertEquals(10.0, stats.getColumnSource("MIN").get(0));
        assertEquals(30.0, stats.getColumnSource("MAX").get(0));
    }

    /**
     * Compute the column statistics of {@code base}'s {@code Value} column, completing an update cycle during the first
     * read of the column so that the first concurrent snapshot attempt is inconsistent and must be retried.
     */
    private static Table statsWithRetriedSnapshot(@NotNull final QueryTable base) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final AtomicInteger chunksRead = new AtomicInteger();
        final QueryTable table = withReadListener(base, usePrev -> {
            if (chunksRead.getAndIncrement() == 0) {
                updateGraph.runWithinUnitTestCycle(() -> {
                });
            }
        });
        final Table stats = computeStats(table);
        assertTrue("snapshot was retried, chunksRead=" + chunksRead.get(), chunksRead.get() > 1);
        return stats;
    }

    private static Table computeStats(@NotNull final Table table) {
        final ColumnStatisticsGrpcImpl impl =
                new ColumnStatisticsGrpcImpl(new TableServiceContextualAuthWiring.AllowAll());
        return impl.create(ColumnStatisticsRequest.newBuilder().setColumnName("Value").build(),
                List.of(SessionState.wrapAsExport(table)));
    }

    @FunctionalInterface
    private interface ReadListener {
        void beforeRead(boolean usePrev);
    }

    /**
     * Make a refreshing table that is a child of {@code base} and whose {@code Value} column calls {@code listener}
     * before each chunk read.
     */
    private static QueryTable withReadListener(@NotNull final QueryTable base, @NotNull final ReadListener listener) {
        final QueryTable table = new QueryTable(base.getRowSet(),
                Map.of("Value", listeningSource(base.getColumnSource("Value"), listener)));
        table.setRefreshing(true);
        table.addParentReference(base);
        return table;
    }

    private static <T> ColumnSource<T> listeningSource(
            @NotNull final ColumnSource<T> delegate,
            @NotNull final ReadListener listener) {
        return new DelegatingColumnSource<>(delegate.getType(), delegate.getComponentType(), delegate) {
            @Override
            public Chunk<? extends Values> getChunk(
                    @NotNull final GetContext context,
                    @NotNull final RowSequence rowSequence) {
                listener.beforeRead(false);
                return super.getChunk(context, rowSequence);
            }

            @Override
            public Chunk<? extends Values> getPrevChunk(
                    @NotNull final GetContext context,
                    @NotNull final RowSequence rowSequence) {
                listener.beforeRead(true);
                return super.getPrevChunk(context, rowSequence);
            }

            @Override
            protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                    @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
                return listeningSource(super.doReinterpret(alternateDataType), listener);
            }
        };
    }
}
