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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertEquals;
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
    public void testStringStatsWithRetriedSnapshot() {
        final Table stats = statsWithRetriedSnapshot(
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), stringCol("Value", "a", "b", "a")));
        assertEquals(3L, stats.getColumnSource("COUNT").get(0));
        assertEquals(2, stats.getColumnSource("UNIQUE_VALUES").get(0));
    }

    /**
     * Compute the column statistics of {@code base}'s {@code Value} column, completing an update cycle during the first
     * read of the column so that the first concurrent snapshot attempt is inconsistent and must be retried.
     */
    private static Table statsWithRetriedSnapshot(@NotNull final QueryTable base) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final AtomicInteger chunksRead = new AtomicInteger();
        final ColumnSource<?> interrupting = interruptingSource(base.getColumnSource("Value"), () -> {
            if (chunksRead.getAndIncrement() == 0) {
                updateGraph.runWithinUnitTestCycle(() -> {
                });
            }
        });
        final QueryTable table = new QueryTable(base.getRowSet(), Map.of("Value", interrupting));
        table.setRefreshing(true);
        table.addParentReference(base);

        final ColumnStatisticsGrpcImpl impl =
                new ColumnStatisticsGrpcImpl(new TableServiceContextualAuthWiring.AllowAll());
        final Table stats = impl.create(ColumnStatisticsRequest.newBuilder().setColumnName("Value").build(),
                List.of(SessionState.wrapAsExport(table)));
        assertTrue("snapshot was retried, chunksRead=" + chunksRead.get(), chunksRead.get() > 1);
        return stats;
    }

    private static <T> ColumnSource<T> interruptingSource(
            @NotNull final ColumnSource<T> delegate,
            @NotNull final Runnable beforeRead) {
        return new DelegatingColumnSource<>(delegate.getType(), delegate.getComponentType(), delegate) {
            @Override
            public Chunk<? extends Values> getChunk(
                    @NotNull final GetContext context,
                    @NotNull final RowSequence rowSequence) {
                beforeRead.run();
                return super.getChunk(context, rowSequence);
            }

            @Override
            public Chunk<? extends Values> getPrevChunk(
                    @NotNull final GetContext context,
                    @NotNull final RowSequence rowSequence) {
                beforeRead.run();
                return super.getPrevChunk(context, rowSequence);
            }
        };
    }
}
