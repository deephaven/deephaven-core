//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.remote;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.ForkJoinPoolOperationInitializer;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Tables and fixtures for exercising the serial and parallel column snapshot paths of {@link ConstructSnapshot}, by
 * intercepting the fill of one column so that a test can make it fail or block.
 */
final class ColumnSnapshotTestSupport {

    /**
     * Comfortably longer than the default {@code ConstructSnapshot.maxConcurrentAttemptDurationMillis} of 5 seconds.
     */
    static final long ATTEMPT_OVERRUN_MILLIS = 5_500;

    private static final int COLUMNS = 4;
    private static final int ROWS = 16;
    private static final int INTERCEPTED_COLUMN_INDEX = 1;

    /** The column of {@link #tableWithInterceptedColumn} whose fill runs the test's {@code beforeFill}. */
    static final String INTERCEPTED_COLUMN_NAME = "C" + INTERCEPTED_COLUMN_INDEX;

    private ColumnSnapshotTestSupport() {}

    /**
     * An {@link IntegerArraySource} that runs {@code beforeFill} ahead of every chunk fill, so that a test can make a
     * column fail or block.
     */
    private static final class InterceptingIntegerArraySource extends IntegerArraySource {

        private final Runnable beforeFill;

        private InterceptingIntegerArraySource(@NotNull final Runnable beforeFill) {
            this.beforeFill = beforeFill;
        }

        @Override
        public void fillChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            beforeFill.run();
            super.fillChunk(context, destination, rowSequence);
        }

        @Override
        public void fillPrevChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            beforeFill.run();
            super.fillPrevChunk(context, destination, rowSequence);
        }
    }

    /**
     * A static table of int columns named {@code C0..Cn}, where filling {@link #INTERCEPTED_COLUMN_NAME} runs
     * {@code beforeFill} first. It has more than one non-empty column, which a snapshot requires before it will
     * consider collecting them in parallel.
     */
    static QueryTable tableWithInterceptedColumn(@NotNull final Runnable beforeFill) {
        return tableWithInterceptedColumn(beforeFill, false);
    }

    /**
     * @see #tableWithInterceptedColumn(Runnable)
     */
    static QueryTable tableWithInterceptedColumn(
            @NotNull final Runnable beforeFill,
            final boolean refreshing) {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        for (int ci = 0; ci < COLUMNS; ++ci) {
            final IntegerArraySource source = ci == INTERCEPTED_COLUMN_INDEX
                    ? new InterceptingIntegerArraySource(beforeFill)
                    : new IntegerArraySource();
            source.ensureCapacity(ROWS, false);
            for (int ri = 0; ri < ROWS; ++ri) {
                source.set(ri, ri * (ci + 1));
            }
            columns.put("C" + ci, source);
        }
        final QueryTable table = new QueryTable(RowSetFactory.flat(ROWS).toTracking(), columns);
        if (refreshing) {
            table.setRefreshing(true);
        }
        return table;
    }

    /**
     * Run {@code snapshotAction} with the parallel column snapshot forced on for tables of any size, failing if this
     * JVM cannot take that path at all: a test that silently ran the serial path instead would be asserting nothing.
     */
    static void withParallelColumnSnapshot(@NotNull final Runnable snapshotAction) {
        assertTrue("The common ForkJoinPool cannot parallelize (parallelism="
                + ForkJoinPoolOperationInitializer.fromCommonPool().parallelismFactor()
                + "), so the parallel column snapshot path is unreachable here",
                ForkJoinPoolOperationInitializer.fromCommonPool().canParallelize());
        withColumnSnapshotParallelism(true, 1, snapshotAction);
    }

    /**
     * Run {@code snapshotAction} with the parallel column snapshot forced off.
     */
    static void withSerialColumnSnapshot(@NotNull final Runnable snapshotAction) {
        withColumnSnapshotParallelism(false, Long.MAX_VALUE, snapshotAction);
    }

    /**
     * Both settings are consulted before a snapshot parallelizes, so a test that needs one path or the other has to pin
     * both: the deployment defaults these tests would otherwise inherit are configurable.
     */
    private static void withColumnSnapshotParallelism(
            final boolean enableParallel,
            final long minimumParallelRows,
            @NotNull final Runnable snapshotAction) {
        final boolean enabled = QueryTable.ENABLE_PARALLEL_SNAPSHOT;
        final long minimumRows = QueryTable.MINIMUM_PARALLEL_SNAPSHOT_ROWS;
        QueryTable.ENABLE_PARALLEL_SNAPSHOT = enableParallel;
        QueryTable.MINIMUM_PARALLEL_SNAPSHOT_ROWS = minimumParallelRows;
        try {
            snapshotAction.run();
        } finally {
            QueryTable.ENABLE_PARALLEL_SNAPSHOT = enabled;
            QueryTable.MINIMUM_PARALLEL_SNAPSHOT_ROWS = minimumRows;
        }
    }
}
