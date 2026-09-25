//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An {@link AsOfStampContext} grows its buffers as it is handed larger buckets. When growing a buffer fails, closing
 * the context releases every pooled chunk and context exactly once.
 */
public class AsOfStampContextTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final int CAPACITY_LIMIT = 8;
    private static final int LARGE_SIZE = 100;

    /**
     * An int column source whose fill contexts are limited in capacity, standing in for any failure (for example an
     * OutOfMemoryError) while the stamp context grows a buffer. Its fill contexts fail when closed twice.
     */
    private static final class CapacityLimitedIntSource extends ImmutableIntArraySource {
        private CapacityLimitedIntSource(final int[] data) {
            super(data);
        }

        @Override
        public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            if (chunkCapacity > CAPACITY_LIMIT) {
                throw new IllegalStateException("fill context capacity " + chunkCapacity + " exceeds "
                        + CAPACITY_LIMIT);
            }
            return new CloseOnceFillContext();
        }
    }

    private static final class CloseOnceFillContext implements ChunkSource.FillContext {
        private boolean closed;

        @Override
        public void close() {
            if (closed) {
                throw new AssertionError("fill context closed twice");
            }
            closed = true;
        }
    }

    private static int[] ascendingStamps() {
        return IntStream.range(0, LARGE_SIZE).toArray();
    }

    @Test
    public void testLeftBufferGrowthFailureReleasesOnce() {
        final ColumnSource<?> leftStamps = new CapacityLimitedIntSource(ascendingStamps());
        final ColumnSource<?> rightStamps = new ImmutableIntArraySource(ascendingStamps());
        checkGrowthFailure(leftStamps, rightStamps, true);
    }

    @Test
    public void testRightBufferGrowthFailureReleasesOnce() {
        final ColumnSource<?> leftStamps = new ImmutableIntArraySource(ascendingStamps());
        final ColumnSource<?> rightStamps = new CapacityLimitedIntSource(ascendingStamps());
        checkGrowthFailure(leftStamps, rightStamps, false);
    }

    /**
     * Buffers are sized to the smallest power of two that holds a bucket, so a bucket of exactly the capacity limit
     * fits in a fill context of that capacity.
     */
    @Test
    public void testPowerOfTwoBucketUsesExactCapacity() {
        final ColumnSource<?> leftStamps = new CapacityLimitedIntSource(ascendingStamps());
        final ColumnSource<?> rightStamps = new CapacityLimitedIntSource(ascendingStamps());
        final WritableRowRedirection rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(LARGE_SIZE);
        try (final WritableRowSet bucket = RowSetFactory.flat(CAPACITY_LIMIT);
                final AsOfStampContext stampContext =
                        new AsOfStampContext(SortingOrder.Ascending, false, leftStamps, rightStamps, rightStamps)) {
            stampContext.processEntry(bucket, bucket, rowRedirection);
        }
        for (int ii = 0; ii < CAPACITY_LIMIT; ++ii) {
            assertEquals(ii, rowRedirection.get(ii));
        }
        ChunkPoolReleaseTracking.check();
    }

    /**
     * A stamp whose comparison is not a total order: every stamp compares greater than every other.
     */
    private static final class AlwaysGreater implements Comparable<AlwaysGreater> {
        @Override
        public int compareTo(@NotNull final AlwaysGreater other) {
            return 1;
        }
    }

    /**
     * Stamps are compacted right after they are sorted, so a stamp that is still out of order means the stamp type does
     * not define a total order; the stamp context fails rather than stamping from a partly compacted chunk.
     */
    @Test
    public void testStampsThatAreNotTotallyOrderedFail() {
        final ColumnSource<?> stamps = new ImmutableObjectArraySource<>(AlwaysGreater.class, null,
                new Object[] {new AlwaysGreater(), new AlwaysGreater()});
        final WritableRowRedirection rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(2);
        try (final WritableRowSet rows = RowSetFactory.flat(2);
                final AsOfStampContext stampContext =
                        new AsOfStampContext(SortingOrder.Ascending, false, stamps, stamps, stamps)) {
            stampContext.processEntry(rows, rows, rowRedirection);
            fail("expected the out of order stamps to fail");
        } catch (final AssertionFailure expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("firstOutOfOrderPosition"));
        }
        ChunkPoolReleaseTracking.check();
    }

    private static void checkGrowthFailure(final ColumnSource<?> leftStamps, final ColumnSource<?> rightStamps,
            final boolean growLeft) {
        final WritableRowRedirection rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(LARGE_SIZE);
        try (final WritableRowSet small = RowSetFactory.flat(1);
                final WritableRowSet large = RowSetFactory.flat(LARGE_SIZE)) {
            final AsOfStampContext stampContext =
                    new AsOfStampContext(SortingOrder.Ascending, false, leftStamps, rightStamps, rightStamps);
            try {
                stampContext.processEntry(small, small, rowRedirection);
                try {
                    stampContext.processEntry(growLeft ? large : small, growLeft ? small : large, rowRedirection);
                    fail("expected the buffer growth to fail");
                } catch (final IllegalStateException expected) {
                    assertEquals("fill context capacity 128 exceeds " + CAPACITY_LIMIT, expected.getMessage());
                }
            } finally {
                stampContext.close();
            }
        }
        ChunkPoolReleaseTracking.check();
    }
}
