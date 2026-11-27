//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.megamerge;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.SparseArraySourceBlocks;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Merges into a destination run that starts beyond {@link Integer#MAX_VALUE}, preceded by a value that is not part of
 * the run. When the destination cursor passes the start of the run, the merge must stop taking destination values, so
 * the preceding value is neither merged into the run nor overwritten.
 */
public class TestMegaMergeBeyondIntegerMaxValue {
    private static final long RUN_START = (1L << 31) + 16;
    private static final long PRECEDING_KEY = 99;
    private static final long RUN_KEY = 100;

    @Test
    public void testAscending() {
        final LongArraySource keys = new LongArraySource();
        final LongArraySource values = new LongArraySource();
        prepare(keys, values, 15, 30);

        LongLongMegaMergeKernel.merge(keys, values, RUN_START, 1,
                LongChunk.<Indices>chunkWrap(new long[] {101, 102}),
                LongChunk.<Values>chunkWrap(new long[] {10, 20}));

        assertArrayEquals(new long[] {15, 10, 20, 30}, read(values));
        assertArrayEquals(new long[] {PRECEDING_KEY, 101, 102, RUN_KEY}, read(keys));
    }

    @Test
    public void testDescending() {
        final LongArraySource keys = new LongArraySource();
        final LongArraySource values = new LongArraySource();
        prepare(keys, values, 25, 10);

        LongLongMegaMergeDescendingKernel.merge(keys, values, RUN_START, 1,
                LongChunk.<Indices>chunkWrap(new long[] {101, 102}),
                LongChunk.<Values>chunkWrap(new long[] {30, 20}));

        assertArrayEquals(new long[] {25, 30, 20, 10}, read(values));
        assertArrayEquals(new long[] {PRECEDING_KEY, 101, 102, RUN_KEY}, read(keys));
    }

    /**
     * Puts a one-element destination run with row key {@code RUN_KEY} at {@code RUN_START}, preceded by
     * {@code precedingValue} with row key {@code PRECEDING_KEY}.
     */
    private static void prepare(final LongArraySource keys, final LongArraySource values,
            final long precedingValue, final long runValue) {
        SparseArraySourceBlocks.allocateOnlyBlockContaining(keys, RUN_START);
        SparseArraySourceBlocks.allocateOnlyBlockContaining(values, RUN_START);
        keys.set(RUN_START - 1, PRECEDING_KEY);
        values.set(RUN_START - 1, precedingValue);
        keys.set(RUN_START, RUN_KEY);
        values.set(RUN_START, runValue);
    }

    /** Reads the preceding row and the three rows of the merged run. */
    private static long[] read(final LongArraySource source) {
        final long[] result = new long[4];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = source.getLong(RUN_START - 1 + ii);
        }
        return result;
    }
}
