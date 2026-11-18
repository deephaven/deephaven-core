//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.megamerge;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.LongMegaMergeKernel;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

/**
 * Object-specific behavior of the descending {@link ObjectLongMegaMergeDescendingKernel}, beyond the replicated
 * {@link TestObjectLongMegaMerge}.
 */
public class TestObjectLongMegaMergeDescendingKernel {
    /**
     * A Comparable whose compareTo returns Integer.MIN_VALUE or Integer.MAX_VALUE for unequal values, which the
     * Comparable contract allows (only the sign is specified).
     */
    private static final class ExtremeComparable implements Comparable<ExtremeComparable> {
        private final int value;

        private ExtremeComparable(final int value) {
            this.value = value;
        }

        @Override
        public int compareTo(final ExtremeComparable other) {
            return value < other.value ? Integer.MIN_VALUE : value > other.value ? Integer.MAX_VALUE : 0;
        }

        @Override
        public String toString() {
            return Integer.toString(value);
        }
    }

    /**
     * The descending Object mega-merge orders values by the reverse of compareTo for any compareTo result, as the
     * descending Object timsort kernel does: merging the descending run [5] into the descending run [9, 1] produces [9,
     * 5, 1].
     */
    @Test
    public void descendingObjectMegaMergeHandlesMinValueCompareTo() {
        final LongArraySource destinationKeys = new LongArraySource();
        final WritableColumnSource<ExtremeComparable> destinationValues =
                ArrayBackedColumnSource.getMemoryColumnSource(0, ExtremeComparable.class);
        final LongMegaMergeKernel<Any, Indices> kernel =
                LongMegaMergeKernel.makeContext(ChunkType.Object, SortingOrder.Descending);

        try (final WritableLongChunk<Indices> keys1 = WritableLongChunk.makeWritableChunk(2);
                final WritableObjectChunk<Object, Any> values1 = WritableObjectChunk.makeWritableChunk(2);
                final WritableLongChunk<Indices> keys2 = WritableLongChunk.makeWritableChunk(1);
                final WritableObjectChunk<Object, Any> values2 = WritableObjectChunk.makeWritableChunk(1)) {
            keys1.set(0, 9);
            values1.set(0, new ExtremeComparable(9));
            keys1.set(1, 1);
            values1.set(1, new ExtremeComparable(1));
            kernel.merge(destinationKeys, destinationValues, 0, 0, keys1, values1);

            keys2.set(0, 5);
            values2.set(0, new ExtremeComparable(5));
            kernel.merge(destinationKeys, destinationValues, 0, 2, keys2, values2);
        }

        final long[] merged = new long[3];
        for (int ii = 0; ii < merged.length; ++ii) {
            merged[ii] = destinationKeys.getLong(ii);
        }
        assertArrayEquals("descending mega-merge of [9, 1] and [5]; got " + Arrays.toString(merged),
                new long[] {9, 5, 1}, merged);
    }
}
