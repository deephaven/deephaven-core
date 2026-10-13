//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

/**
 * {@link SegmentedSortedArray#insertAndGetNextValue} reports, for each inserted stamp, the stamp that follows it in the
 * array after the insertion, and how many inserted stamps have one; only the last inserted stamp can lack a successor.
 */
public class SegmentedSortedArrayNextValueTest {
    private static final int SEEDS = 20;
    private static final int STEPS = 50;

    private static final class Entry {
        private final long value;
        private final long rowKey;

        private Entry(final long value, final long rowKey) {
            this.value = value;
            this.rowKey = rowKey;
        }
    }

    @Test
    public void testNextValuesMatchSuccessors() {
        for (final boolean reverse : new boolean[] {false, true}) {
            for (final int nodeSize : new int[] {2, 3, 8, 64}) {
                for (int seed = 0; seed < SEEDS; ++seed) {
                    checkNextValues(reverse, nodeSize, new Random(seed));
                }
            }
        }
    }

    private static void checkNextValues(final boolean reverse, final int nodeSize, final Random random) {
        final Comparator<Entry> order = Comparator.<Entry>comparingLong(entry -> reverse ? -entry.value : entry.value)
                .thenComparingLong(entry -> entry.rowKey);
        final SegmentedSortedArray ssa = reverse ? new LongReverseSegmentedSortedArray(nodeSize)
                : new LongSegmentedSortedArray(nodeSize);
        final TreeSet<Entry> contents = new TreeSet<>(order);
        long nextRowKey = 0;
        long appendValue = 1000;

        for (int step = 0; step < STEPS; ++step) {
            if (!contents.isEmpty() && random.nextInt(5) == 0) {
                // remove some entries so that inserts land in leaves of varying occupancy
                final TreeSet<Entry> removed = new TreeSet<>(order);
                for (final Entry entry : contents) {
                    if (random.nextInt(3) == 0) {
                        removed.add(entry);
                    }
                }
                final long[] removedValues = new long[removed.size()];
                final long[] removedRowKeys = new long[removed.size()];
                int removedPosition = 0;
                for (final Entry entry : removed) {
                    removedValues[removedPosition] = entry.value;
                    removedRowKeys[removedPosition++] = entry.rowKey;
                }
                ssa.remove(LongChunk.chunkWrap(removedValues), LongChunk.chunkWrap(removedRowKeys));
                contents.removeAll(removed);
            }

            final TreeSet<Entry> batch = new TreeSet<>(order);
            final int batchSize = 1 + random.nextInt(3 * nodeSize);
            final int pattern = random.nextInt(3);
            for (int ii = 0; ii < batchSize; ++ii) {
                final long value;
                if (pattern == 0) {
                    // past the end of the array in its sort order
                    appendValue += 1 + random.nextInt(3);
                    value = reverse ? -appendValue : appendValue;
                } else if (pattern == 1) {
                    value = random.nextInt(2000) - 1000;
                } else {
                    // few distinct values, so runs of equal stamps break ties by row key
                    value = random.nextInt(4);
                }
                batch.add(new Entry(value, nextRowKey++));
            }

            final long[] values = new long[batch.size()];
            final long[] rowKeys = new long[batch.size()];
            int position = 0;
            for (final Entry entry : batch) {
                values[position] = entry.value;
                rowKeys[position++] = entry.rowKey;
            }
            contents.addAll(batch);

            try (final WritableLongChunk<Values> nextValues = WritableLongChunk.makeWritableChunk(values.length)) {
                final int found = ssa.insertAndGetNextValue(LongChunk.chunkWrap(values), LongChunk.chunkWrap(rowKeys),
                        nextValues);
                int expectedFound = 0;
                for (final Entry entry : batch) {
                    final Entry successor = contents.higher(entry);
                    if (successor == null) {
                        break;
                    }
                    assertEquals("next value of stamp " + entry.value + " row " + entry.rowKey, successor.value,
                            nextValues.get(expectedFound));
                    ++expectedFound;
                }
                assertEquals(expectedFound, found);
            }
            assertEquals(contents.size(), ssa.size());
        }
    }
}
