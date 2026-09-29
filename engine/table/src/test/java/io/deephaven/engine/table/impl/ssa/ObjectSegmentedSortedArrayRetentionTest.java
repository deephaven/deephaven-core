//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

/**
 * An Object {@link SegmentedSortedArray} holds references only to the stamps it currently contains, so a removed stamp
 * becomes unreachable once the caller drops it. Small leaves exercise leaf splits, merges, whole leaf removal and the
 * return to a single leaf.
 */
public class ObjectSegmentedSortedArrayRetentionTest {
    private static final int NODE_SIZE = 4;
    private static final int STEPS = 400;

    private static final class Entry {
        private final String value;
        private final long rowKey;

        private Entry(final String value, final long rowKey) {
            this.value = value;
            this.rowKey = rowKey;
        }
    }

    @Test
    public void testAscendingRemovedStampsAreReleased() throws InterruptedException {
        checkRemovedStampsAreReleased(false);
    }

    @Test
    public void testDescendingRemovedStampsAreReleased() throws InterruptedException {
        checkRemovedStampsAreReleased(true);
    }

    private static void checkRemovedStampsAreReleased(final boolean reverse) throws InterruptedException {
        final Comparator<String> valueOrder = reverse ? Comparator.reverseOrder() : Comparator.naturalOrder();
        final Comparator<Entry> ssaOrder =
                Comparator.<Entry, String>comparing(entry -> entry.value, valueOrder)
                        .thenComparingLong(entry -> entry.rowKey);

        final SegmentedSortedArray ssa = reverse ? new ObjectReverseSegmentedSortedArray(NODE_SIZE)
                : new ObjectSegmentedSortedArray(NODE_SIZE);
        final TreeSet<Entry> live = new TreeSet<>(ssaOrder);
        final List<WeakReference<String>> removedValues = new ArrayList<>();
        runSteps(ssa, live, removedValues, ssaOrder);

        for (int attempt = 0; attempt < 100 && countReachable(removedValues) > 0; ++attempt) {
            System.gc();
            Thread.sleep(10);
        }
        assertEquals("removed stamps still reachable", 0, countReachable(removedValues));
        assertEquals(live.size(), ssa.size());
    }

    /**
     * Applies random insertions and removals. Running them in a separate frame leaves no local variable holding a
     * removed stamp once they are done.
     */
    private static void runSteps(final SegmentedSortedArray ssa, final TreeSet<Entry> live,
            final List<WeakReference<String>> removedValues, final Comparator<Entry> ssaOrder) {
        final Random random = new Random(0);
        long nextRowKey = 0;

        for (int step = 0; step < STEPS; ++step) {
            final TreeSet<Entry> toInsert = new TreeSet<>(ssaOrder);
            final int insertCount = random.nextInt(12);
            for (int ii = 0; ii < insertCount; ++ii) {
                // equal values are held in distinct instances, so each row's stamp is reachable only through that row
                toInsert.add(new Entry(new String("v" + random.nextInt(50)), nextRowKey++));
            }
            apply(ssa, toInsert, true);
            live.addAll(toInsert);

            final TreeSet<Entry> toRemove = new TreeSet<>(ssaOrder);
            // occasionally remove nearly everything, so that leaves merge and the array returns to a single leaf
            final int removeCount = step % 25 == 24 ? Math.max(0, live.size() - 2) : random.nextInt(12);
            final List<Entry> candidates = new ArrayList<>(live);
            for (int ii = 0; ii < removeCount && !candidates.isEmpty(); ++ii) {
                toRemove.add(candidates.remove(random.nextInt(candidates.size())));
            }
            apply(ssa, toRemove, false);
            live.removeAll(toRemove);
            for (final Entry entry : toRemove) {
                removedValues.add(new WeakReference<>(entry.value));
            }
            assertEquals(live.size(), ssa.size());
            final List<Long> expectedRowKeys = new ArrayList<>();
            live.forEach(entry -> expectedRowKeys.add(entry.rowKey));
            final List<Long> actualRowKeys = new ArrayList<>();
            ssa.forAllKeys(actualRowKeys::add);
            assertEquals(expectedRowKeys, actualRowKeys);
        }
    }

    private static void apply(final SegmentedSortedArray ssa, final TreeSet<Entry> entries, final boolean insert) {
        if (entries.isEmpty()) {
            return;
        }
        final Object[] values = new Object[entries.size()];
        final long[] rowKeys = new long[entries.size()];
        int position = 0;
        for (final Entry entry : entries) {
            values[position] = entry.value;
            rowKeys[position++] = entry.rowKey;
        }
        if (insert) {
            ssa.insert(ObjectChunk.chunkWrap(values), LongChunk.chunkWrap(rowKeys));
        } else {
            ssa.remove(ObjectChunk.chunkWrap(values), LongChunk.chunkWrap(rowKeys));
        }
    }

    private static int countReachable(final List<WeakReference<String>> references) {
        int reachable = 0;
        for (final WeakReference<String> reference : references) {
            if (reference.get() != null) {
                ++reachable;
            }
        }
        return reachable;
    }
}
