//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Bulk insertion of one {@link SortedRanges} into another must produce the same set as inserting the added ranges one
 * at a time, for every way the added ranges can meet the existing ones: apart, adjacent on either side, overlapping,
 * contained, bridging several ranges, before the first entry, after the last, and coalescing with each other through an
 * existing range; whether the target is shared, has spare capacity, or must grow; and for every packing.
 */
public class SortedRangesBulkInsertTest {

    private static final int TRIALS = 20_000;

    @Test
    public void bulkInsertMatchesIndividualInserts() {
        final Random random = new Random(1);
        int sortedRangesTargets = 0;
        for (int trial = 0; trial < TRIALS; ++trial) {
            // Three packings: shorts over a narrow key space, ints over a wider one, longs over one wider than int.
            final long base;
            final long span;
            switch (trial % 3) {
                case 0:
                    base = random.nextInt(1 << 20);
                    span = 1 + random.nextInt(20_000);
                    break;
                case 1:
                    base = random.nextInt(1 << 20);
                    span = 1 + random.nextInt(1 << 26);
                    break;
                default:
                    base = 1L << 40;
                    span = (1L << 33) + random.nextInt(1 << 20);
                    break;
            }
            final int targetRanges = random.nextInt(60);
            final int addedRanges = 1 + random.nextInt(12);
            try (final WritableRowSet target = randomSet(random, base, span, targetRanges, 1 + random.nextInt(8));
                    final WritableRowSet added = randomSetNear(random, target, base, span, addedRanges)) {
                if (target.isEmpty() || added.isEmpty()) {
                    continue;
                }
                final OrderedLongSet targetInner = ((WritableRowSetImpl) target).getInnerSet();
                if (targetInner instanceof SortedRanges) {
                    ++sortedRangesTargets;
                }

                try (final WritableRowSet expected = target.copy()) {
                    added.forAllRowKeyRanges(expected::insertRange);

                    // A private copy that is written in place.
                    try (final WritableRowSet actual = RowSetFactory.empty()) {
                        actual.insert(target);
                        actual.insert(added);
                        check(trial, expected, actual);
                    }

                    // A shared copy: the insert must leave the original untouched. The snapshot is built key by key
                    // so it shares nothing with the original and still holds its prior contents if the original is
                    // wrongly written in place.
                    try (final WritableRowSet snapshot = snapshot(target);
                            final WritableRowSet shared = target.copy()) {
                        shared.insert(added);
                        check(trial, expected, shared);
                        assertTrue("trial " + trial + " original changed", snapshot.equals(target));
                    }
                }

                // Removal: the same ranges taken out of the union, which cuts existing ranges every way they can be
                // cut, and out of the original, where most of them miss or clip.
                for (final WritableRowSet from : new WritableRowSet[] {expectedUnion(target, added), target}) {
                    try (final WritableRowSet expected = from.copy()) {
                        added.forAllRowKeyRanges(expected::removeRange);
                        try (final WritableRowSet actual = RowSetFactory.empty()) {
                            actual.insert(from);
                            actual.remove(added);
                            check(trial, expected, actual);
                        }
                        try (final WritableRowSet snapshot = snapshot(from);
                                final WritableRowSet shared = from.copy()) {
                            shared.remove(added);
                            check(trial, expected, shared);
                            assertTrue("trial " + trial + " original changed", snapshot.equals(from));
                        }
                    }
                    if (from != target) {
                        from.close();
                    }
                }
            }
        }
        assertTrue("too few SortedRanges targets: " + sortedRangesTargets, sortedRangesTargets > TRIALS / 2);
    }

    /** An independent copy of {@code rowSet}, sharing no state with it. */
    private static WritableRowSet snapshot(final WritableRowSet rowSet) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        rowSet.forAllRowKeyRanges(builder::appendRange);
        return builder.build();
    }

    private static WritableRowSet expectedUnion(final WritableRowSet target, final WritableRowSet added) {
        final WritableRowSet union = target.copy();
        added.forAllRowKeyRanges(union::insertRange);
        return union;
    }

    private static void check(final int trial, final WritableRowSet expected, final WritableRowSet actual) {
        assertEquals("trial " + trial, expected.size(), actual.size());
        assertTrue("trial " + trial + " expected=" + expected + " actual=" + actual, expected.equals(actual));
        final OrderedLongSet inner = ((WritableRowSetImpl) actual).getInnerSet();
        if (inner instanceof SortedRanges) {
            ((SortedRanges) inner).validate();
        }
    }

    /** A set of about {@code ranges} ranges of up to {@code maxRunLength} keys each, within [base, base + span). */
    private static WritableRowSet randomSet(final Random random, final long base, final long span, final int ranges,
            final int maxRunLength) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        for (int i = 0; i < ranges; ++i) {
            final long start = base + (long) (random.nextDouble() * span);
            final long end = start + random.nextInt(maxRunLength);
            builder.addRange(start, end);
        }
        return builder.build();
    }

    /**
     * A set whose ranges are placed relative to {@code target}'s keys so that adjacency, overlap and containment happen
     * often: each range starts at a random target key plus a small signed offset, or anywhere when the target is empty.
     */
    private static WritableRowSet randomSetNear(final Random random, final WritableRowSet target, final long base,
            final long span, final int ranges) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        for (int i = 0; i < ranges; ++i) {
            long start;
            if (target.isEmpty() || random.nextInt(4) == 0) {
                start = base + (long) (random.nextDouble() * span);
            } else {
                final long anchor = target.get(random.nextInt((int) target.size()));
                start = anchor + random.nextInt(7) - 3;
            }
            if (start < 0) {
                start = 0;
            }
            if (random.nextInt(50) == 0) {
                // A range reaching the last key, which has no key after it to compare against.
                builder.addRange(start, Long.MAX_VALUE);
                continue;
            }
            final int runLength = random.nextInt(10) == 0 ? random.nextInt(40) : random.nextInt(3);
            builder.addRange(start, start + runLength);
        }
        return builder.build();
    }
}
