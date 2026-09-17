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

                    // A private copy, built key by key so it shares nothing, that is written in place.
                    try (final WritableRowSet actual = snapshot(target)) {
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
                        try (final WritableRowSet actual = snapshot(from)) {
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

    /**
     * A shared set must stay untouched even when the first inserted range is already contained, which returns the
     * shared set itself unchanged, and only a later range has anything to write.
     */
    @Test
    public void containedFirstRangeKeepsSharedCopyIsolated() {
        // The original is complete before the copy is taken, so the copy still shares its inner set when it is
        // written; an insert into either one before that point would already have split them.
        try (final WritableRowSet original = RowSetFactory.fromRange(10, 20)) {
            original.insert(30);
            try (final WritableRowSet snapshot = snapshot(original);
                    final WritableRowSet shared = original.copy();
                    final WritableRowSet added = RowSetFactory.fromKeys(15, 25)) {
                shared.insert(added);
                assertTrue(snapshot.equals(original));
                assertTrue(shared.containsRange(25, 25));
                assertEquals(original.size() + 1, shared.size());
            }
        }
    }

    /**
     * An inserted range that starts inside one of ours and reaches the next must coalesce all three into one range,
     * through both the planned and the individual paths, into private and shared sets alike.
     */
    @Test
    public void bridgingInsertCoalescesThroughToTheNextRange() {
        // Enough ranges past the bridge that a five-range argument takes the planned path on the private target.
        final RowSetBuilderSequential targetBuilder = RowSetFactory.builderSequential();
        targetBuilder.appendRange(48, 49);
        for (long key = 51; key < 51 + 2_000 * 4; key += 4) {
            targetBuilder.appendKey(key);
        }
        try (final WritableRowSet target = targetBuilder.build()) {
            for (final long[] keys : new long[][] {{50}, {50, 5_000, 5_004, 5_008, 5_012}}) {
                try (final WritableRowSet added = RowSetFactory.fromKeys(keys);
                        final WritableRowSet expected = snapshot(target)) {
                    added.forAllRowKeyRanges(expected::insertRange);
                    try (final WritableRowSet actual = snapshot(target)) {
                        actual.insert(added);
                        check(0, expected, actual);
                        assertTrue(actual.containsRange(48, 51));
                    }
                    try (final WritableRowSet snapshotBefore = snapshot(target);
                            final WritableRowSet shared = target.copy()) {
                        shared.insert(added);
                        check(0, expected, shared);
                        assertTrue(snapshotBefore.equals(target));
                    }
                }
            }
        }
    }

    /**
     * A planned removal whose ranges run past our last entry must stop once our entries are exhausted, leaving the
     * earlier cuts intact, in private and shared sets alike.
     */
    @Test
    public void removalPastTheLastEntryStops() {
        // Enough entries that five removed ranges take the planned path.
        final RowSetBuilderSequential targetBuilder = RowSetFactory.builderSequential();
        for (long key = 0; key < 2_000 * 4; key += 4) {
            targetBuilder.appendKey(key);
        }
        try (final WritableRowSet target = targetBuilder.build();
                final WritableRowSet removed = RowSetFactory.fromKeys(4, 8, 12, 7_996, 9_000);
                final WritableRowSet expected = snapshot(target)) {
            removed.forAllRowKeyRanges(expected::removeRange);
            assertEquals(target.size() - 4, expected.size());
            try (final WritableRowSet actual = snapshot(target)) {
                actual.remove(removed);
                check(0, expected, actual);
            }
            try (final WritableRowSet snapshotBefore = snapshot(target);
                    final WritableRowSet shared = target.copy()) {
                shared.remove(removed);
                check(0, expected, shared);
                assertTrue(snapshotBefore.equals(target));
            }
        }
    }

    /**
     * Two row sets sharing one inner {@code SortedRanges} hand the bulk operations their own receiver as the argument,
     * which the row set wrapper's own {@code == this} checks do not intercept. Inserting must change nothing and leave
     * the other wrapper untouched; it takes the individual path at two ranges and the merge at the larger sizes, since
     * the argument is as large as the set. Removing must empty the receiver through the identity guard and leave the
     * other wrapper untouched.
     */
    @Test
    public void aliasedArgumentThroughSharedInnerSet() {
        for (final int keys : new int[] {2, 40, 3_000}) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (long key = 0; key < keys * 4L; key += 4) {
                builder.appendKey(key);
            }
            try (final WritableRowSet set = builder.build();
                    final WritableRowSet snapshotBefore = snapshot(set)) {
                try (final WritableRowSet shared = set.copy()) {
                    shared.insert(set);
                    assertTrue("keys=" + keys, snapshotBefore.equals(shared));
                    assertTrue("keys=" + keys, snapshotBefore.equals(set));
                }
                try (final WritableRowSet shared = set.copy()) {
                    shared.remove(set);
                    assertTrue("keys=" + keys, shared.isEmpty());
                    assertTrue("keys=" + keys, snapshotBefore.equals(set));
                }
            }
        }
    }

    /**
     * An independent copy of {@code rowSet}, sharing no state with it. Inserting into an empty row set would instead
     * take a shared reference to the argument's inner set.
     */
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
