//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestFloatSegmentedSortedMultisetSpecialValues and run "./gradlew replicateSegmentedSortedMultisetTests"
// to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.vector.ObjectVectorDirect;

/**
 * Tests for {@link DoubleSegmentedSortedMultiset} behavior around the special double values that
 * {@link io.deephaven.util.compare.DoubleComparisons} treats specially: {@code -0.0d} (which compares equal to
 * {@code +0.0d}) and {@code NaN} (where any NaN bit pattern compares equal to any other NaN).
 *
 * <p>
 * The SSM's delta-tracking added/removed sets must agree with the SSM's leaf-storage equality (DoubleComparisons.eq). If
 * they disagree -- e.g. because the underlying hash set treats {@code -0.0d} and {@code +0.0d} as distinct, or keeps
 * two NaN bit patterns as separate entries -- then a net-no-change insert/remove pair can leak phantom adds/removes
 * through {@code fillAddedChunk}/{@code fillRemovedChunk}.
 */
public class TestDoubleSegmentedSortedMultisetSpecialValues extends RefreshingTableTestCase {

    private static final int NODE_SIZE = 64;

    private static DoubleSegmentedSortedMultiset trackingSsm() {
        final DoubleSegmentedSortedMultiset ssm = new DoubleSegmentedSortedMultiset(NODE_SIZE);
        ssm.setTrackDeltas(true);
        return ssm;
    }

    private static void insert(final DoubleSegmentedSortedMultiset ssm, final double[] values, final int[] counts) {
        try (final WritableDoubleChunk<Values> v = WritableDoubleChunk.makeWritableChunk(values.length);
                final WritableIntChunk<ChunkLengths> c = WritableIntChunk.makeWritableChunk(counts.length)) {
            v.setSize(values.length);
            c.setSize(counts.length);
            for (int i = 0; i < values.length; i++) {
                v.set(i, values[i]);
                c.set(i, counts[i]);
            }
            ssm.insert(v, c);
        }
    }

    private static void remove(final DoubleSegmentedSortedMultiset ssm, final double[] values, final int[] counts) {
        try (final WritableDoubleChunk<Values> v = WritableDoubleChunk.makeWritableChunk(values.length);
                final WritableIntChunk<ChunkLengths> c = WritableIntChunk.makeWritableChunk(counts.length)) {
            v.setSize(values.length);
            c.setSize(counts.length);
            for (int i = 0; i < values.length; i++) {
                v.set(i, values[i]);
                c.set(i, counts[i]);
            }
            ssm.remove(SegmentedSortedMultiSet.makeRemoveContext(NODE_SIZE), v, c);
        }
    }

    /**
     * Insert {@code -0.0d} then remove {@code +0.0d}. DoubleComparisons treats the two as equal, so the leaf empties out
     * AND the delta-tracking should recognize that the removal cancels the prior addition, leaving both added and
     * removed sets empty.
     */
    public void testInsertNegativeZeroThenRemovePositiveZero() {
        final DoubleSegmentedSortedMultiset ssm = trackingSsm();

        insert(ssm, new double[] {-0.0d}, new int[] {1});
        assertEquals(1L, ssm.totalSize());
        assertEquals(1, ssm.getAddedSize());
        assertEquals(0, ssm.getRemovedSize());

        remove(ssm, new double[] {+0.0d}, new int[] {1});
        assertEquals("Leaf should be empty after removing the (DoubleComparisons-equal) value",
                0L, ssm.totalSize());
        assertEquals("Insert(-0) then remove(+0) is a net no-change; added should be empty",
                0, ssm.getAddedSize());
        assertEquals("Insert(-0) then remove(+0) is a net no-change; removed should be empty",
                0, ssm.getRemovedSize());
    }

    /**
     * Insert {@code +0.0d} then remove {@code -0.0d} -- the mirror of the previous test. Same DoubleComparisons
     * semantics apply.
     */
    public void testInsertPositiveZeroThenRemoveNegativeZero() {
        final DoubleSegmentedSortedMultiset ssm = trackingSsm();

        insert(ssm, new double[] {+0.0d}, new int[] {1});
        assertEquals(1L, ssm.totalSize());
        assertEquals(1, ssm.getAddedSize());

        remove(ssm, new double[] {-0.0d}, new int[] {1});
        assertEquals(0L, ssm.totalSize());
        assertEquals(0, ssm.getAddedSize());
        assertEquals(0, ssm.getRemovedSize());
    }

    /**
     * Remove {@code +0.0d} from an SSM that contains {@code -0.0d} placed there outside of the tracked cycle. The
     * removal should match the existing entry and be recorded in {@code removed} exactly once.
     */
    public void testRemoveZeroAcrossSign() {
        final DoubleSegmentedSortedMultiset ssm = new DoubleSegmentedSortedMultiset(NODE_SIZE);
        // Seed the leaf without delta tracking.
        insert(ssm, new double[] {-0.0d}, new int[] {1});

        ssm.setTrackDeltas(true);
        remove(ssm, new double[] {+0.0d}, new int[] {1});
        assertEquals(0L, ssm.totalSize());
        assertEquals(0, ssm.getAddedSize());
        assertEquals("removed should record the single removal", 1, ssm.getRemovedSize());
    }

    /**
     * Insert two NaN values with distinct raw bit patterns in separate cycles. DoubleComparisons treats them as equal,
     * so the leaf must collapse them into a single entry with count 2 and the delta-tracking added set must contain
     * exactly one NaN entry.
     */
    public void testNaNsWithDifferentBitPatternsAreSameValue() {
        final DoubleSegmentedSortedMultiset ssm = trackingSsm();
        final double nanA = Double.NaN; // canonical 0x7ff8000000000000L
        final double nanB = Double.longBitsToDouble(0x7ff8000000000001L); // alternate NaN bit pattern
        assertTrue(Double.isNaN(nanA) && Double.isNaN(nanB));
        assertNotSame("NaN bit patterns must differ for this test to be meaningful",
                Double.doubleToRawLongBits(nanA), Double.doubleToRawLongBits(nanB));

        insert(ssm, new double[] {nanA}, new int[] {1});
        insert(ssm, new double[] {nanB}, new int[] {1});

        assertEquals("NaN-with-any-bits should occupy a single SSM entry with count 2", 2L, ssm.totalSize());
        assertEquals("Different NaN bit patterns are the same value to the delta tracker",
                1, ssm.getAddedSize());
        assertEquals(0, ssm.getRemovedSize());
    }

    /**
     * Insert one NaN bit pattern then remove a different NaN bit pattern. DoubleComparisons treats them as equal, so the
     * leaf empties and the delta tracking should net to no change.
     */
    public void testInsertOneNaNThenRemoveAnotherNaN() {
        final DoubleSegmentedSortedMultiset ssm = trackingSsm();
        final double nanA = Double.NaN;
        final double nanB = Double.longBitsToDouble(0x7ff8000000000001L);

        insert(ssm, new double[] {nanA}, new int[] {1});
        assertEquals(1L, ssm.totalSize());
        assertEquals(1, ssm.getAddedSize());

        remove(ssm, new double[] {nanB}, new int[] {1});
        assertEquals("Removing a NaN with different bits should empty the leaf", 0L, ssm.totalSize());
        assertEquals("Insert(NaN_A) then remove(NaN_B) is a net no-change; added should be empty",
                0, ssm.getAddedSize());
        assertEquals("Insert(NaN_A) then remove(NaN_B) is a net no-change; removed should be empty",
                0, ssm.getRemovedSize());
    }

    /**
     * Remove a NaN with one bit pattern from an SSM seeded (outside the tracked cycle) with NaN of a different bit
     * pattern. The removal should find the existing entry and record exactly one removal.
     */
    public void testRemoveNaNAcrossBitPattern() {
        final DoubleSegmentedSortedMultiset ssm = new DoubleSegmentedSortedMultiset(NODE_SIZE);
        final double nanA = Double.NaN;
        final double nanB = Double.longBitsToDouble(0x7ff8000000000001L);
        insert(ssm, new double[] {nanA}, new int[] {1});

        ssm.setTrackDeltas(true);
        remove(ssm, new double[] {nanB}, new int[] {1});
        assertEquals(0L, ssm.totalSize());
        assertEquals(0, ssm.getAddedSize());
        assertEquals("removed should record the single NaN removal", 1, ssm.getRemovedSize());
    }

    /**
     * Two SSMs differing only in which NaN bit pattern they store hold, per DoubleComparisons, the same value. They must
     * therefore compare equal -- against each other and against every other Vector spelling of those contents -- and
     * hash alike. {@link DoubleSegmentedSortedMultiset#hashCode()} hashes elements with
     * {@link io.deephaven.util.compare.DoubleComparisons#hashCode(double)}, which collapses NaN bit patterns, so
     * comparing elements with {@code ==} here would break the hashCode contract.
     */
    public void testEqualsAcrossNaNBitPatterns() {
        final double nanA = Double.NaN; // canonical 0x7ff8000000000000L
        final double nanB = Double.longBitsToDouble(0x7ff8000000000001L); // alternate NaN bit pattern

        final double[] withA = new double[] {Double.NEGATIVE_INFINITY, 0.0d, Double.MAX_VALUE, nanA};
        final double[] withB = new double[] {Double.NEGATIVE_INFINITY, 0.0d, Double.MAX_VALUE, nanB};

        final DoubleSegmentedSortedMultiset ssmA = new DoubleSegmentedSortedMultiset(NODE_SIZE);
        insert(ssmA, withA, new int[] {1, 1, 1, 1});
        final DoubleSegmentedSortedMultiset ssmB = new DoubleSegmentedSortedMultiset(NODE_SIZE);
        insert(ssmB, withB, new int[] {1, 1, 1, 1});

        assertEqualBothWays(ssmA, ssmB);
        assertEqualBothWays(ssmA, ssmB.getDirect());
        assertEqualBothWays(ssmA, new DoubleVectorDirect(withB));

        // a boxed Vector of the same values is a different Vector type, and so not equal in either direction
        final Double[] boxedB = new Double[withB.length];
        for (int ii = 0; ii < withB.length; ++ii) {
            boxedB[ii] = withB[ii];
        }
        assertNotEqualBothWays(ssmA, new ObjectVectorDirect<>(boxedB));
    }

    /**
     * The same requirement for signed zero: DoubleComparisons treats {@code -0.0d} and {@code +0.0d} as one value and
     * hashes them alike, so an SSM seeded with one must compare equal to every Vector spelling of the other.
     */
    public void testEqualsAcrossSignedZero() {
        final double[] withNegative = new double[] {-0.0d, Double.MAX_VALUE};
        final double[] withPositive = new double[] {0.0d, Double.MAX_VALUE};

        final DoubleSegmentedSortedMultiset ssmNegative = new DoubleSegmentedSortedMultiset(NODE_SIZE);
        insert(ssmNegative, withNegative, new int[] {1, 1});
        final DoubleSegmentedSortedMultiset ssmPositive = new DoubleSegmentedSortedMultiset(NODE_SIZE);
        insert(ssmPositive, withPositive, new int[] {1, 1});

        assertEqualBothWays(ssmNegative, ssmPositive);
        assertEqualBothWays(ssmNegative, ssmPositive.getDirect());
        assertEqualBothWays(ssmNegative, new DoubleVectorDirect(withPositive));

        final Double[] boxedPositive = new Double[withPositive.length];
        for (int ii = 0; ii < withPositive.length; ++ii) {
            boxedPositive[ii] = withPositive[ii];
        }
        assertNotEqualBothWays(ssmNegative, new ObjectVectorDirect<>(boxedPositive));
    }

    /**
     * Assert that two Vectors agree that they are equal no matter which is the receiver, and that they hash alike as
     * {@link Object#hashCode()} then requires.
     */
    private void assertEqualBothWays(final Object lhs, final Object rhs) {
        assertTrue(lhs + " should equal " + rhs, lhs.equals(rhs));
        assertTrue(rhs + " should equal " + lhs, rhs.equals(lhs));
        assertEquals("equal values must hash alike", lhs.hashCode(), rhs.hashCode());
    }

    /**
     * Assert that two Vectors agree that they are unequal no matter which is the receiver. Their hash codes are
     * unconstrained -- unequal values are permitted to collide.
     */
    private void assertNotEqualBothWays(final Object lhs, final Object rhs) {
        assertFalse(lhs + " should not equal " + rhs, lhs.equals(rhs));
        assertFalse(rhs + " should not equal " + lhs, rhs.equals(lhs));
    }
}
