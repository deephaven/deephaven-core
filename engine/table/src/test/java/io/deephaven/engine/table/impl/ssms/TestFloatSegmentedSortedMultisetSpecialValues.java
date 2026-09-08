//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.vector.FloatVectorDirect;
import io.deephaven.vector.ObjectVectorDirect;

/**
 * Tests for {@link FloatSegmentedSortedMultiset} behavior around the special float values that
 * {@link io.deephaven.util.compare.FloatComparisons} treats specially: {@code -0.0f} (which compares equal to
 * {@code +0.0f}) and {@code NaN} (where any NaN bit pattern compares equal to any other NaN).
 *
 * <p>
 * The SSM's delta-tracking added/removed sets must agree with the SSM's leaf-storage equality (FloatComparisons.eq). If
 * they disagree -- e.g. because the underlying hash set treats {@code -0.0f} and {@code +0.0f} as distinct, or keeps
 * two NaN bit patterns as separate entries -- then a net-no-change insert/remove pair can leak phantom adds/removes
 * through {@code fillAddedChunk}/{@code fillRemovedChunk}.
 */
public class TestFloatSegmentedSortedMultisetSpecialValues extends RefreshingTableTestCase {

    private static final int NODE_SIZE = 64;

    private static FloatSegmentedSortedMultiset trackingSsm() {
        final FloatSegmentedSortedMultiset ssm = new FloatSegmentedSortedMultiset(NODE_SIZE);
        ssm.setTrackDeltas(true);
        return ssm;
    }

    private static void insert(final FloatSegmentedSortedMultiset ssm, final float[] values, final int[] counts) {
        try (final WritableFloatChunk<Values> v = WritableFloatChunk.makeWritableChunk(values.length);
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

    private static void remove(final FloatSegmentedSortedMultiset ssm, final float[] values, final int[] counts) {
        try (final WritableFloatChunk<Values> v = WritableFloatChunk.makeWritableChunk(values.length);
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
     * Insert {@code -0.0f} then remove {@code +0.0f}. FloatComparisons treats the two as equal, so the leaf empties out
     * AND the delta-tracking should recognize that the removal cancels the prior addition, leaving both added and
     * removed sets empty.
     */
    public void testInsertNegativeZeroThenRemovePositiveZero() {
        final FloatSegmentedSortedMultiset ssm = trackingSsm();

        insert(ssm, new float[] {-0.0f}, new int[] {1});
        assertEquals(1L, ssm.totalSize());
        assertEquals(1, ssm.getAddedSize());
        assertEquals(0, ssm.getRemovedSize());

        remove(ssm, new float[] {+0.0f}, new int[] {1});
        assertEquals("Leaf should be empty after removing the (FloatComparisons-equal) value",
                0L, ssm.totalSize());
        assertEquals("Insert(-0) then remove(+0) is a net no-change; added should be empty",
                0, ssm.getAddedSize());
        assertEquals("Insert(-0) then remove(+0) is a net no-change; removed should be empty",
                0, ssm.getRemovedSize());
    }

    /**
     * Insert {@code +0.0f} then remove {@code -0.0f} -- the mirror of the previous test. Same FloatComparisons
     * semantics apply.
     */
    public void testInsertPositiveZeroThenRemoveNegativeZero() {
        final FloatSegmentedSortedMultiset ssm = trackingSsm();

        insert(ssm, new float[] {+0.0f}, new int[] {1});
        assertEquals(1L, ssm.totalSize());
        assertEquals(1, ssm.getAddedSize());

        remove(ssm, new float[] {-0.0f}, new int[] {1});
        assertEquals(0L, ssm.totalSize());
        assertEquals(0, ssm.getAddedSize());
        assertEquals(0, ssm.getRemovedSize());
    }

    /**
     * Remove {@code +0.0f} from an SSM that contains {@code -0.0f} placed there outside of the tracked cycle. The
     * removal should match the existing entry and be recorded in {@code removed} exactly once.
     */
    public void testRemoveZeroAcrossSign() {
        final FloatSegmentedSortedMultiset ssm = new FloatSegmentedSortedMultiset(NODE_SIZE);
        // Seed the leaf without delta tracking.
        insert(ssm, new float[] {-0.0f}, new int[] {1});

        ssm.setTrackDeltas(true);
        remove(ssm, new float[] {+0.0f}, new int[] {1});
        assertEquals(0L, ssm.totalSize());
        assertEquals(0, ssm.getAddedSize());
        assertEquals("removed should record the single removal", 1, ssm.getRemovedSize());
    }

    /**
     * Insert two NaN values with distinct raw bit patterns in separate cycles. FloatComparisons treats them as equal,
     * so the leaf must collapse them into a single entry with count 2 and the delta-tracking added set must contain
     * exactly one NaN entry.
     */
    public void testNaNsWithDifferentBitPatternsAreSameValue() {
        final FloatSegmentedSortedMultiset ssm = trackingSsm();
        final float nanA = Float.NaN; // canonical 0x7fc00000
        final float nanB = Float.intBitsToFloat(0x7fc12345); // alternate NaN bit pattern
        assertTrue(Float.isNaN(nanA) && Float.isNaN(nanB));
        assertNotSame("NaN bit patterns must differ for this test to be meaningful",
                Float.floatToRawIntBits(nanA), Float.floatToRawIntBits(nanB));

        insert(ssm, new float[] {nanA}, new int[] {1});
        insert(ssm, new float[] {nanB}, new int[] {1});

        assertEquals("NaN-with-any-bits should occupy a single SSM entry with count 2", 2L, ssm.totalSize());
        assertEquals("Different NaN bit patterns are the same value to the delta tracker",
                1, ssm.getAddedSize());
        assertEquals(0, ssm.getRemovedSize());
    }

    /**
     * Insert one NaN bit pattern then remove a different NaN bit pattern. FloatComparisons treats them as equal, so the
     * leaf empties and the delta tracking should net to no change.
     */
    public void testInsertOneNaNThenRemoveAnotherNaN() {
        final FloatSegmentedSortedMultiset ssm = trackingSsm();
        final float nanA = Float.NaN;
        final float nanB = Float.intBitsToFloat(0x7fc12345);

        insert(ssm, new float[] {nanA}, new int[] {1});
        assertEquals(1L, ssm.totalSize());
        assertEquals(1, ssm.getAddedSize());

        remove(ssm, new float[] {nanB}, new int[] {1});
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
        final FloatSegmentedSortedMultiset ssm = new FloatSegmentedSortedMultiset(NODE_SIZE);
        final float nanA = Float.NaN;
        final float nanB = Float.intBitsToFloat(0x7fc12345);
        insert(ssm, new float[] {nanA}, new int[] {1});

        ssm.setTrackDeltas(true);
        remove(ssm, new float[] {nanB}, new int[] {1});
        assertEquals(0L, ssm.totalSize());
        assertEquals(0, ssm.getAddedSize());
        assertEquals("removed should record the single NaN removal", 1, ssm.getRemovedSize());
    }

    /**
     * Two SSMs differing only in which NaN bit pattern they store hold, per FloatComparisons, the same value. They must
     * therefore compare equal -- against each other and against every other Vector spelling of those contents -- and
     * hash alike. {@link FloatSegmentedSortedMultiset#hashCode()} hashes elements with
     * {@link io.deephaven.util.compare.FloatComparisons#hashCode(float)}, which collapses NaN bit patterns, so
     * comparing elements with {@code ==} here would break the hashCode contract.
     */
    public void testEqualsAcrossNaNBitPatterns() {
        final float nanA = Float.NaN; // canonical 0x7fc00000
        final float nanB = Float.intBitsToFloat(0x7fc12345); // alternate NaN bit pattern

        final float[] withA = new float[] {Float.NEGATIVE_INFINITY, 0.0f, Float.MAX_VALUE, nanA};
        final float[] withB = new float[] {Float.NEGATIVE_INFINITY, 0.0f, Float.MAX_VALUE, nanB};

        final FloatSegmentedSortedMultiset ssmA = new FloatSegmentedSortedMultiset(NODE_SIZE);
        insert(ssmA, withA, new int[] {1, 1, 1, 1});
        final FloatSegmentedSortedMultiset ssmB = new FloatSegmentedSortedMultiset(NODE_SIZE);
        insert(ssmB, withB, new int[] {1, 1, 1, 1});

        assertEqualBothWays(ssmA, ssmB);
        assertEqualBothWays(ssmA, ssmB.getDirect());
        assertEqualBothWays(ssmA, new FloatVectorDirect(withB));

        // a boxed Vector of the same values is a different Vector type, and so not equal in either direction
        final Float[] boxedB = new Float[withB.length];
        for (int ii = 0; ii < withB.length; ++ii) {
            boxedB[ii] = withB[ii];
        }
        assertNotEqualBothWays(ssmA, new ObjectVectorDirect<>(boxedB));
    }

    /**
     * The same requirement for signed zero: FloatComparisons treats {@code -0.0f} and {@code +0.0f} as one value and
     * hashes them alike, so an SSM seeded with one must compare equal to every Vector spelling of the other.
     */
    public void testEqualsAcrossSignedZero() {
        final float[] withNegative = new float[] {-0.0f, Float.MAX_VALUE};
        final float[] withPositive = new float[] {0.0f, Float.MAX_VALUE};

        final FloatSegmentedSortedMultiset ssmNegative = new FloatSegmentedSortedMultiset(NODE_SIZE);
        insert(ssmNegative, withNegative, new int[] {1, 1});
        final FloatSegmentedSortedMultiset ssmPositive = new FloatSegmentedSortedMultiset(NODE_SIZE);
        insert(ssmPositive, withPositive, new int[] {1, 1});

        assertEqualBothWays(ssmNegative, ssmPositive);
        assertEqualBothWays(ssmNegative, ssmPositive.getDirect());
        assertEqualBothWays(ssmNegative, new FloatVectorDirect(withPositive));

        final Float[] boxedPositive = new Float[withPositive.length];
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
