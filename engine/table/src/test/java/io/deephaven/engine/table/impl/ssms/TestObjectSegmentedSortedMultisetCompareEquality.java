//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.ObjectSsmBackedSource;
import io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.engine.table.impl.util.compact.EqualsConsistentObjectCompactKernel;
import io.deephaven.engine.table.impl.util.compact.ObjectCompactKernel;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ObjectSegmentedSortedMultiset} with values that compare equal without being {@code equals}:
 * BigDecimal values that differ only in scale. The set holds one entry for each class of compare-equal values, and that
 * entry keeps the first value inserted into the class.
 */
public class TestObjectSegmentedSortedMultisetCompareEquality {
    private static final int NODE_SIZE = 4;
    private static final int VALUE_COUNT = 5 * NODE_SIZE;

    private static BigDecimal scaled(final int value, final int scale) {
        return BigDecimal.valueOf(value).setScale(scale);
    }

    private static ObjectSegmentedSortedMultiset makeSsm() {
        return new ObjectSegmentedSortedMultiset(NODE_SIZE, BigDecimal.class);
    }

    /**
     * Insert each of {@code 0 .. VALUE_COUNT - 1} with the given scale, {@code count} times.
     */
    private static void insertRange(final ObjectSegmentedSortedMultiset ssm, final int scale, final int count) {
        try (final WritableObjectChunk<Object, Values> values = WritableObjectChunk.makeWritableChunk(VALUE_COUNT);
                final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(VALUE_COUNT)) {
            for (int ii = 0; ii < VALUE_COUNT; ++ii) {
                values.set(ii, scaled(ii, scale));
                counts.set(ii, count);
            }
            ssm.insert(values, counts);
        }
    }

    /**
     * Remove each of {@code 0 .. VALUE_COUNT - 1} with the given scale, {@code count} times.
     */
    private static void removeRange(final ObjectSegmentedSortedMultiset ssm, final int scale, final int count) {
        try (final WritableObjectChunk<Object, Values> values = WritableObjectChunk.makeWritableChunk(VALUE_COUNT);
                final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(VALUE_COUNT)) {
            for (int ii = 0; ii < VALUE_COUNT; ++ii) {
                values.set(ii, scaled(ii, scale));
                counts.set(ii, count);
            }
            ssm.remove(SegmentedSortedMultiSet.makeRemoveContext(NODE_SIZE), values, counts);
        }
    }

    private static void insert(final ObjectSegmentedSortedMultiset ssm, final Object value, final int count) {
        try (final WritableObjectChunk<Object, Values> values = WritableObjectChunk.makeWritableChunk(1);
                final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(1)) {
            values.set(0, value);
            counts.set(0, count);
            ssm.insert(values, counts);
        }
    }

    private static void remove(final ObjectSegmentedSortedMultiset ssm, final Object value, final int count) {
        try (final WritableObjectChunk<Object, Values> values = WritableObjectChunk.makeWritableChunk(1);
                final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(1)) {
            values.set(0, value);
            counts.set(0, count);
            ssm.remove(SegmentedSortedMultiSet.makeRemoveContext(NODE_SIZE), values, counts);
        }
    }

    private static void assertRange(final ObjectSegmentedSortedMultiset ssm, final int scale, final long count) {
        assertEquals(VALUE_COUNT, ssm.size());
        assertEquals(VALUE_COUNT * count, ssm.totalSize());
        for (int ii = 0; ii < VALUE_COUNT; ++ii) {
            assertEquals(scaled(ii, scale), ssm.get(ii));
        }
    }

    @Test(timeout = 60_000)
    public void testChunkInsertMergesCompareEqualValues() {
        final ObjectSegmentedSortedMultiset ssm = makeSsm();
        insertRange(ssm, 1, 1);
        insertRange(ssm, 2, 1);
        assertRange(ssm, 1, 2);
        insertRange(ssm, 3, 1);
        assertRange(ssm, 1, 3);
    }

    @Test
    public void testSingleValueInsertMergesCompareEqualValues() {
        final ObjectSegmentedSortedMultiset ssm = makeSsm();
        // singleton
        assertTrue(ssm.insert(scaled(5, 1), 1));
        ssm.insert(scaled(5, 2), 1);
        assertEquals(1, ssm.size());
        assertEquals(2, ssm.totalSize());

        // maximum, minimum and interior values of the directory
        ssm.insert(scaled(9, 1), 1);
        ssm.insert(scaled(1, 1), 1);
        ssm.insert(scaled(9, 2), 1);
        ssm.insert(scaled(1, 2), 1);
        ssm.insert(scaled(5, 3), 1);
        assertEquals(3, ssm.size());
        assertEquals(7, ssm.totalSize());
        assertEquals(scaled(1, 1), ssm.get(0));
        assertEquals(scaled(5, 1), ssm.get(1));
        assertEquals(scaled(9, 1), ssm.get(2));

        // interior values of the leaves
        for (int ii = 10; ii < 10 + 2 * NODE_SIZE; ++ii) {
            ssm.insert(scaled(ii, 1), 1);
        }
        ssm.insert(scaled(5, 2), 1);
        ssm.insert(scaled(12, 2), 1);
        assertEquals(3 + 2 * NODE_SIZE, ssm.size());
        assertEquals(9 + 2 * NODE_SIZE, ssm.totalSize());
    }

    @Test
    public void testRemoveCompareEqualValues() {
        final ObjectSegmentedSortedMultiset ssm = makeSsm();
        insertRange(ssm, 1, 3);
        removeRange(ssm, 2, 1);
        assertRange(ssm, 1, 2);
        for (int ii = 0; ii < VALUE_COUNT; ++ii) {
            ssm.remove(scaled(ii, 3), 1);
        }
        assertRange(ssm, 1, 1);
        removeRange(ssm, 2, 1);
        assertEquals(0, ssm.size());
        assertEquals(0, ssm.totalSize());

        insert(ssm, scaled(7, 1), 2);
        remove(ssm, scaled(7, 2), 1);
        assertEquals(1, ssm.totalSize());
        assertTrue(ssm.remove(scaled(7, 3), 1));
        assertEquals(0, ssm.size());
    }

    @Test
    public void testRemoveAbsentValue() {
        final ObjectSegmentedSortedMultiset ssm = makeSsm();
        for (int ii = 0; ii < VALUE_COUNT; ++ii) {
            ssm.insert(scaled(2 * ii, 1), 1);
        }
        assertThrows(AssertionFailure.class, () -> remove(ssm, scaled(3, 1), 1));
    }

    @Test
    public void testDeltasOfCompareEqualValues() {
        final ObjectSegmentedSortedMultiset ssm = makeSsm();
        ssm.setTrackDeltas(true);
        insert(ssm, scaled(1, 1), 1);
        remove(ssm, scaled(1, 2), 1);
        assertEquals(0, ssm.size());
        assertEquals(0, ssm.getAddedSize());
        assertEquals(0, ssm.getRemovedSize());

        insertRange(ssm, 1, 1);
        ssm.clearDeltas();
        removeRange(ssm, 2, 1);
        assertEquals(0, ssm.getAddedSize());
        assertEquals(VALUE_COUNT, ssm.getRemovedSize());
        insertRange(ssm, 1, 1);
        assertEquals(0, ssm.getAddedSize());
        assertEquals(0, ssm.getRemovedSize());
    }

    @Test
    public void testMoveMergesCompareEqualBoundary() {
        final ObjectSegmentedSortedMultiset lo = makeSsm();
        final ObjectSegmentedSortedMultiset hi = makeSsm();
        lo.insert(scaled(0, 1), 1);
        lo.insert(scaled(1, 1), 1);
        hi.insert(scaled(1, 2), 2);
        hi.insert(scaled(2, 1), 1);

        hi.moveFrontToBack(lo, 1);
        assertEquals(2, lo.size());
        assertEquals(scaled(1, 1), lo.getMaxObject());
        assertEquals(2, lo.getMaxCount());
        assertEquals(2, hi.size());
        assertEquals(1, hi.getMinCount());

        lo.moveBackToFront(hi, 2);
        assertEquals(1, lo.size());
        assertEquals(2, hi.size());
        assertEquals(scaled(1, 2), hi.getMinObject());
        assertEquals(3, hi.getMinCount());

        // a singleton source
        final ObjectSegmentedSortedMultiset single = makeSsm();
        single.insert(scaled(1, 3), 2);
        single.moveBackToFront(hi, 1);
        assertEquals(2, hi.size());
        assertEquals(4, hi.getMinCount());
        single.moveFrontToBack(lo, 1);
        assertEquals(2, lo.size());
        assertEquals(scaled(1, 3), lo.getMaxObject());
    }

    private static void assertCompactsToOneValue(final CompactKernel kernel, final Object... values) {
        try (final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(values.length);
                final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(values.length)) {
            for (int ii = 0; ii < values.length; ++ii) {
                chunk.set(ii, values[ii]);
            }
            kernel.compactAndCount(chunk, counts);
            assertEquals(1, chunk.size());
            assertSame(values[0], chunk.get(0));
            assertEquals(values.length, counts.get(0));
        }
    }

    @Test
    public void testCompactKernelCompactsCompareEqualValues() {
        final CompactKernel kernel = CompactKernel.makeCompact(ChunkType.Object,
                BinarySearchKernelHelper.compareConsistentWithEquality(BigDecimal.class));
        assertTrue(kernel instanceof ObjectCompactKernel);
        assertCompactsToOneValue(kernel, scaled(1, 1), scaled(1, 2), scaled(1, 1), scaled(1, 3));
    }

    @Test
    public void testCompactKernelChoosesByDecision() {
        final CompactKernel consistent = CompactKernel.makeCompact(ChunkType.Object, true);
        assertTrue(consistent instanceof EqualsConsistentObjectCompactKernel);
        final CompactKernel general = CompactKernel.makeCompact(ChunkType.Object, false);
        assertTrue(general instanceof ObjectCompactKernel);
        for (final CompactKernel kernel : new CompactKernel[] {consistent, general}) {
            assertCompactsToOneValue(kernel, new String("a"), new String("a"), new String("a"));
        }
    }

    @Test
    public void testSsmFactoriesChooseByDecision() {
        for (final Class<?> dataType : new Class<?>[] {String.class, Boolean.class, java.time.Instant.class}) {
            assertTrue(dataType.getName(), BinarySearchKernelHelper.compareConsistentWithEquality(dataType));
        }
        for (final Class<?> dataType : new Class<?>[] {BigDecimal.class, CharSequence.class, Object.class}) {
            assertFalse(dataType.getName(), BinarySearchKernelHelper.compareConsistentWithEquality(dataType));
        }
        for (final Class<?> dataType : new Class<?>[] {String.class, BigDecimal.class, Object.class}) {
            for (final boolean equalsConsistent : new boolean[] {true, false}) {
                final Class<?> expected = equalsConsistent ? EqualsConsistentObjectSegmentedSortedMultiset.class
                        : ObjectSegmentedSortedMultiset.class;
                final String description = dataType.getName() + ", equalsConsistent=" + equalsConsistent;
                final SegmentedSortedMultiSet<?> ssm =
                        SegmentedSortedMultiSet.make(ChunkType.Object, NODE_SIZE, dataType, equalsConsistent);
                assertEquals(description, expected, ssm.getClass());
                final ObjectSsmBackedSource source = new ObjectSsmBackedSource(dataType, equalsConsistent);
                source.ensureCapacity(1);
                assertEquals(description, expected, source.getOrCreate(0).getClass());
                assertEquals(description, expected, source.getUnderlyingSource().getType());
            }
        }
    }

    @Test
    public void testEqualsConsistentSsmMergesEqualInstances() {
        final EqualsConsistentObjectSegmentedSortedMultiset ssm =
                new EqualsConsistentObjectSegmentedSortedMultiset(NODE_SIZE, String.class);
        for (int ii = 0; ii < VALUE_COUNT; ++ii) {
            ssm.insert(Integer.toString(100 + ii), 1);
        }
        for (int ii = 0; ii < VALUE_COUNT; ++ii) {
            ssm.insert(new String(Integer.toString(100 + ii).toCharArray()), 1);
        }
        assertEquals(VALUE_COUNT, ssm.size());
        assertEquals(2 * VALUE_COUNT, ssm.totalSize());
        for (int ii = 0; ii < VALUE_COUNT; ++ii) {
            assertTrue(ssm.remove(new String(Integer.toString(100 + ii).toCharArray()), 2));
        }
        assertEquals(0, ssm.size());
    }
}
