//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Row keys are nonnegative. Every way of constructing a row set or row sequence rejects a negative key, even as the
 * bound of an empty range; an empty range between nonnegative keys still yields an empty result.
 */
public class RowSetNegativeKeyTest {

    @Test
    public void testFactory() {
        assertThrows(AssertionFailure.class, () -> RowSetFactory.fromKeys(-1));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.fromKeys(-1, 5));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.fromKeys(5, -1));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.fromRange(-1, 5));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.fromRange(-1, Long.MAX_VALUE));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.fromRange(0, -1));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.fromRange(-1, -2));
        try (final WritableRowSet rs = RowSetFactory.fromRange(5, 4)) {
            assertTrue(rs.isEmpty());
        }
    }

    @Test
    public void testForRange() {
        assertThrows(AssertionFailure.class, () -> RowSequenceFactory.forRange(-1, 5));
        assertThrows(AssertionFailure.class, () -> RowSequenceFactory.forRange(0, -1));
        assertThrows(AssertionFailure.class, () -> RowSequenceFactory.forRange(-1, -2));
        assertSame(RowSequenceFactory.EMPTY, RowSequenceFactory.forRange(5, 4));
        final RowSequence seq = RowSequenceFactory.forRange(3, 5);
        assertEquals(3, seq.size());
        try (final RowSet rs = seq.asRowSet()) {
            assertEquals(3, rs.firstRowKey());
            assertEquals(5, rs.lastRowKey());
        }
    }

    @Test
    public void testFlatRowSequence() {
        assertSame(RowSequenceFactory.EMPTY, RowSequenceFactory.flat(0));
        assertThrows(AssertionFailure.class, () -> RowSequenceFactory.flat(-1));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.flat(-1));
        try (final WritableRowSet rs = RowSetFactory.flat(0)) {
            assertTrue(rs.isEmpty());
        }
        final RowSequence seq = RowSequenceFactory.flat(4);
        assertEquals(4, seq.size());
        assertEquals(0, seq.firstRowKey());
        assertEquals(3, seq.lastRowKey());
    }

    @Test
    public void testInsert() {
        try (final WritableRowSet rs = RowSetFactory.fromRange(10, 20)) {
            assertThrows(AssertionFailure.class, () -> rs.insert(-1));
            assertThrows(AssertionFailure.class, () -> rs.insertRange(-1, 5));
            assertThrows(AssertionFailure.class, () -> rs.insertRange(0, -1));
            try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(2)) {
                chunk.set(0, -2);
                chunk.set(1, 3);
                assertThrows(AssertionFailure.class, () -> rs.insert(chunk, 0, 2));
                // only the slice is checked
                rs.insert(chunk, 1, 1);
            }
            try (final RowSet other = RowSetFactory.fromRange(2, 4)) {
                assertThrows(AssertionFailure.class, () -> rs.insertWithShift(-3, other));
                rs.insertWithShift(-2, other);
            }
            try (final RowSet expected = RowSetFactory.fromKeys(0, 1, 2, 3, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                    20)) {
                assertEquals(expected, rs);
            }
        }
    }

    @Test
    public void testShift() {
        try (final WritableRowSet rs = RowSetFactory.fromKeys(3, 5, 100)) {
            assertThrows(IllegalArgumentException.class, () -> rs.shift(-4));
            assertThrows(IllegalArgumentException.class, () -> rs.shiftInPlace(-4));
            try (final RowSet shifted = rs.shift(-3);
                    final RowSet expected = RowSetFactory.fromKeys(0, 2, 97)) {
                assertEquals(expected, shifted);
            }
            rs.shiftInPlace(-3);
            try (final RowSet expected = RowSetFactory.fromKeys(0, 2, 97)) {
                assertEquals(expected, rs);
            }
        }
        try (final WritableRowSet rs = RowSetFactory.fromRange(Long.MAX_VALUE - 1, Long.MAX_VALUE)) {
            // Shifting up carries the last key past Long.MAX_VALUE, where it wraps negative.
            assertThrows(IllegalArgumentException.class, () -> rs.shift(1));
            assertThrows(IllegalArgumentException.class, () -> rs.shiftInPlace(1));
            try (final WritableRowSet target = RowSetFactory.fromKeys(0)) {
                assertThrows(AssertionFailure.class, () -> target.insertWithShift(1, rs));
            }
        }
        // An empty row set holds no keys to push below zero.
        try (final WritableRowSet rs = RowSetFactory.empty();
                final RowSet shifted = rs.shift(-10)) {
            rs.shiftInPlace(-10);
            assertTrue(rs.isEmpty());
            assertTrue(shifted.isEmpty());
        }
    }

    @Test
    public void testBuilderRandom() {
        assertThrows(AssertionFailure.class, () -> RowSetFactory.builderRandom().addKey(-1));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.builderRandom().addRange(-1, 5));
        assertThrows(AssertionFailure.class, () -> {
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            builder.addRange(10, 20);
            builder.addKey(-5);
        });
        try (final WritableLongChunk<RowKeys> chunk = WritableLongChunk.makeWritableChunk(3)) {
            chunk.set(0, 7);
            chunk.set(1, -3);
            chunk.set(2, 9);
            assertThrows(AssertionFailure.class,
                    () -> RowSetFactory.builderRandom().addRowKeysChunk(chunk, 0, 3));
        }
        try (final WritableLongChunk<RowKeys> chunk = WritableLongChunk.makeWritableChunk(2)) {
            // Long.MAX_VALUE + 1 wraps to Long.MIN_VALUE, so the two keys look like one run.
            chunk.set(0, Long.MAX_VALUE);
            chunk.set(1, Long.MIN_VALUE);
            assertThrows(AssertionFailure.class,
                    () -> RowSetFactory.builderRandom().addRowKeysChunk(chunk, 0, 2));
        }
        try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(2)) {
            chunk.set(0, -2);
            chunk.set(1, -1);
            assertThrows(AssertionFailure.class,
                    () -> RowSetFactory.builderRandom().addOrderedRowKeysChunk(chunk));
        }
    }

    @Test
    public void testBuilderSequential() {
        assertThrows(AssertionFailure.class, () -> RowSetFactory.builderSequential().appendKey(-1));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.builderSequential().appendRange(-1, 5));
        assertThrows(AssertionFailure.class, () -> RowSetFactory.builderSequential().appendRange(0, -1));
        // The order check catches a negative key after the first.
        assertThrows(IllegalArgumentException.class, () -> {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            builder.appendKey(3);
            builder.appendKey(-1);
        });
        assertThrows(AssertionFailure.class, () -> {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            builder.appendKey(0);
            builder.appendRange(1, -1);
        });
        try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(3)) {
            chunk.set(0, -4);
            chunk.set(1, -2);
            chunk.set(2, 0);
            assertThrows(AssertionFailure.class,
                    () -> RowSetFactory.builderSequential().appendOrderedRowKeysChunk(chunk));
        }
    }
}
