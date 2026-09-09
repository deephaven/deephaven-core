//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequenceFactory;
import org.junit.Test;

public class ShiftedRowSequenceTest extends RowSequenceTestBase {

    private final long SHIFT = 1 << 20;

    @Override
    protected RowSequence create(long... values) {
        final long[] shifted = new long[values.length];
        for (int i = 0; i < values.length; ++i) {
            shifted[i] = values[i] + SHIFT;
        }
        final RowSequence other = RowSequenceFactory.wrapRowKeysChunkAsRowSequence(LongChunk.chunkWrap(shifted));
        return ShiftedRowSequence.wrap(closeOnTearDownCase(other), -SHIFT);
    }

    @Test
    @Override
    // The original test uses some large keys that overflow when shifted.
    public void testCanConstructRowSequence() {
        final long[] indices = indicesFromRanges(0, 4, Long.MAX_VALUE - 4 - SHIFT, Long.MAX_VALUE - SHIFT);
        try (final RowSequence OK = create(indices)) {
            assertContentsByIndices(indices, OK);
        }
    }

    @Test
    public void testUnboundedArgumentsWithNegativeShift() {
        // Unshifting Long.MAX_VALUE ("no upper bound") with a negative shift must saturate, not wrap.
        try (final io.deephaven.engine.rowset.RowSet toWrap = io.deephaven.engine.rowset.RowSetFactory
                .fromKeys(100, 200, 300)) {
            final RowSequence shifted = ShiftedRowSequence.wrap(toWrap, -50);
            try (final RowSequence.Iterator it = shifted.getRowSequenceIterator()) {
                final RowSequence all = it.getNextRowSequenceThrough(Long.MAX_VALUE);
                org.junit.Assert.assertEquals(3, all.size());
                org.junit.Assert.assertEquals(50, all.firstRowKey());
                org.junit.Assert.assertEquals(250, all.lastRowKey());
                org.junit.Assert.assertFalse(it.hasMore());
            }
            try (final RowSequence sub = shifted.getRowSequenceByKeyRange(51, Long.MAX_VALUE)) {
                org.junit.Assert.assertEquals(2, sub.size());
                org.junit.Assert.assertEquals(150, sub.firstRowKey());
            }
            shifted.close();
        }
    }

    @Test
    public void testWrapRejectsShiftsThatEscapeTheKeySpace() {
        try (final io.deephaven.engine.rowset.RowSet toWrap = io.deephaven.engine.rowset.RowSetFactory
                .fromKeys(100, 200)) {
            try {
                ShiftedRowSequence.wrap(toWrap, -101);
                org.junit.Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            try {
                ShiftedRowSequence.wrap(toWrap, Long.MAX_VALUE - 199);
                org.junit.Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            try {
                new ShiftedRowSequence().reset(toWrap, -101);
                org.junit.Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
            }
            // Boundary-legal shifts are accepted.
            ShiftedRowSequence.wrap(toWrap, -100).close();
            ShiftedRowSequence.wrap(toWrap, Long.MAX_VALUE - 200).close();
        }
    }

    @Test
    public void testAdvancePastKeySpaceExhausts() {
        // With a negative shift, an advance target whose unshifted value overflows lies beyond any key the
        // sequence can contain: the iterator must exhaust, not position at a smaller key.
        try (final io.deephaven.engine.rowset.RowSet toWrap = io.deephaven.engine.rowset.RowSetFactory
                .fromKeys(Long.MAX_VALUE - 10, Long.MAX_VALUE)) {
            final RowSequence shifted = ShiftedRowSequence.wrap(toWrap, -50);
            try (final RowSequence.Iterator it = shifted.getRowSequenceIterator()) {
                // Shifted keys are MAX-60 and MAX-50; both are below the requested key.
                org.junit.Assert.assertFalse(it.advance(Long.MAX_VALUE));
                org.junit.Assert.assertFalse(it.hasMore());
            }
            shifted.close();
        }
    }
}
