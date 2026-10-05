//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link RowSequence.Iterator#advance} to a key at or before the current position is a no-op: an iterator only ever
 * moves forward, so a key that was already handed out must not come back.
 */
public class RowSequenceRowKeysChunkImplAdvanceTest {

    private static RowSequence keys(final long... keys) {
        return RowSequenceFactory.wrapRowKeysChunkAsRowSequence(LongChunk.chunkWrap(keys));
    }

    @Test
    public void testAdvanceToTheKeyJustConsumedIsANoOp() {
        try (final RowSequence.Iterator it = keys(10, 11, 12).getRowSequenceIterator()) {
            assertTrue(it.advance(12));
            assertEquals(12, it.peekNextKey());
            assertTrue(it.advance(11));
            assertEquals("advance back to a consumed key", 12, it.peekNextKey());
            assertEquals(2, it.getRelativePosition());
        }
    }

    @Test
    public void testAdvanceToTheLastKeyDeliveredByThroughIsANoOp() {
        try (final RowSequence.Iterator it = keys(10, 11, 12, 13).getRowSequenceIterator()) {
            assertEquals(2, it.getNextRowSequenceThrough(11).size());
            assertTrue(it.advance(11));
            assertEquals("advance to the key through() ended on", 12, it.peekNextKey());
            assertEquals(2, it.getNextRowSequenceWithLength(5).size());
        }
    }
}
