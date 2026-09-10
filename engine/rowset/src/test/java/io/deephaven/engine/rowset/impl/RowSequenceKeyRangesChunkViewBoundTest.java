//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A key-ranges-chunk {@link RowSequence} sliced to a sub-view must never expose keys outside that view, and must report
 * position distances relative to it. The equivalent RowSet-backed sequence is used as the oracle.
 */
public class RowSequenceKeyRangesChunkViewBoundTest {

    private static RowSequence chunkSequence(final long... ranges) {
        return RowSequenceFactory.wrapKeyRangesChunkAsRowSequence(LongChunk.chunkWrap(ranges));
    }

    private static RowSet rowSetOf(final RowSequence rs) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        rs.forAllRowKeyRanges(builder::appendRange);
        return builder.build();
    }

    private static List<Long> keysOf(final RowSequence rs) {
        final List<Long> keys = new ArrayList<>();
        rs.forAllRowKeys(keys::add);
        return keys;
    }

    /** Compare a chunk-backed view against an equivalent RowSet-backed sequence for a set of through-keys. */
    private static void assertViewMatchesRowSet(final String m, final RowSequence view, final long... throughKeys) {
        try (final RowSet reference = rowSetOf(view)) {
            assertEquals(m + ": size", reference.size(), view.size());
            assertEquals(m + ": keys", keysOf(reference), keysOf(view));

            for (final long through : throughKeys) {
                final String mk = m + " through=" + through;
                try (final RowSequence.Iterator viewIt = view.getRowSequenceIterator();
                        final RowSequence.Iterator refIt = reference.getRowSequenceIterator()) {
                    final RowSequence viewSlice = viewIt.getNextRowSequenceThrough(through);
                    final RowSequence refSlice = refIt.getNextRowSequenceThrough(through);
                    assertEquals(mk + ": slice keys", keysOf(refSlice), keysOf(viewSlice));
                    assertEquals(mk + ": slice size", refSlice.size(), viewSlice.size());
                    assertEquals(mk + ": hasMore after slice", refIt.hasMore(), viewIt.hasMore());
                }
                try (final RowSequence.Iterator viewIt = view.getRowSequenceIterator();
                        final RowSequence.Iterator refIt = reference.getRowSequenceIterator()) {
                    assertEquals(mk + ": advanceAndGetPositionDistance",
                            refIt.advanceAndGetPositionDistance(through),
                            viewIt.advanceAndGetPositionDistance(through));
                    assertEquals(mk + ": hasMore after advance", refIt.hasMore(), viewIt.hasMore());
                    assertEquals(mk + ": peekNextKey after advance", refIt.peekNextKey(), viewIt.peekNextKey());
                }
            }
        }
    }

    @Test
    public void testThroughKeyBeyondViewEndDoesNotLeakKeys() {
        // Backing [10,15] viewed as [10,12]: the view holds {10,11,12} only.
        try (final RowSequence full = chunkSequence(10, 15);
                final RowSequence view = full.getRowSequenceByKeyRange(10, 12)) {
            try (final RowSequence.Iterator it = view.getRowSequenceIterator()) {
                final RowSequence slice = it.getNextRowSequenceThrough(14);
                assertEquals("keys past the view end must not be exposed", List.of(10L, 11L, 12L), keysOf(slice));
                assertEquals(3, slice.size());
                assertEquals(12, slice.lastRowKey());
                assertFalse(it.hasMore());
            }
            try (final RowSequence.Iterator it = view.getRowSequenceIterator()) {
                final RowSequence slice = it.getNextRowSequenceThrough(Long.MAX_VALUE);
                assertEquals(List.of(10L, 11L, 12L), keysOf(slice));
                assertFalse(it.hasMore());
            }
        }
    }

    @Test
    public void testAdvancePastViewEndReportsViewRelativeDistance() {
        // Backing [10,15] viewed as [10,12].
        try (final RowSequence full = chunkSequence(10, 15);
                final RowSequence view = full.getRowSequenceByKeyRange(10, 12)) {
            assertEquals(3, view.size());
            try (final RowSequence.Iterator it = view.getRowSequenceIterator()) {
                // Advancing past the view's end consumes exactly the view's three keys.
                assertEquals(3, it.advanceAndGetPositionDistance(14));
                assertFalse(it.hasMore());
                assertEquals(RowSequence.NULL_ROW_KEY, it.peekNextKey());
            }
            try (final RowSequence.Iterator it = view.getRowSequenceIterator()) {
                assertFalse("advance past the view end reports exhaustion", it.advance(14));
                assertFalse(it.hasMore());
            }
            try (final RowSequence.Iterator it = view.getRowSequenceIterator()) {
                // Advancing to exactly the view's last key leaves it available.
                assertTrue(it.advance(12));
                assertTrue(it.hasMore());
                assertEquals(12, it.peekNextKey());
            }
        }
    }

    @Test
    public void testViewBoundAgainstRowSetOracle() {
        // A view whose bound falls inside the backing chunk's last range, in several shapes.
        try (final RowSequence full = chunkSequence(10, 15)) {
            try (final RowSequence view = full.getRowSequenceByKeyRange(10, 12)) {
                assertViewMatchesRowSet("[10,15] as [10,12]", view, 9, 10, 11, 12, 13, 14, 15, Long.MAX_VALUE);
            }
            try (final RowSequence view = full.getRowSequenceByKeyRange(11, 12)) {
                assertViewMatchesRowSet("[10,15] as [11,12]", view, 10, 11, 12, 13, 16, Long.MAX_VALUE);
            }
        }
        // Multi-range backing, view bound inside the last range.
        try (final RowSequence full = chunkSequence(1, 3, 10, 15, 20, 25)) {
            try (final RowSequence view = full.getRowSequenceByKeyRange(2, 22)) {
                assertViewMatchesRowSet("multi as [2,22]", view, 1, 2, 3, 5, 10, 15, 20, 22, 23, 25, Long.MAX_VALUE);
            }
            try (final RowSequence view = full.getRowSequenceByKeyRange(11, 13)) {
                assertViewMatchesRowSet("multi as [11,13]", view, 10, 11, 13, 14, 20, Long.MAX_VALUE);
            }
        }
    }

    @Test
    public void testIncrementalConsumptionStaysWithinView() {
        // Consume the view in pieces, with the final request reaching past its end.
        try (final RowSequence full = chunkSequence(10, 15);
                final RowSequence view = full.getRowSequenceByKeyRange(10, 12);
                final RowSequence.Iterator it = view.getRowSequenceIterator()) {
            assertEquals(List.of(10L, 11L), keysOf(it.getNextRowSequenceThrough(11)));
            assertTrue(it.hasMore());
            assertEquals(12, it.peekNextKey());
            assertEquals(List.of(12L), keysOf(it.getNextRowSequenceThrough(14)));
            assertFalse(it.hasMore());
            assertTrue(it.getNextRowSequenceThrough(Long.MAX_VALUE).isEmpty());
        }
    }
}
