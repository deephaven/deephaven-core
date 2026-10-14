//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertTrue;

/**
 * {@link RowSequence#getAverageRunLengthEstimate()} is between one and the size, by contract. A slice of a SortedRanges
 * can touch more array entries than it holds keys, and dividing by entries then rounds to zero.
 */
public class SortedRangesRowSequenceAverageRunLengthTest {

    @Test
    public void testTwoKeysAcrossThreeEntries() {
        try (final WritableRowSet rs = sortedRangesOf(new long[] {5, 5}, new long[] {10, 12});
                final RowSequence slice = rs.getRowSequenceByPosition(0, 2)) {
            final long estimate = slice.getAverageRunLengthEstimate();
            assertTrue("estimate " + estimate, estimate >= 1 && estimate <= slice.size());
        }
    }
}
