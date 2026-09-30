//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * {@link RspArray#getRangeBatchIterator(long, long)} takes a maximum count of keys to produce. A maximum larger than
 * what the bitmap holds produces everything exactly once; the iterator then reports that it is exhausted rather than
 * delivering the last span again.
 */
public class RspRangeBatchIteratorMaxCountTest {

    private static List<long[]> drain(final RspRangeBatchIterator it, final int chunkCapacity) {
        final List<long[]> out = new ArrayList<>();
        int guard = 0;
        try (final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(chunkCapacity)) {
            while (it.hasNext()) {
                if (++guard > 50) {
                    fail("iterator did not terminate; ranges produced so far: " + render(out));
                }
                final int n = it.fillRangeChunk(chunk, 0);
                for (int i = 0; i < n; ++i) {
                    out.add(new long[] {chunk.get(2 * i), chunk.get(2 * i + 1)});
                }
            }
        } finally {
            it.close();
        }
        return out;
    }

    private static String render(final List<long[]> ranges) {
        final StringBuilder sb = new StringBuilder();
        int shown = 0;
        for (final long[] r : ranges) {
            if (++shown > 12) {
                sb.append("... (").append(ranges.size()).append(" ranges in total)");
                break;
            }
            sb.append(r[0]).append('-').append(r[1]).append(' ');
        }
        return sb.toString();
    }

    private static void checkAllCounts(final RspBitmap rb, final String expected) {
        assertEquals("exact count", expected, render(drain(rb.getRangeBatchIterator(0, rb.getCardinality()), 64)));
        assertEquals("count one past the cardinality", expected,
                render(drain(rb.getRangeBatchIterator(0, rb.getCardinality() + 1), 64)));
        assertEquals("unbounded count", expected, render(drain(rb.getRangeBatchIterator(0, Long.MAX_VALUE), 64)));
        // A chunk too small to take all ranges at once exercises the leftover buffer path as well.
        assertEquals("unbounded count, small chunk", expected,
                render(drain(rb.getRangeBatchIterator(0, Long.MAX_VALUE), 2)));
    }

    @Test
    public void testEndingInContainer() {
        final RspBitmap rb = RspBitmap.makeSingleRange(0, 65535); // one full block span
        rb.addRangeUnsafeNoWriteCheck(70000, 70010); // then a container
        rb.addRangeUnsafeNoWriteCheck(70020, 70030);
        rb.finishMutations();
        checkAllCounts(rb, "0-65535 70000-70010 70020-70030 ");
    }

    @Test
    public void testContainersOnly() {
        final RspBitmap rb = RspBitmap.makeSingleRange(5, 10);
        rb.addRangeUnsafeNoWriteCheck(20, 30);
        rb.finishMutations();
        checkAllCounts(rb, "5-10 20-30 ");
    }

    @Test
    public void testEndingInSingleton() {
        final RspBitmap rb = RspBitmap.makeSingleRange(5, 10);
        rb.addRangeUnsafeNoWriteCheck(2 * 65536 + 7, 2 * 65536 + 7);
        rb.finishMutations();
        checkAllCounts(rb, "5-10 131079-131079 ");
    }

    @Test
    public void testEndingInFullBlockSpan() {
        final RspBitmap rb = RspBitmap.makeSingleRange(5, 10);
        rb.addRangeUnsafeNoWriteCheck(2 * 65536, 4 * 65536 - 1);
        rb.finishMutations();
        checkAllCounts(rb, "5-10 131072-262143 ");
    }
}
