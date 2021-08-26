package io.deephaven.web.client.api.barrage;

import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class CompressedRangeSetReaderTest {

    @Test
    public void testEmptyIndex() {
        RangeSet empty = new RangeSet();
        ByteBuffer byteBuffer = CompressedRangeSetReader.writeRange(empty);

        // empty is always one byte, make sure it reset too
        assertEquals(0, byteBuffer.position());
        assertEquals(1, byteBuffer.limit());

        RangeSet read = new CompressedRangeSetReader().read(byteBuffer);
        assertEquals(0, read.rangeCount());
        assertEquals(0, read.size());
        assertEquals(empty, read);
    }

    @Test
    public void testSingleItem() {
        assertRoundTrip(RangeSet.ofItems(0));
        assertRoundTrip(RangeSet.ofItems(1));
        assertRoundTrip(RangeSet.ofItems(Short.MAX_VALUE - 1));
        assertRoundTrip(RangeSet.ofItems(Short.MAX_VALUE));
        assertRoundTrip(RangeSet.ofItems(Short.MAX_VALUE + 1));
        assertRoundTrip(RangeSet.ofItems(Integer.MAX_VALUE - 1));
        assertRoundTrip(RangeSet.ofItems(Integer.MAX_VALUE));
        assertRoundTrip(RangeSet.ofItems((long) Integer.MAX_VALUE + 1));

        assertRoundTrip(RangeSet.ofItems(Long.MAX_VALUE / 2));
    }

    @Test
    public void testSingleItems() {
        assertRoundTrip(RangeSet.ofItems(0, 2, 4, 8, (long) Integer.MAX_VALUE + 1));

        assertRoundTrip(RangeSet.ofItems(100, 1000, 10_000_000, (long) Integer.MAX_VALUE + 1));
    }

    @Test
    public void testSimpleRange() {
        assertRoundTrip(RangeSet.ofRange(0, 9));

        assertRoundTrip(RangeSet.ofRange(10, 19));

        assertRoundTrip(RangeSet.ofRange((long) Integer.MAX_VALUE + 10, (long) Integer.MAX_VALUE + 19));
    }

    @Test
    public void testMultipleRanges() {
        assertRoundTrip(RangeSet.fromSortedRanges(new Range[] {
                new Range(0, (long) Integer.MAX_VALUE + 1),
                new Range(Long.MAX_VALUE - 1000, Long.MAX_VALUE - 1)
        }));

        assertRoundTrip(RangeSet.fromSortedRanges(new Range[] {
                new Range(1, 3),
                new Range(5, 7),
                new Range(9, 1000),
                new Range(5000, 10000),
                new Range(Short.MAX_VALUE, Short.MAX_VALUE + 10),
                new Range(Short.MAX_VALUE + 1000, Integer.MAX_VALUE)
        }));
    }

    private static ByteBuffer assertRoundTrip(RangeSet rangeSet) {
        ByteBuffer payload = CompressedRangeSetReader.writeRange(rangeSet);

        // ensure we are correctly moved to the beginning
        assertEquals(0, payload.position());

        RangeSet read = new CompressedRangeSetReader().read(payload);
        assertEquals(rangeSet, read);

        // ensure we read every sent byte
        assertEquals(payload.position(), payload.limit());

        return payload;
    }
}
