//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Two RspBitmaps may share a container copy-on-write. A packed ArrayContainer keeps its shared flag in the owning
 * array's spanInfo word rather than on the container, so both sides have to be marked when such a container becomes
 * shared: a side whose flag is clear edits the shared {@code short[]} in place and silently changes the other side.
 */
public class RspBitmapContainerSharingTest {

    private static final long BS = BLOCK_SIZE;
    private static final long SHARED_BIT = 1L << 15;

    private static List<Long> valuesOf(final RspBitmap rb) {
        final List<Long> keys = new ArrayList<>();
        rb.forEachLong(v -> {
            keys.add(v);
            return true;
        });
        return keys;
    }

    /**
     * Span 0 is the singleton 10; span 1 is a packed ArrayContainer holding five scattered values in block 1. Five
     * matters: removing one leaves cardinality 4, which keeps the ArrayContainer representation and so takes its
     * in-place compaction path. At cardinality 3 the container would change type instead, hiding the sharing.
     */
    private static RspBitmap withPackedArrayContainer() {
        RspBitmap rb = new RspBitmap();
        rb = rb.add(10);
        rb = rb.add(BS + 2);
        rb = rb.add(BS + 4);
        rb = rb.add(BS + 6);
        rb = rb.add(BS + 9);
        rb = rb.add(BS + 12);
        return rb;
    }

    /** The result of an andNot must not change when its input is mutated afterwards. */
    @Test
    public void testAndNotResultUnaffectedByLaterMutationOfInput() {
        RspBitmap r1 = withPackedArrayContainer();
        // Matches r1's first span exactly, so andNot takes its common-prefix path and snapshots the rest of r1.
        final RspBitmap r2 = RspBitmap.makeSingle(10);

        final RspBitmap result = RspBitmap.andNot(r1, r2);
        final List<Long> expected = valuesOf(result);
        assertEquals(List.of(BS + 2, BS + 4, BS + 6, BS + 9, BS + 12), expected);

        // Edits the ArrayContainer in place without changing its type.
        r1 = r1.remove(BS + 4);
        assertEquals(List.of(10L, BS + 2, BS + 6, BS + 9, BS + 12), valuesOf(r1));

        result.validate("andNot result after mutating its input");
        assertEquals("andNot result changed when its input was mutated afterwards", expected, valuesOf(result));
    }

    /** Sharing a packed container through the span-index constructor must mark both sides. */
    @Test
    public void testSpanIndexConstructorMarksBothSidesShared() {
        final RspBitmap r1 = withPackedArrayContainer();
        assertTrue("precondition: span 1 is a packed ArrayContainer", r1.spans[1] instanceof short[]);
        assertEquals("precondition: it starts out unshared", 0L, r1.spanInfos[1] & SHARED_BIT);

        final RspBitmap sub = new RspBitmap(r1, 1, 1);
        assertSame("the short[] is expected to be shared, not copied", r1.spans[1], sub.spans[0]);
        assertNotEquals("the copy must be marked shared", 0L, sub.spanInfos[0] & SHARED_BIT);
        assertNotEquals("the source must be marked shared too, or it will edit the shared short[] in place",
                0L, r1.spanInfos[1] & SHARED_BIT);
    }

    /** The other direction: mutating the copy must leave the source alone. */
    /**
     * The same constructor also has to cope with a full block span too long to be held by the marker form. Beyond
     * 0xFFFF blocks the span object is a boxed Long, which is no more a container than the marker is.
     */
    @Test
    public void testSubrangeOfALongFullBlockSpan() {
        final RspBitmap rb = RspBitmap.makeSingleRange(5, 5);
        // Over 0xFFFF blocks, so the span cannot be held in the marker's length bits.
        rb.addRangeUnsafeNoWriteCheck(BS, BS + (1L << 32) + (1L << 20) - 1);
        rb.finishMutations();
        assertTrue("the fixture holds a boxed full block span", rb.spans[1] instanceof Long);

        final RspBitmap sub = new RspBitmap(rb, 1, rb.size() - 1);
        sub.validate("subrange of a long full block span");
        assertEquals("cardinality is carried over", (1L << 32) + (1L << 20), sub.getCardinality());
        rb.validate("source after the subrange");
    }

    @Test
    public void testMutatingTheCopyLeavesTheSourceAlone() {
        final RspBitmap r1 = withPackedArrayContainer();
        RspBitmap sub = new RspBitmap(r1, 1, 1);
        final Object sourceSpan = r1.spans[1];
        sub = sub.remove(BS + 4);
        assertSame("the source's span object must be untouched", sourceSpan, r1.spans[1]);
        assertEquals(List.of(10L, BS + 2, BS + 4, BS + 6, BS + 9, BS + 12), valuesOf(r1));
        r1.validate("source after mutating the copy");
    }
}
