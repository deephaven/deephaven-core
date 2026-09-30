//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.impl.rsp.container.ArrayContainer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Two RspBitmaps may share a container copy-on-write. A packed ArrayContainer is a bare {@code short[]} with no
 * container object to carry a flag, so its shared flag lives in the array's reserved last slot, where every holder of
 * the array sees it: a side that missed the flag would edit the shared {@code short[]} in place and silently change the
 * other side. The spanInfo word of a packed span holds only its key and cardinality.
 */
public class RspBitmapContainerSharingTest {

    private static final long BS = BLOCK_SIZE;

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

    /**
     * Sharing a packed container through the span-index constructor marks the shared array, which both sides read, and
     * leaves both words as plain key and cardinality.
     */
    @Test
    public void testSpanIndexConstructorMarksTheSharedArray() {
        final RspBitmap r1 = withPackedArrayContainer();
        assertTrue("precondition: span 1 is a packed ArrayContainer", r1.spans[1] instanceof short[]);
        final short[] packed = (short[]) r1.spans[1];
        assertFalse("precondition: it starts out unshared", ArrayContainer.isContentShared(packed));
        assertEquals("the word is the key and the cardinality", BS | 5, r1.spanInfos[1]);

        final RspBitmap sub = new RspBitmap(r1, 1, 1);
        assertSame("the short[] is expected to be shared, not copied", packed, sub.spans[0]);
        assertTrue("the shared array must be marked, or the source will edit it in place",
                ArrayContainer.isContentShared(packed));
        assertEquals("the source's word is unchanged by sharing", BS | 5, r1.spanInfos[1]);
        assertEquals("the copy's word is the key and the cardinality", BS | 5, sub.spanInfos[0]);
    }

    /**
     * A sub range that covers a whole block shares that block's packed container rather than copying it, through the
     * shared-container append path. The copy's word has to carry the container's cardinality, not just its key, or the
     * copy reads the container as empty.
     */
    @Test
    public void testSubrangeCoveringAWholeBlockSharesThePackedContainer() {
        final RspBitmap r1 = withPackedArrayContainer();
        final short[] packed = (short[]) r1.spans[1];

        final RspBitmap sub = r1.subrangeByValue(BS, 2 * BS - 1, true);
        assertEquals(List.of(BS + 2, BS + 4, BS + 6, BS + 9, BS + 12), valuesOf(sub));
        assertSame("the whole block is shared, not copied", packed, sub.spans[0]);
        assertEquals("the copy's word is the key and the cardinality", BS | 5, sub.spanInfos[0]);
        assertEquals("the source's word is unchanged", BS | 5, r1.spanInfos[1]);
        assertTrue(ArrayContainer.isContentShared(packed));
        sub.validate("sub range sharing a packed container");
        r1.validate("source after a sub range shared its packed container");
    }

    /**
     * The marking protocol: a reader that shares a packed container marks the array it observed, and nothing else in
     * the source. Here the reader is late: by the time it marks, the owner has replaced the span, and in a way that
     * would have defeated a check on the word. The span had cardinality 4 and is replaced by a singleton whose low bits
     * are also 4, so the two words are equal; a flag written into the word would have turned the singleton into a
     * different key. Written into the orphaned array instead, the mark reaches nothing the owner still holds.
     */
    @Test
    public void testLateMarkOfAReplacedSpanLeavesTheOwnerAlone() {
        RspBitmap r1 = new RspBitmap();
        r1 = r1.add(10);
        r1 = r1.add(BS + 1);
        r1 = r1.add(BS + 4);
        r1 = r1.add(BS + 7);
        r1 = r1.add(BS + 9);
        assertTrue("precondition: span 1 is a packed ArrayContainer", r1.spans[1] instanceof short[]);
        assertEquals("precondition: the span's word is key | 4", BS | 4, r1.spanInfos[1]);
        // What a reader deriving from r1 observes before it gets around to marking.
        final short[] observed = (short[]) r1.spans[1];

        // The owner replaces the span with the singleton BS + 4, whose word is also BS | 4.
        r1 = r1.remove(BS + 1);
        r1 = r1.remove(BS + 7);
        r1 = r1.remove(BS + 9);
        assertEquals(List.of(10L, BS + 4), valuesOf(r1));
        assertNull("the block is now a singleton span", r1.spans[1]);
        assertEquals("the singleton's word coincides with the packed span's", BS | 4, r1.spanInfos[1]);
        final long[] wordsBefore = r1.spanInfos.clone();
        final Object[] spansBefore = r1.spans.clone();

        // The late reader marks what it observed.
        ArrayContainer.markContentShared(observed);

        assertArrayEquals("the owner's words are untouched", wordsBefore, r1.spanInfos);
        assertArrayEquals("the owner's spans are untouched", spansBefore, r1.spans);
        assertEquals(List.of(10L, BS + 4), valuesOf(r1));
        r1.validate("owner after a late mark of a span it had replaced");
        assertTrue("the orphaned array carries the mark", ArrayContainer.isContentShared(observed));
    }

    /**
     * The other half of the protocol: when the reader is not late, the owner finds the flag on the array it is about to
     * edit and copies instead. The reader here is a bare mark with no RspBitmap of its own, which is all the owner ever
     * sees of one.
     */
    @Test
    public void testOwnerCopiesOnWriteOnceAReaderHasMarkedItsSpan() {
        RspBitmap r1 = withPackedArrayContainer();
        final short[] observed = (short[]) r1.spans[1];

        ArrayContainer.markContentShared(observed);
        final short[] observedContents = observed.clone();

        r1 = r1.remove(BS + 4);
        assertEquals(List.of(10L, BS + 2, BS + 6, BS + 9, BS + 12), valuesOf(r1));
        assertNotSame("the owner must have copied rather than edited the marked array", observed, r1.spans[1]);
        assertArrayEquals("the marked array is unchanged", observedContents, observed);
        assertEquals("the owner's new word is the key and the cardinality", BS | 4, r1.spanInfos[1]);
        assertTrue("the owner's new array is its own", r1.spans[1] instanceof short[]);
        assertFalse(ArrayContainer.isContentShared((short[]) r1.spans[1]));
        r1.validate("owner after copying a marked span on write");
    }

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
