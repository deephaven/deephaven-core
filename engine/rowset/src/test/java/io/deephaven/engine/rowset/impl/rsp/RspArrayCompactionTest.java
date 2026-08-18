//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;

public class RspArrayCompactionTest {

    @Test
    public void testCompactedRemovedSpansTailIsCleared() {
        // 64 container spans, so that removing a few does not trip tryCompactUnsafe's reallocation
        // (which would mask retained references by dropping the arrays altogether).
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < 64; ++i) {
            rb = rb.addRange((long) i * BLOCK_SIZE, (long) i * BLOCK_SIZE + 10);
        }
        RspBitmap other = RspBitmap.makeEmpty();
        other = other.addRange(10L * BLOCK_SIZE, 14L * BLOCK_SIZE - 1); // wipes blocks 10..13 entirely
        rb = rb.andNotEquals(other);
        assertEquals(60L * 11, rb.getCardinality());
        assertEquals(60, rb.size);
        assertTrue(rb.spans.length > rb.size);
        // The vacated tail must not retain references to the removed containers.
        for (int i = rb.size; i < rb.spans.length; ++i) {
            assertNull("spans[" + i + "]", rb.spans[i]);
        }
    }
}
