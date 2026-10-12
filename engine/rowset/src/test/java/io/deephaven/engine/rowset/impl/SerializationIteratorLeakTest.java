//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Serializing a rowset walks it with a range iterator, which holds a reference to the set it walks and gives that
 * reference back when its walk reaches the end. A write that fails part way through never reaches the end, so the
 * reference has to be returned some other way or the rowset stays marked shared for good.
 */
public class SerializationIteratorLeakTest {

    private static final int REPETITIONS = 20;

    /** A stream that refuses every byte. */
    private static DataOutputStream failingStream() {
        return new DataOutputStream(new OutputStream() {
            @Override
            public void write(final int b) throws IOException {
                throw new IOException("this stream is closed for business");
            }
        });
    }

    /**
     * Several spread-out ranges: the reference is only observably retained when the walk stops with ranges unread, and
     * only the two reference-counted implementations can show it at all -- a single range hands out copies.
     */
    private static List<OrderedLongSet> leakableFixtures() {
        final List<OrderedLongSet> out = new ArrayList<>();
        final RspBitmap rsp = RspBitmap.makeSingleRange(5, 9);
        for (int i = 2; i < 10; ++i) {
            rsp.addRangeUnsafeNoWriteCheck(i * BLOCK_SIZE, i * BLOCK_SIZE + 5);
        }
        rsp.finishMutations();
        out.add(rsp);

        SortedRanges sr = SortedRanges.makeSingleRange(5, 9);
        for (int i = 2; i < 10; ++i) {
            sr = sr.addRange(i * BLOCK_SIZE, i * BLOCK_SIZE + 5);
        }
        out.add(sr);
        return out;
    }

    @Test
    public void testAFailedWriteDoesNotRetainTheRowSet() {
        for (final OrderedLongSet inner : leakableFixtures()) {
            final String name = inner.getClass().getSimpleName();
            try (final WritableRowSetImpl rs = new WritableRowSetImpl(inner)) {
                final int steadyState = inner.ixRefCount();
                for (int i = 0; i < REPETITIONS; ++i) {
                    try {
                        ExternalizableRowSetUtils.writeExternalCompressedDeltas(failingStream(), rs);
                        fail(name + ": the stream was supposed to refuse the write");
                    } catch (IOException expected) {
                        // The point of the exercise.
                    }
                }
                assertEquals(name + ": reference count after " + REPETITIONS + " failed writes", steadyState,
                        inner.ixRefCount());
            }
        }
    }

    /** A write that succeeds must leave the count alone as well. */
    @Test
    public void testASuccessfulWriteDoesNotRetainTheRowSet() throws IOException {
        for (final OrderedLongSet inner : leakableFixtures()) {
            final String name = inner.getClass().getSimpleName();
            try (final WritableRowSetImpl rs = new WritableRowSetImpl(inner)) {
                final int steadyState = inner.ixRefCount();
                for (int i = 0; i < REPETITIONS; ++i) {
                    ExternalizableRowSetUtils.writeExternalCompressedDeltas(
                            new DataOutputStream(OutputStream.nullOutputStream()), rs);
                }
                assertEquals(name + ": reference count after " + REPETITIONS + " writes", steadyState,
                        inner.ixRefCount());
            }
        }
    }

    /** A single range cannot show a leak, but it must still serialize and fail cleanly. */
    @Test
    public void testASingleRangeStillSerializes() throws IOException {
        try (final WritableRowSetImpl rs = new WritableRowSetImpl(SingleRange.make(5, 9))) {
            ExternalizableRowSetUtils.writeExternalCompressedDeltas(
                    new DataOutputStream(OutputStream.nullOutputStream()), rs);
            try {
                ExternalizableRowSetUtils.writeExternalCompressedDeltas(failingStream(), rs);
                fail("the stream was supposed to refuse the write");
            } catch (IOException expected) {
                // expected
            }
        }
    }
}
