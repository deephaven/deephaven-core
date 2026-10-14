//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Adding a rowset to a builder walks it with a range iterator, which holds a reference to the set being walked and
 * gives it back on reaching the end. A builder that rejects a range stops the walk short, so the reference has to be
 * returned some other way.
 */
public class BuilderAddRowSetLeakTest {

    private static final int REPETITIONS = 20;

    /** A builder that accepts one range and then refuses. */
    private static RowSetBuilderRandom refusingBuilder() {
        return new RowSetBuilderRandom() {
            private int accepted = 0;

            @Override
            public void addKey(final long rowKey) {
                addRange(rowKey, rowKey);
            }

            @Override
            public void addRange(final long firstRowKey, final long lastRowKey) {
                if (++accepted > 1) {
                    throw new IllegalStateException("this builder is full");
                }
            }

            @Override
            public WritableRowSet build() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Several spread-out ranges, so the walk stops with ranges unread; only the counted implementations can show it.
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
    public void testABuilderThatRefusesDoesNotRetainTheAddedRowSet() {
        for (final OrderedLongSet inner : leakableFixtures()) {
            final String name = inner.getClass().getSimpleName();
            try (final WritableRowSetImpl rs = new WritableRowSetImpl(inner)) {
                final int steadyState = inner.ixRefCount();
                for (int i = 0; i < REPETITIONS; ++i) {
                    try {
                        refusingBuilder().addRowSet(rs);
                        fail(name + ": the builder was supposed to refuse");
                    } catch (IllegalStateException expected) {
                        // The point of the exercise.
                    }
                }
                assertEquals(name + ": reference count after " + REPETITIONS + " refused adds", steadyState,
                        inner.ixRefCount());
            }
        }
    }

    /** A builder that accepts everything must leave the count alone too. */
    @Test
    public void testAnAcceptingBuilderDoesNotRetainTheAddedRowSet() {
        for (final OrderedLongSet inner : leakableFixtures()) {
            final String name = inner.getClass().getSimpleName();
            try (final WritableRowSetImpl rs = new WritableRowSetImpl(inner)) {
                final int steadyState = inner.ixRefCount();
                for (int i = 0; i < REPETITIONS; ++i) {
                    final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
                    builder.addRowSet(rs);
                    builder.build().close();
                }
                assertEquals(name + ": reference count after " + REPETITIONS + " adds", steadyState,
                        inner.ixRefCount());
            }
        }
    }

    /** A single range cannot show a leak, but the walk must still work. */
    @Test
    public void testASingleRangeStillAdds() {
        try (final WritableRowSetImpl rs = new WritableRowSetImpl(SingleRange.make(5, 9))) {
            try {
                refusingBuilder().addRowSet(rs);
            } catch (IllegalStateException expected) {
                fail("a single range yields one range, which the builder accepts");
            }
        }
    }
}
