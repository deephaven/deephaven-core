//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import junit.framework.TestCase;

import java.util.Random;

public class MixedBuilderTest extends TestCase {
    public void testMixedBuilder() {
        final Random r = new Random();
        final MixedBuilderRandom mb = new MixedBuilderRandom(16);
        final RangePriorityQueueBuilder pqb = new RangePriorityQueueBuilder(16);
        final int sz = 1024 * 1024;
        for (int i = 0; i < sz; ++i) {
            final long n = r.nextInt();
            final long v = (n < 0) ? -n : n;
            mb.addKey(v);
            pqb.addKey(v);
        }
        final OrderedLongSet mbi = mb.getOrderedLongSet();
        final OrderedLongSet pqbi = pqb.getOrderedLongSet();
        assertEquals(pqbi.ixCardinality(), mbi.ixCardinality());
        final RowSet.Iterator mbit = mbi.ixIterator();
        final RowSet.Iterator pqbit = pqbi.ixIterator();
        while (mbit.hasNext()) {
            final long mv = mbit.nextLong();
            final long mp = pqbit.nextLong();
            assertEquals(mp, mv);
        }
    }

    public void testAddRowSetReleasesReplacedAccumulator() {
        // Both inputs are big enough (>= addAsIndexThreshold) that the builder accumulates them as
        // cow references rather than iterating their ranges.
        RspBitmap rb1 = RspBitmap.makeEmpty();
        rb1 = rb1.addRange(0, 70000);
        RspBitmap rb2 = RspBitmap.makeEmpty();
        rb2 = rb2.addRange(200000, 280000);
        final MixedBuilderRandom mb = new MixedBuilderRandom(16);
        mb.add(rb1, false);
        mb.add(rb2, false);
        final OrderedLongSet result = mb.getOrderedLongSet();
        assertEquals(70001L + 80001L, result.ixCardinality());
        result.ixRelease();
        // The builder must not retain references on the source sets once done with them.
        assertEquals(1, rb1.refCount());
        assertEquals(1, rb2.refCount());
    }
}
