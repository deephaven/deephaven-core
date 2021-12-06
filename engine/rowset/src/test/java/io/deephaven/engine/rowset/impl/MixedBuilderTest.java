package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.MixedBuilderRandom;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.RangePriorityQueueBuilder;
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
        final OrderedLongSet mbi = mb.getTreeIndexImpl();
        final OrderedLongSet pqbi = pqb.getTreeIndexImpl();
        assertEquals(pqbi.ixCardinality(), mbi.ixCardinality());
        final RowSet.Iterator mbit = mbi.ixIterator();
        final RowSet.Iterator pqbit = pqbi.ixIterator();
        while (mbit.hasNext()) {
            final long mv = mbit.nextLong();
            final long mp = pqbit.nextLong();
            assertEquals(mp, mv);
        }
    }
}
