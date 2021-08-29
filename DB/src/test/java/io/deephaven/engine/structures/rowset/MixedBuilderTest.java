package io.deephaven.engine.structures.rowset;

import io.deephaven.engine.structures.rowset.Index;
import io.deephaven.engine.structures.rowset.MixedBuilder;
import io.deephaven.engine.structures.rowset.RangePriorityQueueBuilder;
import io.deephaven.engine.structures.rowset.TreeIndexImpl;
import junit.framework.TestCase;

import java.util.Random;

public class MixedBuilderTest extends TestCase {
    public void testMixedBuilder() {
        final Random r = new Random();
        final MixedBuilder mb = new MixedBuilder(16);
        final RangePriorityQueueBuilder pqb = new RangePriorityQueueBuilder(16);
        final int sz = 1024 * 1024;
        for (int i = 0; i < sz; ++i) {
            final long n = r.nextInt();
            final long v = (n < 0) ? -n : n;
            mb.addKey(v);
            pqb.addKey(v);
        }
        final TreeIndexImpl mbi = mb.getTreeIndexImpl();
        final TreeIndexImpl pqbi = pqb.getTreeIndexImpl();
        assertEquals(pqbi.ixCardinality(), mbi.ixCardinality());
        final Index.Iterator mbit = mbi.ixIterator();
        final Index.Iterator pqbit = pqbi.ixIterator();
        while (mbit.hasNext()) {
            final long mv = mbit.nextLong();
            final long mp = pqbit.nextLong();
            assertEquals(mp, mv);
        }
    }
}
