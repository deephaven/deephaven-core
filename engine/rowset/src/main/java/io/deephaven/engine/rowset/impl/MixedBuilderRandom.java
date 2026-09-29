//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;

public class MixedBuilderRandom implements OrderedLongSet.BuilderRandom {
    protected RangePriorityQueueBuilder pqb;
    private OrderedLongSet accumIndex;

    private static final int pqSizeThreshold = Configuration.getInstance().getIntegerForClassWithDefault(
            MixedBuilderRandom.class, "pqSizeThreshold", 2 * 1024 * 1024);

    private static final int addAsIndexThreshold = Configuration.getInstance().getIntegerForClassWithDefault(
            MixedBuilderRandom.class, "addAsIndexThreshold", 64 * 1024);

    public MixedBuilderRandom(final int pqInitialCapacity) {
        accumIndex = null;
        pqb = new RangePriorityQueueBuilder(pqInitialCapacity);
    }

    private void merge(final OrderedLongSet ix) {
        if (accumIndex == null) {
            accumIndex = ix;
            return;
        }
        // ixInsert may return a new object (copy-on-write when accumIndex is shared); release the
        // replaced reference or it is leaked.
        final OrderedLongSet newAccumIndex = accumIndex.ixInsert(ix);
        if (newAccumIndex != accumIndex) {
            accumIndex.ixRelease();
        }
        accumIndex = newAccumIndex;
        ix.ixRelease();
    }

    private void newPq() {
        pqb.reset();
    }

    private void checkPqSize() {
        if (pqb.size() < pqSizeThreshold) {
            return;
        }
        final OrderedLongSet ix = pqb.getOrderedLongSetAndReset();
        merge(ix);
        newPq();
    }

    @Override
    public void addKey(final long key) {
        checkPqSize();
        pqb.addKey(key);
    }

    @Override
    public void addRange(final long startKey, final long endKey) {
        checkPqSize();
        pqb.addRange(startKey, endKey);
    }

    private void addOrderedLongSet(final OrderedLongSet ix) {
        if (ix.ixIsEmpty()) {
            return;
        }
        // A union into the accumulator is linear in the accumulator, except when the set lies past it, which appends
        // and costs only the set itself. The queue holds ranges, so what a set costs there is its range count, not its
        // cardinality, and the queue sorts it for free. So: append when we can, union when the set has too many ranges
        // to queue, and queue the rest. The first set seeds the accumulator, so an ascending stream of sets appends.
        final boolean appends = accumIndex == null || ix.ixFirstKey() > accumIndex.ixLastKey();
        if (appends || ix.ixRangesCountUpperBound() >= addAsIndexThreshold) {
            merge(ix.ixCowRef());
            return;
        }
        ix.ixForEachLongRange((final long start, final long end) -> {
            addRange(start, end);
            return true;
        });
    }

    @Override
    public void add(final SortedRanges ix, final boolean acquire) {
        addOrderedLongSet(ix);
    }

    @Override
    public void add(final RspBitmap ix, final boolean acquire) {
        addOrderedLongSet(ix);
    }

    @Override
    public OrderedLongSet getOrderedLongSet() {
        final OrderedLongSet ix = pqb.getOrderedLongSet();
        pqb = null;
        merge(ix);
        final OrderedLongSet ans = accumIndex;
        accumIndex = null;
        return ans;
    }
}
