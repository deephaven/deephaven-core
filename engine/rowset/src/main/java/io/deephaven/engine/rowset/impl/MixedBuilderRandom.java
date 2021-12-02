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
        accumIndex = accumIndex.ixInsert(ix);
        ix.ixRelease();
    }

    private void newPq() {
        pqb.reset();
    }

    private void checkPqSize() {
        if (pqb.size() < pqSizeThreshold) {
            return;
        }
        final OrderedLongSet ix = pqb.getTreeIndexImplAndReset();
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

    private void addTreeIndexImpl(final OrderedLongSet ix) {
        if (ix.ixCardinality() >= addAsIndexThreshold) {
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
        addTreeIndexImpl(ix);
    }

    @Override
    public void add(final RspBitmap ix, final boolean acquire) {
        addTreeIndexImpl(ix);
    }

    @Override
    public OrderedLongSet getTreeIndexImpl() {
        final OrderedLongSet ix = pqb.getTreeIndexImpl();
        pqb = null;
        merge(ix);
        final OrderedLongSet ans = accumIndex;
        accumIndex = null;
        return ans;
    }
}
