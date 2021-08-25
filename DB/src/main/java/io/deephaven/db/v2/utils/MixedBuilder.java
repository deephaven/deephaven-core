package io.deephaven.db.v2.utils;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;

public class MixedBuilder implements TreeIndexImpl.RandomBuilder {
    protected RangePriorityQueueBuilder pqb;
    private TreeIndexImpl accumIndex;

    private static final int pqSizeThreshold =
        Configuration.getInstance().getIntegerForClassWithDefault(
            MixedBuilder.class, "pqSizeThreshold", 2 * 1024 * 1024);

    private static final int addAsIndexThreshold =
        Configuration.getInstance().getIntegerForClassWithDefault(
            MixedBuilder.class, "addAsIndexThreshold", 64 * 1024);

    public MixedBuilder(final int pqInitialCapacity) {
        accumIndex = null;
        pqb = new RangePriorityQueueBuilder(pqInitialCapacity);
    }

    private void merge(final TreeIndexImpl ix) {
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
        final TreeIndexImpl ix = pqb.getTreeIndexImplAndReset();
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

    private void addTreeIndexImpl(final TreeIndexImpl ix) {
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
    public TreeIndexImpl getTreeIndexImpl() {
        final TreeIndexImpl ix = pqb.getTreeIndexImpl();
        pqb = null;
        merge(ix);
        final TreeIndexImpl ans = accumIndex;
        accumIndex = null;
        return ans;
    }
}
