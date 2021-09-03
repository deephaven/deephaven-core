package io.deephaven.engine.structures.rowset;

import io.deephaven.engine.structures.rowset.rsp.RspBitmap;

public class TreeIndexImplRandomBuilder extends AbstractTreeIndexImplRandomBuilder {
    private RspBitmap.RandomBuilder builder = null;

    @Override
    protected TreeIndexImpl.RandomBuilder innerBuilder() {
        return builder;
    }

    @Override
    protected void setupInnerBuilderForRange(final long start, final long end) {
        builder = new RspBitmap.RandomBuilder(indexCounts, start, end);
    }

    @Override
    protected void setupInnerBuilderEmpty() {
        builder = new RspBitmap.RandomBuilder(indexCounts);
    }

    @Override
    protected void setInnerBuilderNull() {
        builder = null;
    }
}
