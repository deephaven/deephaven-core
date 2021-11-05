package io.deephaven.engine.v2.utils;

import io.deephaven.engine.v2.utils.rsp.RspBitmap;

/**
 * {@link RowSetBuilderRandom} implementation that uses a {@link RspBitmap.BuilderRandom} internally.
 */
class BasicRowSetBuilderRandom extends AbstractOrderedLongSetBuilderRandom
        implements RowSetBuilderRandom {

    private RspBitmap.BuilderRandom builder;

    @Override
    public MutableRowSet build() {
        return new MutableRowSetImpl(getTreeIndexImpl());
    }

    @Override
    protected OrderedLongSet.BuilderRandom innerBuilder() {
        return builder;
    }

    @Override
    protected void setupInnerBuilderForRange(final long start, final long end) {
        builder = new RspBitmap.BuilderRandom(indexCounts, start, end);
    }

    @Override
    protected void setupInnerBuilderEmpty() {
        builder = new RspBitmap.BuilderRandom(indexCounts);
    }

    @Override
    protected void setInnerBuilderNull() {
        builder = null;
    }
}
