package io.deephaven.engine.v2.utils;

import io.deephaven.engine.v2.utils.rsp.RspBitmap;
import io.deephaven.engine.v2.utils.sortedranges.SortedRanges;

class AdaptiveTreeIndexImplBuilderRandom extends AbstractTreeIndexImplBuilderRandom {

    private MixedBuilderRandom builder = null;

    @Override
    protected TreeIndexImpl.BuilderRandom innerBuilder() {
        return builder;
    }

    @Override
    protected void setupInnerBuilderForRange(final long start, final long endInclusive) {
        setupInnerBuilderEmpty();
        builder.addRange(start, endInclusive);
    }

    @Override
    protected void setupInnerBuilderEmpty() {
        builder = new MixedBuilderRandom(2 * SortedRanges.MAX_CAPACITY);
    }

    @Override
    protected void setInnerBuilderNull() {
        builder = null;
    }

    @Override
    public void add(final SortedRanges ix, final boolean acquire) {
        builder.add(ix, acquire);
    }

    @Override
    public void add(final RspBitmap ix, final boolean acquire) {
        builder.add(ix, acquire);
    }
}
