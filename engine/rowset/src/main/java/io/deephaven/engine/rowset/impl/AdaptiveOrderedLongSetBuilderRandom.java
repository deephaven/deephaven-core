package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;

class AdaptiveOrderedLongSetBuilderRandom extends AbstractOrderedLongSetBuilderRandom {

    private MixedBuilderRandom builder = null;

    @Override
    protected OrderedLongSet.BuilderRandom innerBuilder() {
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
