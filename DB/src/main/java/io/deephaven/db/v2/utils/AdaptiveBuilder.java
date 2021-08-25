package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;

public class AdaptiveBuilder extends AbstractTreeIndexImplRandomBuilder {
    private MixedBuilder builder = null;

    @Override
    protected TreeIndexImpl.RandomBuilder innerBuilder() {
        return builder;
    }

    @Override
    protected void setupInnerBuilderForRange(final long start, final long endInclusive) {
        setupInnerBuilderEmpty();
        builder.addRange(start, endInclusive);
    }

    @Override
    protected void setupInnerBuilderEmpty() {
        builder = new MixedBuilder(2 * SortedRanges.MAX_CAPACITY);
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
