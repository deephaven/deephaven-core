package io.deephaven.engine.v2.utils;

/**
 * {@link RowSetBuilderRandom} implementation that uses an adaptive pattern based on workload.
 */
class AdaptiveRowSetBuilderRandom implements RowSetBuilderRandom {

    private final AdaptiveOrderedLongSetBuilderRandom builder = new AdaptiveOrderedLongSetBuilderRandom();

    @Override
    public MutableRowSet build() {
        return new MutableRowSetImpl(builder.getTreeIndexImpl());
    }

    @Override
    public void addKey(final long rowKey) {
        builder.addKey(rowKey);
    }

    @Override
    public void addRange(final long firstRowKey, final long lastRowKey) {
        builder.addRange(firstRowKey, lastRowKey);
    }
}
