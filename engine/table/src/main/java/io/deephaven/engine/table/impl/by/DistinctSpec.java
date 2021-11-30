package io.deephaven.engine.table.impl.by;

/**
 * An Iterative state factory that computes the count of distinct items within a particular state. It supports rollups,
 * and converts itself into a Sum at the second level of the rollup.
 */
public class DistinctSpec extends IterativeOperatorSpec {
    private static final AggregationMemoKey NO_NULLS_INSTANCE = new AggregationMemoKey() {};
    private static final AggregationMemoKey WITH_NULLS_INSTANCE = new AggregationMemoKey() {};
    private final boolean secondRollup;
    private final boolean countNulls;

    DistinctSpec() {
        this(false, false);
    }

    DistinctSpec(boolean countNulls) {
        this(false, countNulls);
    }

    private DistinctSpec(boolean secondRollup, boolean countNulls) {
        this.secondRollup = secondRollup;
        this.countNulls = countNulls;
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        return countNulls ? WITH_NULLS_INSTANCE : NO_NULLS_INSTANCE;
    }

    @Override
    boolean supportsRollup() {
        return true;
    }

    @Override
    DistinctSpec forRollup() {
        return this;
    }

    @Override
    DistinctSpec rollupFactory() {
        return new DistinctSpec(true, countNulls);
    }

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return getDistinctChunked(type, name, countNulls, exposeInternalColumns, secondRollup);
    }

    // endregion

    public boolean countNulls() {
        return countNulls;
    }
}
