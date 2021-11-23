package io.deephaven.engine.table.impl.by;

/**
 * An Iterative state factory that computes the count of distinct items within a particular state.
 */
public class CountDistinctSpec extends IterativeOperatorSpec {
    private static final AggregationMemoKey NO_NULLS_INSTANCE = new AggregationMemoKey() {};
    private static final AggregationMemoKey WITH_NULLS_INSTANCE = new AggregationMemoKey() {};
    private final boolean secondRollup;
    private final boolean countNulls;

    CountDistinctSpec() {
        this(false, false);
    }

    CountDistinctSpec(boolean countNulls) {
        this(false, countNulls);
    }

    private CountDistinctSpec(boolean secondRollup, boolean countNulls) {
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
    CountDistinctSpec forRollup() {
        return this;
    }

    @Override
    CountDistinctSpec rollupFactory() {
        return new CountDistinctSpec(true, countNulls);
    }

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return getCountDistinctChunked(type, name, countNulls, exposeInternalColumns, secondRollup);
    }

    // endregion

    public boolean countNulls() {
        return countNulls;
    }
}
