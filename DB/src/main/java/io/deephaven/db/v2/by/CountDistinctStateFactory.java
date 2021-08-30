package io.deephaven.db.v2.by;

/**
 * An Iterative state factory that computes the count of distinct items within a particular state.
 */
public class CountDistinctStateFactory extends IterativeOperatorStateFactory {
    private static final AggregationMemoKey NO_NULLS_INSTANCE = new AggregationMemoKey() {};
    private static final AggregationMemoKey WITH_NULLS_INSTANCE = new AggregationMemoKey() {};
    private final boolean secondRollup;
    private final boolean countNulls;

    CountDistinctStateFactory() {
        this(false, false);
    }

    CountDistinctStateFactory(boolean countNulls) {
        this(false, countNulls);
    }

    private CountDistinctStateFactory(boolean secondRollup, boolean countNulls) {
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
    CountDistinctStateFactory forRollup() {
        return this;
    }

    @Override
    CountDistinctStateFactory rollupFactory() {
        return new CountDistinctStateFactory(true, countNulls);
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
