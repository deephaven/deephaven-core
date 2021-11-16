/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by;

/**
 * State factory for firstBy using an InterativeIndexState to create a redirection TrackingWritableRowSet.
 */
public class FirstBySpecImpl extends IterativeIndexSpec {
    public FirstBySpecImpl() {
        this(false, false, 0);
    }

    private FirstBySpecImpl(boolean lowestRollup, boolean secondRollup, int rollupColumnIdentifier) {
        super(lowestRollup, secondRollup, rollupColumnIdentifier);
    }

    private static final AggregationMemoKey FIRST_BY_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return FIRST_BY_INSTANCE;
    }

    @Override
    ReaggregatableStatefactory forRollup() {
        return new FirstBySpecImpl(true, false, 0);
    }

    /**
     * Sort the results by the original rowSet when aggregating on state.
     */
    @Override
    ReaggregatableStatefactory rollupFactory() {
        return new SortedFirstOrLastByFactoryImpl(true, false, true, rollupColumnIdentifier,
                REDIRECTION_INDEX_PREFIX + rollupColumnIdentifier + AggregationFactory.ROLLUP_COLUMN_SUFFIX);
    }

    @Override
    public String toString() {
        if (!lowestRollup && !secondRollup) {
            return "FirstByStateFactory";
        } else {
            return "FirstByStateFactory{" +
                    "lowestRollup=" + lowestRollup +
                    ", secondRollup=" + secondRollup +
                    ", rollupColumnIdentifier=" + rollupColumnIdentifier +
                    '}';
        }
    }
}
