/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

/**
 * State factory for firstBy using an InterativeIndexState to create a redirection Index.
 */
public class FirstByStateFactoryImpl extends IterativeIndexStateFactory {
    public FirstByStateFactoryImpl() {
        this(false, false, 0);
    }

    private FirstByStateFactoryImpl(boolean lowestRollup, boolean secondRollup,
        int rollupColumnIdentifier) {
        super(lowestRollup, secondRollup, rollupColumnIdentifier);
    }

    private static final AggregationMemoKey FIRST_BY_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return FIRST_BY_INSTANCE;
    }

    @Override
    ReaggregatableStatefactory forRollup() {
        return new FirstByStateFactoryImpl(true, false, 0);
    }

    /**
     * Sort the results by the original index when aggregating on state.
     */
    @Override
    ReaggregatableStatefactory rollupFactory() {
        return new SortedFirstOrLastByFactoryImpl(true, false, true, rollupColumnIdentifier,
            REDIRECTION_INDEX_PREFIX + rollupColumnIdentifier
                + ComboAggregateFactory.ROLLUP_COLUMN_SUFFIX);
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
