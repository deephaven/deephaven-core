/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

public class TrackingLastByStateFactoryImpl extends IterativeIndexStateFactory {
    public TrackingLastByStateFactoryImpl() {
        this(false, false, 0);
    }

    private TrackingLastByStateFactoryImpl(boolean lowestRollup, boolean secondRollup, int rollupColumnIdentifier) {
        super(lowestRollup, secondRollup, rollupColumnIdentifier);
    }

    private static final AggregationMemoKey TRACKING_LASTBY_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return TRACKING_LASTBY_INSTANCE;
    }


    @Override
    ReaggregatableStatefactory forRollup() {
        return new TrackingLastByStateFactoryImpl(true, false, 0);
    }

    /**
     * Sort the results by the original index when aggregating on state.
     */
    @Override
    ReaggregatableStatefactory rollupFactory() {
        return new SortedFirstOrLastByFactoryImpl(false, false, true, rollupColumnIdentifier,
                REDIRECTION_INDEX_PREFIX + rollupColumnIdentifier + ComboAggregateFactory.ROLLUP_COLUMN_SUFFIX);
    }

    @Override
    public String toString() {
        if (!lowestRollup && !secondRollup) {
            return "TrackingLastByStateFactory";
        } else {
            return "TrackingLastByStateFactory{" +
                    "lowestRollup=" + lowestRollup +
                    ", secondRollup=" + secondRollup +
                    ", rollupColumnIdentifier=" + rollupColumnIdentifier +
                    '}';
        }
    }
}
