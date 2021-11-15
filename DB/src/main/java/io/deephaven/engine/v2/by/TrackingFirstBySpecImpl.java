/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by;

public class TrackingFirstBySpecImpl extends IterativeIndexSpec {
    public TrackingFirstBySpecImpl() {
        this(false, false, 0);
    }

    private TrackingFirstBySpecImpl(boolean lowestRollup, boolean secondRollup, int rollupColumnIdentifier) {
        super(lowestRollup, secondRollup, rollupColumnIdentifier);
    }

    private static final AggregationMemoKey TRACKING_FIRSTBY_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return TRACKING_FIRSTBY_INSTANCE;
    }


    @Override
    ReaggregatableStatefactory forRollup() {
        return new TrackingFirstBySpecImpl(true, false, 0);
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
            return "TrackingFirstByStateFactory";
        } else {
            return "TrackingFirstByStateFactory{" +
                    "lowestRollup=" + lowestRollup +
                    ", secondRollup=" + secondRollup +
                    ", rollupColumnIdentifier=" + rollupColumnIdentifier +
                    '}';
        }
    }
}
