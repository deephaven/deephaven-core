/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by;

public class LastBySpecImpl extends IterativeIndexSpec {
    public LastBySpecImpl() {
        this(false, false, 0);
    }

    private LastBySpecImpl(boolean lowestRollup, boolean secondRollup, int rollupColumnIdentifier) {
        super(lowestRollup, secondRollup, rollupColumnIdentifier);
    }

    private static final AggregationMemoKey LAST_BY_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return LAST_BY_INSTANCE;
    }


    @Override
    ReaggregatableStatefactory forRollup() {
        return new LastBySpecImpl(true, false, 0);
    }

    /**
     * Sort the results by the original rowSet when aggregating on state.
     */
    @Override
    ReaggregatableStatefactory rollupFactory() {
        return new SortedFirstOrLastByFactoryImpl(false, false, true, rollupColumnIdentifier,
                REDIRECTION_INDEX_PREFIX + rollupColumnIdentifier + AggregationFactory.ROLLUP_COLUMN_SUFFIX);
    }

    @Override
    public String toString() {
        if (!lowestRollup && !secondRollup) {
            return "LastByStateFactory";
        } else {
            return "LastByStateFactory{" +
                    "lowestRollup=" + lowestRollup +
                    ", secondRollup=" + secondRollup +
                    ", rollupColumnIdentifier=" + rollupColumnIdentifier +
                    '}';
        }
    }
}
