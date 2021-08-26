package io.deephaven.db.v2.by;

public abstract class ReaggregatableStatefactory implements AggregationStateFactory {
    /**
     * Returns true if this state factory supports rollup.
     *
     * If this factory does not support rollup, calling forRollup and rollupFactory produce undefined results.
     *
     * @return true if forRollup() and rollupFactory() are implemented.
     */
    abstract boolean supportsRollup();

    /**
     * Returns the lowest level state factory for rollup.
     *
     * This may differ from the regular factory in that often the result column is insufficient to perform a rollup (for
     * example an average needs not just the result, but the count and sum).
     */
    abstract ReaggregatableStatefactory forRollup();

    /**
     * Returns the factory used to reaggregate the lowest or intermediate levels into the next level.
     *
     * For example, a count factory should return a sum factory to roll up the counts by summation.
     */
    abstract ReaggregatableStatefactory rollupFactory();
}
