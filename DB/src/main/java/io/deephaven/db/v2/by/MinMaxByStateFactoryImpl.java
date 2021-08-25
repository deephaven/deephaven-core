/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

/**
 * Minimum and Maximum aggregation factory.
 *
 * Operates in two modes, for non-refreshing tables it requires very little state (just the current minimum or maximum).
 *
 * For refreshing tables, it requires maintaining a TreeMap of values to counts; so that if the min/max value is removed
 * we are able to identify the next lowest/highest value.
 *
 * You can use {@link AddOnlyMinMaxByStateFactoryImpl} if you want to force add-only behavior.
 *
 */
public class MinMaxByStateFactoryImpl extends ReaggregatableStatefactory {
    private final boolean minimum;
    private final boolean requireAddOnly;

    /**
     * Create a minBy or maxBy factory.
     *
     * @param minimum true if selecting the minimum value, false if selecting the maximum value.
     */
    public MinMaxByStateFactoryImpl(boolean minimum) {
        this(minimum, false);
    }

    /**
     * Create a minBy or maxBy factory.
     *
     * @param minimum true if selecting the minimum value, false if selecting the maximum value.
     * @param addOnly if true create a factory only suitable for add-only tables, if false the add-only factory will be
     *        created for non-refreshing tables and the general factory is created for refreshing tables
     */
    MinMaxByStateFactoryImpl(boolean minimum, boolean addOnly) {
        this.minimum = minimum;
        this.requireAddOnly = addOnly;
    }

    public boolean isMinimum() {
        return minimum;
    }

    private static final AggregationMemoKey MIN_INSTANCE = new AggregationMemoKey() {};
    private static final AggregationMemoKey MAX_INSTANCE = new AggregationMemoKey() {};
    private static final AggregationMemoKey ADD_ONLY_MIN_INSTANCE = new AggregationMemoKey() {};
    private static final AggregationMemoKey ADD_ONLY_MAX_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        if (requireAddOnly) {
            return minimum ? MIN_INSTANCE : MAX_INSTANCE;
        } else {
            return minimum ? ADD_ONLY_MIN_INSTANCE : ADD_ONLY_MAX_INSTANCE;
        }
    }

    @Override
    boolean supportsRollup() {
        return true;
    }

    @Override
    ReaggregatableStatefactory forRollup() {
        return this;
    }

    @Override
    ReaggregatableStatefactory rollupFactory() {
        return this;
    }

    @Override
    public String toString() {
        return (minimum ? "Min" : "Max") + "ByStateFactoryImpl";
    }
}
