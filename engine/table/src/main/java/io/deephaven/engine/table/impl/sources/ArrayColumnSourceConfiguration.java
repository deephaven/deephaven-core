//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.configuration.Configuration;

import java.util.stream.IntStream;

/**
 * A central view of configuration options for array column sources.
 */
public class ArrayColumnSourceConfiguration {
    private static final String RECYCLER_CAPACITY_PROPERTY_PREFIX = "array.recycler.capacity.";
    private static final int DEFAULT_RECYCLER_CAPACITY_DEFAULT_VALUE = 1024;
    public static final int DEFAULT_RECYCLER_CAPACITY =
            getRecyclerCapacity("default", DEFAULT_RECYCLER_CAPACITY_DEFAULT_VALUE);
    public static final int BOOLEAN_RECYCLER_CAPACITY = getRecyclerCapacity("boolean", DEFAULT_RECYCLER_CAPACITY);
    public static final int BYTE_RECYCLER_CAPACITY = getRecyclerCapacity("byte", DEFAULT_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_CAPACITY = getRecyclerCapacity("char", DEFAULT_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_CAPACITY = getRecyclerCapacity("double", DEFAULT_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_CAPACITY = getRecyclerCapacity("float", DEFAULT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_CAPACITY = getRecyclerCapacity("int", DEFAULT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_CAPACITY = getRecyclerCapacity("object", DEFAULT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_CAPACITY = getRecyclerCapacity("short", DEFAULT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_CAPACITY = getRecyclerCapacity("long", DEFAULT_RECYCLER_CAPACITY);

    public static final int IN_USE_RECYCLER_CAPACITY = getInUseRecyclerCapacity();

    private static final String RECYCLER_MAXIMUM_CAPACITY_PROPERTY_PREFIX = "array.recycler.maximumCapacity.";
    /**
     * The most items a recycler grows to hold if not specified by type. Unlimited by default, since the recycled items
     * are soft references, which the garbage collector reclaims under memory pressure.
     */
    public static final int DEFAULT_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("default", Integer.MAX_VALUE, 0);
    public static final int BOOLEAN_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("boolean", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, BOOLEAN_RECYCLER_CAPACITY);
    public static final int BYTE_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("byte", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, BYTE_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("char", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, CHAR_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("double", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, DOUBLE_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("float", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, FLOAT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("int", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, INT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("object", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, OBJECT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("short", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, SHORT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("long", DEFAULT_RECYCLER_MAXIMUM_CAPACITY, LONG_RECYCLER_CAPACITY);
    public static final int IN_USE_RECYCLER_MAXIMUM_CAPACITY = getInUseRecyclerMaximumCapacity();

    private static int getRecyclerCapacity(final String thisType, final int defaultValue) {
        return Configuration.getInstance().getIntegerWithDefault(RECYCLER_CAPACITY_PROPERTY_PREFIX + thisType,
                defaultValue);
    }

    /**
     * @return the configured maximum capacity for {@code thisType}, but never less than {@code capacity}, the capacity
     *         its recycler starts with
     */
    private static int getRecyclerMaximumCapacity(final String thisType, final int defaultValue, final int capacity) {
        return Math.max(capacity, Configuration.getInstance()
                .getIntegerWithDefault(RECYCLER_MAXIMUM_CAPACITY_PROPERTY_PREFIX + thisType, defaultValue));
    }

    /**
     * @return the sum of {@code values}, or {@link Integer#MAX_VALUE} if the sum would exceed it, as it does whenever a
     *         value is the unlimited default maximum capacity
     */
    private static int saturatingSum(final int... values) {
        return (int) Math.min(Integer.MAX_VALUE, IntStream.of(values).asLongStream().sum());
    }

    /**
     * The in-use recycler is shared by every element type, each of which borrows and returns one in-use bitmap with
     * each block of previous values, so its capacity is the sum of the types' capacities.
     */
    private static int getInUseRecyclerCapacity() {
        return saturatingSum(BOOLEAN_RECYCLER_CAPACITY,
                BYTE_RECYCLER_CAPACITY,
                CHAR_RECYCLER_CAPACITY,
                DOUBLE_RECYCLER_CAPACITY,
                FLOAT_RECYCLER_CAPACITY,
                INT_RECYCLER_CAPACITY,
                OBJECT_RECYCLER_CAPACITY,
                SHORT_RECYCLER_CAPACITY,
                LONG_RECYCLER_CAPACITY);
    }

    /**
     * The in-use recycler's maximum, by the same reasoning as {@link #getInUseRecyclerCapacity()}: the sum of the
     * types' maximum capacities.
     */
    private static int getInUseRecyclerMaximumCapacity() {
        return saturatingSum(BOOLEAN_RECYCLER_MAXIMUM_CAPACITY,
                BYTE_RECYCLER_MAXIMUM_CAPACITY,
                CHAR_RECYCLER_MAXIMUM_CAPACITY,
                DOUBLE_RECYCLER_MAXIMUM_CAPACITY,
                FLOAT_RECYCLER_MAXIMUM_CAPACITY,
                INT_RECYCLER_MAXIMUM_CAPACITY,
                OBJECT_RECYCLER_MAXIMUM_CAPACITY,
                SHORT_RECYCLER_MAXIMUM_CAPACITY,
                LONG_RECYCLER_MAXIMUM_CAPACITY);
    }
}

