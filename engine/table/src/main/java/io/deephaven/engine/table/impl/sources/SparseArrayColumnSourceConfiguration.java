//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.configuration.Configuration;

import java.util.stream.IntStream;

/**
 * A central view of configuration options for sparse array column sources.
 */
public abstract class SparseArrayColumnSourceConfiguration {
    private static final String RECYCLER_CAPACITY_PROPERTY_PREFIX = "sparsearray.recycler.capacity.";
    private static final int DEFAULT_RECYCLER_CAPACITY_DEFAULT_VALUE = 1024;

    /**
     * The default capacity for {@link io.deephaven.util.SoftRecycler} objects if not specified by type
     */
    public static final int DEFAULT_RECYCLER_CAPACITY =
            getRecyclerCapacity("default", DEFAULT_RECYCLER_CAPACITY_DEFAULT_VALUE);

    // The 'CAPACITY' options set the type-specific default capacity of the recycler for arrays of that type.
    // They also set the default for blocks and arrays of arrays.
    public static final int BOOLEAN_RECYCLER_CAPACITY = getRecyclerCapacity("boolean", DEFAULT_RECYCLER_CAPACITY);
    public static final int BYTE_RECYCLER_CAPACITY = getRecyclerCapacity("byte", DEFAULT_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_CAPACITY = getRecyclerCapacity("char", DEFAULT_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_CAPACITY = getRecyclerCapacity("double", DEFAULT_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_CAPACITY = getRecyclerCapacity("float", DEFAULT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_CAPACITY = getRecyclerCapacity("int", DEFAULT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_CAPACITY = getRecyclerCapacity("object", DEFAULT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_CAPACITY = getRecyclerCapacity("short", DEFAULT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_CAPACITY = getRecyclerCapacity("long", DEFAULT_RECYCLER_CAPACITY);

    // The 'CAPACITY2' options set the type-specific default capacity of the recycler for arrays of arrays of that type.
    public static final int BOOLEAN_RECYCLER_CAPACITY2 = getRecyclerCapacity("boolean.2", BOOLEAN_RECYCLER_CAPACITY);
    public static final int BYTE_RECYCLER_CAPACITY2 = getRecyclerCapacity("byte.2", BYTE_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_CAPACITY2 = getRecyclerCapacity("char.2", CHAR_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_CAPACITY2 = getRecyclerCapacity("double.2", DOUBLE_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_CAPACITY2 = getRecyclerCapacity("float.2", FLOAT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_CAPACITY2 = getRecyclerCapacity("int.2", INT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_CAPACITY2 = getRecyclerCapacity("object.2", OBJECT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_CAPACITY2 = getRecyclerCapacity("short.2", SHORT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_CAPACITY2 = getRecyclerCapacity("long.2", LONG_RECYCLER_CAPACITY);

    // The 'CAPACITY1' options set the type-specific default capacity of the recycler for arrays of blocks of that type
    public static final int BOOLEAN_RECYCLER_CAPACITY1 = getRecyclerCapacity("boolean.1", BOOLEAN_RECYCLER_CAPACITY);
    public static final int BYTE_RECYCLER_CAPACITY1 = getRecyclerCapacity("byte.1", BYTE_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_CAPACITY1 = getRecyclerCapacity("char.1", CHAR_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_CAPACITY1 = getRecyclerCapacity("double.1", DOUBLE_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_CAPACITY1 = getRecyclerCapacity("float.1", FLOAT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_CAPACITY1 = getRecyclerCapacity("int.1", INT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_CAPACITY1 = getRecyclerCapacity("object.1", OBJECT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_CAPACITY1 = getRecyclerCapacity("short.1", SHORT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_CAPACITY1 = getRecyclerCapacity("long.1", LONG_RECYCLER_CAPACITY);

    // The 'CAPACITY0' options set the type-specific default capacity of the recycler for blocks of blocks of that type.
    public static final int BOOLEAN_RECYCLER_CAPACITY0 = getRecyclerCapacity("boolean.0", BOOLEAN_RECYCLER_CAPACITY);
    public static final int BYTE_RECYCLER_CAPACITY0 = getRecyclerCapacity("byte.0", BYTE_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_CAPACITY0 = getRecyclerCapacity("char.0", CHAR_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_CAPACITY0 = getRecyclerCapacity("double.0", DOUBLE_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_CAPACITY0 = getRecyclerCapacity("float.0", FLOAT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_CAPACITY0 = getRecyclerCapacity("int.0", INT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_CAPACITY0 = getRecyclerCapacity("object.0", OBJECT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_CAPACITY0 = getRecyclerCapacity("short.0", SHORT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_CAPACITY0 = getRecyclerCapacity("long.0", LONG_RECYCLER_CAPACITY);

    public static final int IN_USE_RECYCLER_CAPACITY = getInUseLowestLevelRecyclerCapacity();

    public static final int IN_USE_RECYCLER_CAPACITY2 = getInUseRecyclerCapacity2();
    public static final int IN_USE_RECYCLER_CAPACITY1 = getInUseRecyclerCapacity1();
    public static final int IN_USE_RECYCLER_CAPACITY0 = getInUseRecyclerCapacity0();

    private static final String RECYCLER_MAXIMUM_CAPACITY_PROPERTY_PREFIX = "sparsearray.recycler.maximumCapacity.";
    /**
     * The most items a recycler grows to hold if not specified by type. Unlimited by default, since the recycled items
     * are soft references, which the garbage collector reclaims under memory pressure.
     */
    public static final int DEFAULT_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("default", Integer.MAX_VALUE, 0);

    // The 'MAXIMUM_CAPACITY' options set the type-specific maximum capacity of the recycler for arrays of that type,
    // and
    // the default maximum for its other levels.
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

    public static final int BOOLEAN_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("boolean.2", BOOLEAN_RECYCLER_MAXIMUM_CAPACITY, BOOLEAN_RECYCLER_CAPACITY2);
    public static final int BYTE_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("byte.2", BYTE_RECYCLER_MAXIMUM_CAPACITY, BYTE_RECYCLER_CAPACITY2);
    public static final int CHAR_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("char.2", CHAR_RECYCLER_MAXIMUM_CAPACITY, CHAR_RECYCLER_CAPACITY2);
    public static final int DOUBLE_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("double.2", DOUBLE_RECYCLER_MAXIMUM_CAPACITY, DOUBLE_RECYCLER_CAPACITY2);
    public static final int FLOAT_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("float.2", FLOAT_RECYCLER_MAXIMUM_CAPACITY, FLOAT_RECYCLER_CAPACITY2);
    public static final int INT_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("int.2", INT_RECYCLER_MAXIMUM_CAPACITY, INT_RECYCLER_CAPACITY2);
    public static final int OBJECT_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("object.2", OBJECT_RECYCLER_MAXIMUM_CAPACITY, OBJECT_RECYCLER_CAPACITY2);
    public static final int SHORT_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("short.2", SHORT_RECYCLER_MAXIMUM_CAPACITY, SHORT_RECYCLER_CAPACITY2);
    public static final int LONG_RECYCLER_MAXIMUM_CAPACITY2 =
            getRecyclerMaximumCapacity("long.2", LONG_RECYCLER_MAXIMUM_CAPACITY, LONG_RECYCLER_CAPACITY2);

    public static final int BOOLEAN_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("boolean.1", BOOLEAN_RECYCLER_MAXIMUM_CAPACITY, BOOLEAN_RECYCLER_CAPACITY1);
    public static final int BYTE_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("byte.1", BYTE_RECYCLER_MAXIMUM_CAPACITY, BYTE_RECYCLER_CAPACITY1);
    public static final int CHAR_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("char.1", CHAR_RECYCLER_MAXIMUM_CAPACITY, CHAR_RECYCLER_CAPACITY1);
    public static final int DOUBLE_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("double.1", DOUBLE_RECYCLER_MAXIMUM_CAPACITY, DOUBLE_RECYCLER_CAPACITY1);
    public static final int FLOAT_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("float.1", FLOAT_RECYCLER_MAXIMUM_CAPACITY, FLOAT_RECYCLER_CAPACITY1);
    public static final int INT_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("int.1", INT_RECYCLER_MAXIMUM_CAPACITY, INT_RECYCLER_CAPACITY1);
    public static final int OBJECT_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("object.1", OBJECT_RECYCLER_MAXIMUM_CAPACITY, OBJECT_RECYCLER_CAPACITY1);
    public static final int SHORT_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("short.1", SHORT_RECYCLER_MAXIMUM_CAPACITY, SHORT_RECYCLER_CAPACITY1);
    public static final int LONG_RECYCLER_MAXIMUM_CAPACITY1 =
            getRecyclerMaximumCapacity("long.1", LONG_RECYCLER_MAXIMUM_CAPACITY, LONG_RECYCLER_CAPACITY1);

    public static final int BOOLEAN_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("boolean.0", BOOLEAN_RECYCLER_MAXIMUM_CAPACITY, BOOLEAN_RECYCLER_CAPACITY0);
    public static final int BYTE_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("byte.0", BYTE_RECYCLER_MAXIMUM_CAPACITY, BYTE_RECYCLER_CAPACITY0);
    public static final int CHAR_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("char.0", CHAR_RECYCLER_MAXIMUM_CAPACITY, CHAR_RECYCLER_CAPACITY0);
    public static final int DOUBLE_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("double.0", DOUBLE_RECYCLER_MAXIMUM_CAPACITY, DOUBLE_RECYCLER_CAPACITY0);
    public static final int FLOAT_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("float.0", FLOAT_RECYCLER_MAXIMUM_CAPACITY, FLOAT_RECYCLER_CAPACITY0);
    public static final int INT_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("int.0", INT_RECYCLER_MAXIMUM_CAPACITY, INT_RECYCLER_CAPACITY0);
    public static final int OBJECT_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("object.0", OBJECT_RECYCLER_MAXIMUM_CAPACITY, OBJECT_RECYCLER_CAPACITY0);
    public static final int SHORT_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("short.0", SHORT_RECYCLER_MAXIMUM_CAPACITY, SHORT_RECYCLER_CAPACITY0);
    public static final int LONG_RECYCLER_MAXIMUM_CAPACITY0 =
            getRecyclerMaximumCapacity("long.0", LONG_RECYCLER_MAXIMUM_CAPACITY, LONG_RECYCLER_CAPACITY0);

    public static final int IN_USE_RECYCLER_MAXIMUM_CAPACITY = getInUseLowestLevelRecyclerMaximumCapacity();
    public static final int IN_USE_RECYCLER_MAXIMUM_CAPACITY2 = getInUseRecyclerMaximumCapacity2();
    public static final int IN_USE_RECYCLER_MAXIMUM_CAPACITY1 = getInUseRecyclerMaximumCapacity1();
    public static final int IN_USE_RECYCLER_MAXIMUM_CAPACITY0 = getInUseRecyclerMaximumCapacity0();

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
     * The level-2 in-use recycler is shared by every element type, each of which borrows and returns one level-2 in-use
     * array with each level-2 array of previous-value blocks, so its capacity is the sum of the types' level-2
     * capacities.
     */
    private static int getInUseRecyclerCapacity2() {
        return saturatingSum(BOOLEAN_RECYCLER_CAPACITY2,
                BYTE_RECYCLER_CAPACITY2,
                CHAR_RECYCLER_CAPACITY2,
                DOUBLE_RECYCLER_CAPACITY2,
                FLOAT_RECYCLER_CAPACITY2,
                INT_RECYCLER_CAPACITY2,
                OBJECT_RECYCLER_CAPACITY2,
                SHORT_RECYCLER_CAPACITY2,
                LONG_RECYCLER_CAPACITY2);
    }

    /**
     * The level-1 in-use recycler is shared by every element type, each of which borrows and returns one level-1 in-use
     * array with each level-1 array of previous-value blocks, so its capacity is the sum of the types' level-1
     * capacities.
     */
    private static int getInUseRecyclerCapacity1() {
        return saturatingSum(BOOLEAN_RECYCLER_CAPACITY1,
                BYTE_RECYCLER_CAPACITY1,
                CHAR_RECYCLER_CAPACITY1,
                DOUBLE_RECYCLER_CAPACITY1,
                FLOAT_RECYCLER_CAPACITY1,
                INT_RECYCLER_CAPACITY1,
                OBJECT_RECYCLER_CAPACITY1,
                SHORT_RECYCLER_CAPACITY1,
                LONG_RECYCLER_CAPACITY1);
    }

    /**
     * The level-0 in-use recycler is shared by every element type, each of which borrows and returns one level-0 in-use
     * array with each level-0 array of previous-value blocks, so its capacity is the sum of the types' level-0
     * capacities.
     */
    private static int getInUseRecyclerCapacity0() {
        return saturatingSum(BOOLEAN_RECYCLER_CAPACITY0,
                BYTE_RECYCLER_CAPACITY0,
                CHAR_RECYCLER_CAPACITY0,
                DOUBLE_RECYCLER_CAPACITY0,
                FLOAT_RECYCLER_CAPACITY0,
                INT_RECYCLER_CAPACITY0,
                OBJECT_RECYCLER_CAPACITY0,
                SHORT_RECYCLER_CAPACITY0,
                LONG_RECYCLER_CAPACITY0);
    }

    /**
     * The lowest-level in-use recycler is shared by every element type, each of which borrows and returns one in-use
     * bitmap with each block of previous values, so its capacity is the sum of the types' capacities.
     */
    private static int getInUseLowestLevelRecyclerCapacity() {
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
     * The lowest-level in-use recycler's maximum, by the same reasoning as
     * {@link #getInUseLowestLevelRecyclerCapacity()}: the sum of the types' maximum capacities.
     */
    private static int getInUseLowestLevelRecyclerMaximumCapacity() {
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

    /**
     * The level-2 in-use recycler's maximum, by the same reasoning as {@link #getInUseRecyclerCapacity2()}: the sum of
     * the types' level-2 maximum capacities.
     */
    private static int getInUseRecyclerMaximumCapacity2() {
        return saturatingSum(BOOLEAN_RECYCLER_MAXIMUM_CAPACITY2,
                BYTE_RECYCLER_MAXIMUM_CAPACITY2,
                CHAR_RECYCLER_MAXIMUM_CAPACITY2,
                DOUBLE_RECYCLER_MAXIMUM_CAPACITY2,
                FLOAT_RECYCLER_MAXIMUM_CAPACITY2,
                INT_RECYCLER_MAXIMUM_CAPACITY2,
                OBJECT_RECYCLER_MAXIMUM_CAPACITY2,
                SHORT_RECYCLER_MAXIMUM_CAPACITY2,
                LONG_RECYCLER_MAXIMUM_CAPACITY2);
    }

    /**
     * The level-1 in-use recycler's maximum, by the same reasoning as {@link #getInUseRecyclerCapacity1()}: the sum of
     * the types' level-1 maximum capacities.
     */
    private static int getInUseRecyclerMaximumCapacity1() {
        return saturatingSum(BOOLEAN_RECYCLER_MAXIMUM_CAPACITY1,
                BYTE_RECYCLER_MAXIMUM_CAPACITY1,
                CHAR_RECYCLER_MAXIMUM_CAPACITY1,
                DOUBLE_RECYCLER_MAXIMUM_CAPACITY1,
                FLOAT_RECYCLER_MAXIMUM_CAPACITY1,
                INT_RECYCLER_MAXIMUM_CAPACITY1,
                OBJECT_RECYCLER_MAXIMUM_CAPACITY1,
                SHORT_RECYCLER_MAXIMUM_CAPACITY1,
                LONG_RECYCLER_MAXIMUM_CAPACITY1);
    }

    /**
     * The level-0 in-use recycler's maximum, by the same reasoning as {@link #getInUseRecyclerCapacity0()}: the sum of
     * the types' level-0 maximum capacities.
     */
    private static int getInUseRecyclerMaximumCapacity0() {
        return saturatingSum(BOOLEAN_RECYCLER_MAXIMUM_CAPACITY0,
                BYTE_RECYCLER_MAXIMUM_CAPACITY0,
                CHAR_RECYCLER_MAXIMUM_CAPACITY0,
                DOUBLE_RECYCLER_MAXIMUM_CAPACITY0,
                FLOAT_RECYCLER_MAXIMUM_CAPACITY0,
                INT_RECYCLER_MAXIMUM_CAPACITY0,
                OBJECT_RECYCLER_MAXIMUM_CAPACITY0,
                SHORT_RECYCLER_MAXIMUM_CAPACITY0,
                LONG_RECYCLER_MAXIMUM_CAPACITY0);
    }
}

