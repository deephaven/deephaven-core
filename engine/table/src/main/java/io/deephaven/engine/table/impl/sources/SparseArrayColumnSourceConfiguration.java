//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.configuration.Configuration;

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
    public static final int BYTE_RECYCLER_CAPACITY = getRecyclerCapacity("byte", DEFAULT_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_CAPACITY = getRecyclerCapacity("char", DEFAULT_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_CAPACITY = getRecyclerCapacity("double", DEFAULT_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_CAPACITY = getRecyclerCapacity("float", DEFAULT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_CAPACITY = getRecyclerCapacity("int", DEFAULT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_CAPACITY = getRecyclerCapacity("object", DEFAULT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_CAPACITY = getRecyclerCapacity("short", DEFAULT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_CAPACITY = getRecyclerCapacity("long", DEFAULT_RECYCLER_CAPACITY);

    // The 'CAPACITY2' options set the type-specific default capacity of the recycler for arrays of arrays of that type.
    public static final int BYTE_RECYCLER_CAPACITY2 = getRecyclerCapacity("byte.2", BYTE_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_CAPACITY2 = getRecyclerCapacity("char.2", CHAR_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_CAPACITY2 = getRecyclerCapacity("double.2", DOUBLE_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_CAPACITY2 = getRecyclerCapacity("float.2", FLOAT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_CAPACITY2 = getRecyclerCapacity("int.2", INT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_CAPACITY2 = getRecyclerCapacity("object.2", OBJECT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_CAPACITY2 = getRecyclerCapacity("short.2", SHORT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_CAPACITY2 = getRecyclerCapacity("long.2", LONG_RECYCLER_CAPACITY);

    // The 'CAPACITY1' options set the type-specific default capacity of the recycler for arrays of blocks of that type
    public static final int BYTE_RECYCLER_CAPACITY1 = getRecyclerCapacity("byte.1", BYTE_RECYCLER_CAPACITY);
    public static final int CHAR_RECYCLER_CAPACITY1 = getRecyclerCapacity("char.1", CHAR_RECYCLER_CAPACITY);
    public static final int DOUBLE_RECYCLER_CAPACITY1 = getRecyclerCapacity("double.1", DOUBLE_RECYCLER_CAPACITY);
    public static final int FLOAT_RECYCLER_CAPACITY1 = getRecyclerCapacity("float.1", FLOAT_RECYCLER_CAPACITY);
    public static final int INT_RECYCLER_CAPACITY1 = getRecyclerCapacity("int.1", INT_RECYCLER_CAPACITY);
    public static final int OBJECT_RECYCLER_CAPACITY1 = getRecyclerCapacity("object.1", OBJECT_RECYCLER_CAPACITY);
    public static final int SHORT_RECYCLER_CAPACITY1 = getRecyclerCapacity("short.1", SHORT_RECYCLER_CAPACITY);
    public static final int LONG_RECYCLER_CAPACITY1 = getRecyclerCapacity("long.1", LONG_RECYCLER_CAPACITY);


    private static final String RECYCLER_MAXIMUM_CAPACITY_PROPERTY_PREFIX = "sparsearray.recycler.maximumCapacity.";
    /**
     * The most items a recycler grows to hold if not specified by type. Unlimited by default, since the recycled items
     * are soft references, which the garbage collector reclaims under memory pressure.
     */
    public static final int DEFAULT_RECYCLER_MAXIMUM_CAPACITY =
            getRecyclerMaximumCapacity("default", Integer.MAX_VALUE, 0);

    // The 'MAXIMUM_CAPACITY' options set the type-specific maximum capacity of the recycler for arrays of that type,
    // and the default maximum for its other levels.
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
}

