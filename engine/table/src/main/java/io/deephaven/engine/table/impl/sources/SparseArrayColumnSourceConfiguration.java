//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.configuration.Configuration;

import java.util.Arrays;
import java.util.Collections;

/**
 * A central view of configuration options for sparse array column sources.
 */
public abstract class SparseArrayColumnSourceConfiguration {
    private static final String RECYCLER_CAPACITY_PROPERTY = "sparsearray.recycler.capacity.";
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

    /**
     * The default capacity InUse SoftRecycler for the lowest level should be the sum of all the other capacities.
     */
    public static final int IN_USE_RECYCLER_CAPACITY =
            getRecyclerCapacity("inuse", getDefaultInUseLowestLevelRecyclerCapacity());

    /**
     * The default capacity InUse SoftRecycler for all levels other than the lowest should be the max recycler capacity
     * for any type.
     */
    public static final int IN_USE_RECYCLER_CAPACITY2 =
            getRecyclerCapacity("inuse.2", getDefaultInUseRecyclerCapacity2());
    public static final int IN_USE_RECYCLER_CAPACITY1 =
            getRecyclerCapacity("inuse.1", getDefaultInUseRecyclerCapacity1());
    public static final int IN_USE_RECYCLER_CAPACITY0 =
            getRecyclerCapacity("inuse.0", getDefaultInUseRecyclerCapacity0());

    private static int getRecyclerCapacity(final String thisType, final int defaultValue) {
        return Configuration.getInstance().getIntegerWithDefault(RECYCLER_CAPACITY_PROPERTY + thisType, defaultValue);
    }

    private static int getDefaultInUseRecyclerCapacity2() {
        final Integer[] CAPACITIES = {BOOLEAN_RECYCLER_CAPACITY2,
                BYTE_RECYCLER_CAPACITY2,
                CHAR_RECYCLER_CAPACITY2,
                DOUBLE_RECYCLER_CAPACITY2,
                FLOAT_RECYCLER_CAPACITY2,
                INT_RECYCLER_CAPACITY2,
                OBJECT_RECYCLER_CAPACITY2,
                SHORT_RECYCLER_CAPACITY2,
                LONG_RECYCLER_CAPACITY2
        };
        return Collections.max(Arrays.asList(CAPACITIES));
    }

    private static int getDefaultInUseRecyclerCapacity1() {
        final Integer[] CAPACITIES = {BOOLEAN_RECYCLER_CAPACITY1,
                BYTE_RECYCLER_CAPACITY1,
                CHAR_RECYCLER_CAPACITY1,
                DOUBLE_RECYCLER_CAPACITY1,
                FLOAT_RECYCLER_CAPACITY1,
                INT_RECYCLER_CAPACITY1,
                OBJECT_RECYCLER_CAPACITY1,
                SHORT_RECYCLER_CAPACITY1,
                LONG_RECYCLER_CAPACITY1
        };
        return Collections.max(Arrays.asList(CAPACITIES));
    }

    private static int getDefaultInUseRecyclerCapacity0() {
        final Integer[] CAPACITIES = {BOOLEAN_RECYCLER_CAPACITY0,
                BYTE_RECYCLER_CAPACITY0,
                CHAR_RECYCLER_CAPACITY0,
                DOUBLE_RECYCLER_CAPACITY0,
                FLOAT_RECYCLER_CAPACITY0,
                INT_RECYCLER_CAPACITY0,
                OBJECT_RECYCLER_CAPACITY0,
                SHORT_RECYCLER_CAPACITY0,
                LONG_RECYCLER_CAPACITY0
        };
        return Collections.max(Arrays.asList(CAPACITIES));
    }

    private static int getDefaultInUseLowestLevelRecyclerCapacity() {
        return BOOLEAN_RECYCLER_CAPACITY
                + BYTE_RECYCLER_CAPACITY
                + CHAR_RECYCLER_CAPACITY
                + DOUBLE_RECYCLER_CAPACITY
                + FLOAT_RECYCLER_CAPACITY
                + INT_RECYCLER_CAPACITY
                + OBJECT_RECYCLER_CAPACITY
                + SHORT_RECYCLER_CAPACITY
                + LONG_RECYCLER_CAPACITY;
    }
}

