//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.configuration.Configuration;

import java.util.Arrays;
import java.util.Collections;

/**
 * A central view of configuration options for array column sources.
 */
public class ArrayColumnSourceConfiguration {
    private static final String RECYCLER_CAPACITY_PROPERTY = "array.recycler.capacity.";
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

    /**
     * The default capacity InUse SoftRecycler for all levels other than the lowest should be the max recycler capacity
     * for any type.
     */
    public static final int IN_USE_RECYCLER_CAPACITY = getRecyclerCapacity("inuse", getDefaultInUseRecyclerCapacity());

    private static int getRecyclerCapacity(final String thisType, final int defaultValue) {
        return Configuration.getInstance().getIntegerWithDefault(RECYCLER_CAPACITY_PROPERTY + thisType, defaultValue);
    }

    private static int getDefaultInUseRecyclerCapacity() {
        final Integer[] CAPACITIES = {BOOLEAN_RECYCLER_CAPACITY,
                BYTE_RECYCLER_CAPACITY,
                CHAR_RECYCLER_CAPACITY,
                DOUBLE_RECYCLER_CAPACITY,
                FLOAT_RECYCLER_CAPACITY,
                INT_RECYCLER_CAPACITY,
                OBJECT_RECYCLER_CAPACITY,
                SHORT_RECYCLER_CAPACITY,
                LONG_RECYCLER_CAPACITY};
        return Collections.max(Arrays.asList(CAPACITIES));
    }
}

