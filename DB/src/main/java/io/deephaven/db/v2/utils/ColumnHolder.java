/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;

/**
 * Data to construct a data column.
 */
public class ColumnHolder<T> {
    public static final ColumnHolder[] ZERO_LENGTH_COLUMN_HOLDER_ARRAY = new ColumnHolder[0];

    public final String name;
    public final Class<T> dataType;
    public final Class<?> componentType;
    public final boolean grouped;
    public final Object data;

    /**
     * Construct a new set of column data.
     * 
     * @param name column name
     * @param dataType column data type
     * @param componentType column component type (for array or
     *        {@link io.deephaven.db.tables.dbarrays.DbArray>} data types)
     * @param data column data
     */
    @SuppressWarnings("unchecked")
    public ColumnHolder(String name, Class<T> dataType, Class<?> componentType, boolean grouped,
        T... data) {
        this(name, grouped, dataType, componentType, data);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data
     */
    public ColumnHolder(String name, boolean grouped, long... data) {
        this(name, grouped, long.class, null, data);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data
     */
    public ColumnHolder(String name, boolean grouped, int... data) {
        this(name, grouped, int.class, null, data);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data
     */
    public ColumnHolder(String name, boolean grouped, short... data) {
        this(name, grouped, short.class, null, data);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data
     */
    public ColumnHolder(String name, boolean grouped, char... data) {
        this(name, grouped, char.class, null, data);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data
     */
    public ColumnHolder(String name, boolean grouped, byte... data) {
        this(name, grouped, byte.class, null, data);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data
     */
    public ColumnHolder(String name, boolean grouped, double... data) {
        this(name, grouped, double.class, null, data);
    }

    /**
     * Construct a new set of column data.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data
     */
    public ColumnHolder(String name, boolean grouped, float... data) {
        this(name, grouped, float.class, null, data);
    }

    /**
     * Construct a new set of column data with a specified type. This overload allows the creation
     * of a ColumnHolder where the official data type type does not match the data.
     * 
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param dataType column data type
     * @param componentType column component type (for array or
     *        {@link io.deephaven.db.tables.dbarrays.DbArray>} data types)
     * @param data column data
     */
    private ColumnHolder(String name, boolean grouped, Class<?> dataType, Class<?> componentType,
        Object data) {
        if (!data.getClass().isArray()) {
            throw new IllegalArgumentException("Data must be provided as an array");
        }
        if (!data.getClass().getComponentType().isAssignableFrom(dataType)
            && !(dataType == DBDateTime.class && data.getClass().getComponentType() == long.class)
            && !(dataType == Boolean.class && data.getClass().getComponentType() == byte.class)) {
            throw new IllegalArgumentException("Incompatible data type: " + dataType
                + " can not be stored in array of type " + data.getClass());
        }
        this.name = NameValidator.validateColumnName(name);
        // noinspection unchecked
        this.dataType = (Class<T>) dataType;
        this.componentType = componentType;
        this.grouped = grouped;
        this.data = data;
    }

    /**
     * Create a column holder for a DateTime column where the values are represented as longs.
     * Whatever process produces a table from this column holder should respect this and create the
     * appropriate type of ColumnSource. Under normal conditions, this will be a DateTimeArraySource
     * (see {@link #getColumnSource()}).
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data (long integers representing nanos since the epoch)
     * @return a DBDateTime column holder implemented with longs for storage
     */
    public static ColumnHolder<DBDateTime> getDateTimeColumnHolder(String name, boolean grouped,
        long... data) {
        return new ColumnHolder<>(name, grouped, DBDateTime.class, null, data);
    }

    /**
     * Create a column holder for a Boolean column where the calues are represented as bytes. The
     * given byte array will be converted to a Boolean array.
     *
     * @param name column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data column data (byte values where 1 represents true, 0 represents false, and null
     *        otherwise)
     * @return a Boolean column holder
     */
    public static ColumnHolder<Boolean> getBooleanColumnHolder(String name, boolean grouped,
        byte... data) {
        final Boolean[] dbData = new Boolean[data.length];
        for (int i = 0; i < data.length; i++) {
            if (data[i] == (byte) 0) {
                dbData[i] = false;
            } else if (data[i] == (byte) 1) {
                dbData[i] = true;
            } else {
                dbData[i] = null;
            }
        }
        return new ColumnHolder<>(name, Boolean.class, null, grouped, dbData);
    }

    /**
     * Create a column holder from an array object, inferring the data type from the given array
     * object.
     *
     * @param name The column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data The data array
     * @return a column holder with a type matching the component type of the provided array
     */
    public static <T> ColumnHolder<T> createColumnHolder(String name, boolean grouped, T... data) {
        return new ColumnHolder(name, data.getClass().getComponentType(),
            data.getClass().getComponentType().getComponentType(), grouped, data);
    }

    public String getName() {
        return name;
    }

    /**
     * Gets a column source for the data. Other than the special case of DBDateTime columns, this
     * requires that the type specified match the component type of the actual data.
     *
     * @return column source constructed with data from this column holder
     */
    public ColumnSource<?> getColumnSource() {
        if (data.getClass().getComponentType().equals(dataType)) {
            return ArrayBackedColumnSource.getMemoryColumnSourceUntyped(data, dataType,
                componentType);
        } else if (dataType.equals(DBDateTime.class)
            && data.getClass().getComponentType().equals(long.class)) {
            return ArrayBackedColumnSource.getDateTimeMemoryColumnSource((long[]) data);
        } else {
            throw new IllegalStateException(
                "Unsupported column holder data & type: " + dataType.getName() + ", "
                    + data.getClass().getComponentType().getName());
        }
    }
}
