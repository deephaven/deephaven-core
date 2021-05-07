/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBNameValidator;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;

/**
 * Data to construct a data column.
 *
 * @IncludeAll
 */
public class ColumnHolder {
    public static final ColumnHolder[] ZERO_LENGTH_COLUMN_HOLDER_ARRAY = new ColumnHolder[0];

    public final String name;
    public final Class type;
    public final Object data;
    public final boolean grouped;

    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     * @param <T>     column element type
     */
    @SuppressWarnings("unchecked")
    public <T> ColumnHolder(String name, boolean grouped, T... data) {
        type = data.getClass().getComponentType();
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }


    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param data    column data
     * @param <T>     column element type
     */
    @SuppressWarnings("unchecked")
    public <T> ColumnHolder(String name, Class<T> clazz, T... data) {
        type = clazz;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = false;
    }

    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     */
    public ColumnHolder(String name, boolean grouped, long... data) {
        type = long.class;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     */
    public ColumnHolder(String name, boolean grouped, int... data) {
        type = int.class;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     */
    public ColumnHolder(String name, boolean grouped, short... data) {
        type = short.class;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     */
    public ColumnHolder(String name, boolean grouped, char... data) {
        type = char.class;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     */
    public ColumnHolder(String name, boolean grouped, byte... data) {
        type = byte.class;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     */
    public ColumnHolder(String name, boolean grouped, double... data) {
        type = double.class;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    /**
     * Construct a new set of column data.
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     */
    public ColumnHolder(String name, boolean grouped, float... data) {
        type = float.class;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    /**
     * Construct a new set of column data with a specified type.
     * This overload allows the creation of a ColumnHolder where the official data type type does not match the data.
     *
     * @param name    column name
     * @param type    abstract data type for the column
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data
     */
    private ColumnHolder(String name, Class type, boolean grouped, Object data) {
        this.type = type;
        this.data = data;
        this.name = DBNameValidator.validateColumnName(name);
        this.grouped = grouped;
    }

    /**
     * Create a column holder for a DateTime column where the values are represented as longs.
     * Whatever process produces a table from this column holder should respect this and create the appropriate type
     * of ColumnSource. Under normal conditions, this will be a DateTimeArraySource (see {@link #getColumnSource()}).
     *
     * @param name    column name
     * @param grouped true if the column is grouped; false otherwise
     * @param data    column data (long integers representing nanos since the epoch)
     * @return a DBDateTime column holder implemented with longs for storage
     */
    public static ColumnHolder getDateTimeColumnHolder(String name, boolean grouped, long... data) {
        return new ColumnHolder(name, DBDateTime.class, grouped, data);
    }

    /**
     * Create a column holder for a Boolean column where the calues are represented as bytes.
     * The given byte array will be converted to a Boolean array.
     *
     * @param name      column name
     * @param grouped   true if the column is grouped; false otherwise
     * @param data      column data (byte values where 1 represents true, 0 represents false, and null otherwise)
     * @return a Boolean column holder
     */
    public static ColumnHolder getBooleanColumnHolder(String name, boolean grouped, byte... data) {
        final Boolean[] dbData = new Boolean[data.length];
        for(int i = 0; i < data.length; i++) {
            if (data[i] == (byte)0) {
                dbData[i] = false;
            } else if(data[i] == (byte)1) {
                dbData[i] = true;
            } else {
                dbData[i] = null;
            }
        }
        return new ColumnHolder(name, Boolean.class, grouped, dbData);
    }

    /**
     * Create a column holder from an array object, inferring the data type from the given array object.
     *
     * @param name      column name
     * @param grouped   true if the column is grouped; false otherwise
     * @param data     an object holding the column data (must be an array)
     * @return a column holder with a type matching the component type of the provided array
     */
    public static ColumnHolder getColumnHolderFromArray(String name, boolean grouped, Object data) {
        if(!data.getClass().isArray()) {
            throw new IllegalArgumentException("Data must be provided as an array");
        }
        return new ColumnHolder(name, data.getClass().getComponentType(),
                grouped, data);
    }

    public String getName() {
        return name;
    }

    /**
     * Gets a column source for the data. Other than the special case of DBDateTime columns, this requires
     * that the type specified match the component type of the actual data.
     *
     * @return column source constructed with data from this column holder
     */
    public ColumnSource getColumnSource() {
        if (data.getClass().getComponentType().equals(type)) {
            return ArrayBackedColumnSource.getMemoryColumnSource(data);
        } else if (type.equals(DBDateTime.class) && data.getClass().getComponentType().equals(long.class)) {
            return ArrayBackedColumnSource.getDateTimeMemoryColumnSource((long[]) data);
        } else {
            throw new IllegalStateException("Unsupported column holder data & type: " + type.getName() + ", "
                    + data.getClass().getComponentType().getName());
        }
    }
}
