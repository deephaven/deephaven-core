/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.tablelogger;

/**
 * Type-safe interface for setting cell values in individual columns of a row to allow a row to be
 * written.
 */
public interface RowSetter<T> {
    void set(T value);

    void setBoolean(Boolean value);

    void setByte(byte value);

    void setChar(char value);

    void setDouble(double value);

    void setFloat(float value);

    void setInt(int value);

    void setLong(long value);

    void setShort(short value);

    Class getType();
}
