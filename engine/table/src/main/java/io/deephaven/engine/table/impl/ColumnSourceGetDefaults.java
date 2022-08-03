/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTime;

import static io.deephaven.time.DateTimeUtils.nanosToTime;
import static io.deephaven.util.type.TypeUtils.box;

/**
 * Defaulted interfaces for various base {@link ColumnSource} types, in order to avoid having defaults at higher levels
 * in the class hierarchy.
 */
public final class ColumnSourceGetDefaults {

    /**
     * Default interface for Object {@link ColumnSource} implementations.
     */
    public interface ForObject<DATA_TYPE> extends ColumnSource<DATA_TYPE> {

        @Override
        default Boolean getBoolean(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for Boolean {@link ColumnSource} implementations.
     */
    public interface ForBoolean extends ColumnSource<Boolean> {

        @Override
        default Boolean getBoolean(final long rowKey) {
            return get(rowKey);
        }

        @Override
        default byte getByte(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for byte {@link ColumnSource} implementations.
     */
    public interface ForByte extends ColumnSource<Byte> {

        @Override
        default Byte get(final long rowKey) {
            return box(getByte(rowKey));
        }

        @Override
        default Boolean getBoolean(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for char {@link ColumnSource} implementations.
     */
    public interface ForChar extends ColumnSource<Character> {

        @Override
        default Character get(final long rowKey) {
            return box(getChar(rowKey));
        }

        @Override
        default Boolean getBoolean(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for double {@link ColumnSource} implementations.
     */
    public interface ForDouble extends ColumnSource<Double> {

        @Override
        default Double get(final long rowKey) {
            return box(getDouble(rowKey));
        }

        @Override
        default Boolean getBoolean(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for float {@link ColumnSource} implementations.
     */
    public interface ForFloat extends ColumnSource<Float> {

        @Override
        default Float get(final long rowKey) {
            return box(getFloat(rowKey));
        }

        @Override
        default Boolean getBoolean(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for int {@link ColumnSource} implementations.
     */
    public interface ForInt extends ColumnSource<Integer> {

        @Override
        default Integer get(final long rowKey) {
            return box(getInt(rowKey));
        }

        @Override
        default Boolean getBoolean(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for long-backed {@link ColumnSource} implementations.
     */
    public interface LongBacked<DATA_TYPE> extends ColumnSource<DATA_TYPE> {

        default Boolean getBoolean(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for long {@link ColumnSource} implementations.
     */
    public interface ForLong extends LongBacked<Long> {

        @Override
        default Long get(final long rowKey) {
            return box(getLong(rowKey));
        }
    }

    /**
     * Default interface for {@link DateTime} {@link ColumnSource} implementations.
     */
    public interface ForLongAsDateTime extends LongBacked<DateTime> {

        @Override
        default DateTime get(final long rowKey) {
            return nanosToTime(getLong(rowKey));
        }
    }

    /**
     * Default interface for short {@link ColumnSource} implementations.
     */
    public interface ForShort extends ColumnSource<Short> {

        @Override
        default Short get(final long rowKey) {
            return box(getShort(rowKey));
        }

        @Override
        default Boolean getBoolean(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long rowKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long rowKey) {
            throw new UnsupportedOperationException();
        }
    }
}
