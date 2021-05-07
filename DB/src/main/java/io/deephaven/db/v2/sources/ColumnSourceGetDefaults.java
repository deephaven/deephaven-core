package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.DBDateTime;

import static io.deephaven.db.tables.utils.DBTimeUtils.nanosToTime;
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
        default Boolean getBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for Boolean {@link ColumnSource} implementations.
     */
    public interface ForBoolean extends ColumnSource<Boolean> {

        @Override
        default Boolean getBoolean(final long index) {
            return get(index);
        }

        @Override
        default byte getByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for byte {@link ColumnSource} implementations.
     */
    public interface ForByte extends ColumnSource<Byte> {

        @Override
        default Byte get(final long index) {
            return box(getByte(index));
        }

        @Override
        default Boolean getBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for char {@link ColumnSource} implementations.
     */
    public interface ForChar extends ColumnSource<Character> {

        @Override
        default Character get(final long index) {
            return box(getChar(index));
        }

        @Override
        default Boolean getBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for double {@link ColumnSource} implementations.
     */
    public interface ForDouble extends ColumnSource<Double> {

        @Override
        default Double get(final long index) {
            return box(getDouble(index));
        }

        @Override
        default Boolean getBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for float {@link ColumnSource} implementations.
     */
    public interface ForFloat extends ColumnSource<Float> {

        @Override
        default Float get(final long index) {
            return box(getFloat(index));
        }

        @Override
        default Boolean getBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for int {@link ColumnSource} implementations.
     */
    public interface ForInt extends ColumnSource<Integer> {

        @Override
        default Integer get(final long index) {
            return box(getInt(index));
        }

        @Override
        default Boolean getBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for long-backed {@link ColumnSource} implementations.
     */
    public interface LongBacked<DATA_TYPE> extends ColumnSource<DATA_TYPE> {

        default Boolean getBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for long {@link ColumnSource} implementations.
     */
    public interface ForLong extends LongBacked<Long> {

        @Override
        default Long get(final long index) {
            return box(getLong(index));
        }
    }

    /**
     * Default interface for {@link DBDateTime} {@link ColumnSource} implementations.
     */
    public interface ForLongAsDateTime extends LongBacked<DBDateTime> {

        @Override
        default DBDateTime get(final long index) {
            return nanosToTime(getLong(index));
        }
    }

    /**
     * Default interface for short {@link ColumnSource} implementations.
     */
    public interface ForShort extends ColumnSource<Short> {

        @Override
        default Short get(final long index) {
            return box(getShort(index));
        }

        @Override
        default Boolean getBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getLong(final long index) {
            throw new UnsupportedOperationException();
        }
    }
}
