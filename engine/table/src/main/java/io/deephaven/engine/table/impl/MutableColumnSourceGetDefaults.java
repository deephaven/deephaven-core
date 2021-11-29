package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTime;

import static io.deephaven.time.DateTimeUtils.nanosToTime;
import static io.deephaven.util.type.TypeUtils.box;

/**
 * Defaulted interfaces for various mutable {@link ColumnSource} types, in order to avoid having defaults at higher
 * levels in the class hierarchy.
 */
public final class MutableColumnSourceGetDefaults {

    /**
     * Default interface for mutable Object {@link ColumnSource} implementations.
     */
    public interface ForObject<DATA_TYPE>
            extends ColumnSourceGetDefaults.ForObject<DATA_TYPE>, MutableColumnSource<DATA_TYPE> {

        @Override
        default Boolean getPrevBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getPrevByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getPrevChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getPrevDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getPrevFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getPrevInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getPrevLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getPrevShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for mutable Boolean {@link ColumnSource} implementations.
     */
    public interface ForBoolean extends ColumnSourceGetDefaults.ForBoolean, MutableColumnSource<Boolean> {

        @Override
        default Boolean getPrevBoolean(final long index) {
            return getPrev(index);
        }

        @Override
        default byte getPrevByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getPrevChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getPrevDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getPrevFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getPrevInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getPrevLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getPrevShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for mutable byte {@link ColumnSource} implementations.
     */
    public interface ForByte extends ColumnSourceGetDefaults.ForByte, MutableColumnSource<Byte> {

        @Override
        default Byte getPrev(final long index) {
            return box(getPrevByte(index));
        }

        @Override
        default Boolean getPrevBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getPrevChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getPrevDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getPrevFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getPrevInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getPrevLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getPrevShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for mutable char {@link ColumnSource} implementations.
     */
    public interface ForChar extends ColumnSourceGetDefaults.ForChar, MutableColumnSource<Character> {

        @Override
        default Character getPrev(final long index) {
            return box(getPrevChar(index));
        }

        @Override
        default Boolean getPrevBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getPrevByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getPrevDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getPrevFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getPrevInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getPrevLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getPrevShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for mutable double {@link ColumnSource} implementations.
     */
    public interface ForDouble extends ColumnSourceGetDefaults.ForDouble, MutableColumnSource<Double> {

        @Override
        default Double getPrev(final long index) {
            return box(getPrevDouble(index));
        }

        @Override
        default Boolean getPrevBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getPrevByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getPrevChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getPrevFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getPrevInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getPrevLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getPrevShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for mutable float {@link ColumnSource} implementations.
     */
    public interface ForFloat extends ColumnSourceGetDefaults.ForFloat, MutableColumnSource<Float> {

        @Override
        default Float getPrev(final long index) {
            return box(getPrevFloat(index));
        }

        @Override
        default Boolean getPrevBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getPrevByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getPrevChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getPrevDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getPrevInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getPrevLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getPrevShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for mutable int {@link ColumnSource} implementations.
     */
    public interface ForInt extends ColumnSourceGetDefaults.ForInt, MutableColumnSource<Integer> {

        @Override
        default Integer getPrev(final long index) {
            return box(getPrevInt(index));
        }

        @Override
        default Boolean getPrevBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getPrevByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getPrevChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getPrevDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getPrevFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getPrevLong(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getPrevShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for mutable long-backed {@link ColumnSource} implementations.
     */
    public interface LongBacked<DATA_TYPE>
            extends ColumnSourceGetDefaults.LongBacked<DATA_TYPE>, MutableColumnSource<DATA_TYPE> {

        default Boolean getPrevBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getPrevByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getPrevChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getPrevDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getPrevFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getPrevInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default short getPrevShort(final long index) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Default interface for mutable long {@link ColumnSource} implementations.
     */
    public interface ForLong extends ColumnSourceGetDefaults.ForLong, LongBacked<Long> {

        @Override
        default Long getPrev(final long index) {
            return box(getPrevLong(index));
        }
    }

    /**
     * Default interface for mutable {@link DateTime} {@link ColumnSource} implementations.
     */
    public interface ForLongAsDateTime extends ColumnSourceGetDefaults.ForLongAsDateTime, LongBacked<DateTime> {

        @Override
        default DateTime getPrev(final long index) {
            return nanosToTime(getPrevLong(index));
        }
    }

    /**
     * Default interface for mutable short {@link ColumnSource} implementations.
     */
    public interface ForShort extends ColumnSourceGetDefaults.ForShort, MutableColumnSource<Short> {

        @Override
        default Short getPrev(final long index) {
            return box(getPrevShort(index));
        }

        @Override
        default Boolean getPrevBoolean(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default byte getPrevByte(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default char getPrevChar(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default double getPrevDouble(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default float getPrevFloat(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default int getPrevInt(final long index) {
            throw new UnsupportedOperationException();
        }

        @Override
        default long getPrevLong(final long index) {
            throw new UnsupportedOperationException();
        }
    }
}
