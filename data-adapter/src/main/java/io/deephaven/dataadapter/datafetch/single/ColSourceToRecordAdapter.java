package io.deephaven.dataadapter.datafetch.single;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.dataadapter.rec.updaters.*;

/**
 * Interface for updating a record of type {@code T} with data from the given index of a ColumnSource.
 */
interface ColSourceToRecordAdapter<T, C> {

    @SuppressWarnings("unchecked")
    static <R, C> ColSourceToRecordAdapter<R, C> getColSourceToRecordAdapter(
            final RecordUpdater<R, C> recordUpdater) {
        if (recordUpdater instanceof ByteRecordUpdater) {
            return (ColSourceToRecordAdapter<R, C>) getByteColAdapter((ByteRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof CharRecordUpdater) {
            return (ColSourceToRecordAdapter<R, C>) getCharColAdapter((CharRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof ShortRecordUpdater) {
            return (ColSourceToRecordAdapter<R, C>) getShortColAdapter((ShortRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof IntRecordUpdater) {
            return (ColSourceToRecordAdapter<R, C>) getIntColAdapter((IntRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof FloatRecordUpdater) {
            return (ColSourceToRecordAdapter<R, C>) getFloatColAdapter((FloatRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof LongRecordUpdater) {
            return (ColSourceToRecordAdapter<R, C>) getLongColAdapter((LongRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof DoubleRecordUpdater) {
            return (ColSourceToRecordAdapter<R, C>) getDoubleColAdapter((DoubleRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof ObjRecordUpdater) {
            return getObjColAdapter((ObjRecordUpdater<R, C>) recordUpdater);
        } else {
            throw new IllegalStateException("Unexpected updater type: " + recordUpdater.getClass());
        }
    }

    void updateRecordFromColumn(final ColumnSource<C> colSource, long k, boolean usePrev, T record);

    // Utility methods for getting ColSourceToRecordAdapters:

    static <R> ColSourceToRecordAdapter<R, Byte> getByteColAdapter(ByteRecordUpdater<R> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final byte colValue = usePrev ? colSource.getPrevByte(k) : colSource.getByte(k);
            recordUpdater.accept(record, colValue);
        };
    }

    static <R> ColSourceToRecordAdapter<R, Character> getCharColAdapter(CharRecordUpdater<R> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final char colValue = usePrev ? colSource.getPrevChar(k) : colSource.getChar(k);
            recordUpdater.accept(record, colValue);
        };
    }

    static <R> ColSourceToRecordAdapter<R, Short> getShortColAdapter(ShortRecordUpdater<R> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final short colValue = usePrev ? colSource.getPrevShort(k) : colSource.getShort(k);
            recordUpdater.accept(record, colValue);
        };
    }

    static <R> ColSourceToRecordAdapter<R, Float> getFloatColAdapter(FloatRecordUpdater<R> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final float colValue = usePrev ? colSource.getPrevFloat(k) : colSource.getFloat(k);
            recordUpdater.accept(record, colValue);
        };
    }

    static <R> ColSourceToRecordAdapter<R, Integer> getIntColAdapter(IntRecordUpdater<R> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final int colValue = usePrev ? colSource.getPrevInt(k) : colSource.getInt(k);
            recordUpdater.accept(record, colValue);
        };
    }

    static <R> ColSourceToRecordAdapter<R, Long> getLongColAdapter(LongRecordUpdater<R> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final long colValue = usePrev ? colSource.getPrevLong(k) : colSource.getLong(k);
            recordUpdater.accept(record, colValue);
        };
    }

    static <R> ColSourceToRecordAdapter<R, Double> getDoubleColAdapter(DoubleRecordUpdater<R> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final double colValue = usePrev ? colSource.getPrevDouble(k) : colSource.getDouble(k);
            recordUpdater.accept(record, colValue);
        };
    }

    static <R, T> ColSourceToRecordAdapter<R, T> getObjColAdapter(ObjRecordUpdater<R, T> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final T colValue = usePrev ? colSource.getPrev(k) : colSource.get(k);
            recordUpdater.accept(record, colValue);
        };
    }
}
