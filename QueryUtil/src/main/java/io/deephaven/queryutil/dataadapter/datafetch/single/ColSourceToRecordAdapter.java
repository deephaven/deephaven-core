package io.deephaven.queryutil.dataadapter.datafetch.single;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.queryutil.dataadapter.rec.RecordUpdater;

/**
 * Interface for updating a record of type {@code T} with data from the given index of a ColumnSource.
 */
interface ColSourceToRecordAdapter<T, C> {

    @SuppressWarnings("unchecked")
    static <R, C> ColSourceToRecordAdapter<R, C> getColSourceToRecordAdapter(final RecordUpdater<R, C> recordUpdater) {
        final Class<C> colType = recordUpdater.getSourceType();
        if (byte.class.equals(colType)) {
            return (ColSourceToRecordAdapter<R, C>) getByteColAdapter((RecordUpdater<R, Byte>) recordUpdater);
        } else if (char.class.equals(colType)) {
            return (ColSourceToRecordAdapter<R, C>) getCharColAdapter((RecordUpdater<R, Character>) recordUpdater);
        } else if (short.class.equals(colType)) {
            return (ColSourceToRecordAdapter<R, C>) getShortColAdapter((RecordUpdater<R, Short>) recordUpdater);
        } else if (int.class.equals(colType)) {
            return (ColSourceToRecordAdapter<R, C>) getIntColAdapter((RecordUpdater<R, Integer>) recordUpdater);
        } else if (float.class.equals(colType)) {
            return (ColSourceToRecordAdapter<R, C>) getFloatColAdapter((RecordUpdater<R, Float>) recordUpdater);
        } else if (long.class.equals(colType)) {
            return (ColSourceToRecordAdapter<R, C>) getLongColAdapter((RecordUpdater<R, Long>) recordUpdater);
        } else if (double.class.equals(colType)) {
            return (ColSourceToRecordAdapter<R, C>) getDoubleColAdapter((RecordUpdater<R, Double>) recordUpdater);
        } else {
            return getObjColAdapter(recordUpdater);
        }
    }

    void updateRecordFromColumn(final ColumnSource<C> colSource, long k, boolean usePrev, T record);

    static <T> ColSourceToRecordAdapter<T, Byte> getByteColAdapter(RecordUpdater<T, Byte> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final byte colValue = usePrev ? colSource.getPrevByte(k) : colSource.getByte(k);
            recordUpdater.updateRecordWithByte(record, colValue);
        };
    }

    static <T> ColSourceToRecordAdapter<T, Character> getCharColAdapter(RecordUpdater<T, Character> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final char colValue = usePrev ? colSource.getPrevChar(k) : colSource.getChar(k);
            recordUpdater.updateRecordWithChar(record, colValue);
        };
    }

    static <T> ColSourceToRecordAdapter<T, Short> getShortColAdapter(RecordUpdater<T, Short> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final short colValue = usePrev ? colSource.getPrevShort(k) : colSource.getShort(k);
            recordUpdater.updateRecordWithShort(record, colValue);
        };
    }

    static <T> ColSourceToRecordAdapter<T, Float> getFloatColAdapter(RecordUpdater<T, Float> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final float colValue = usePrev ? colSource.getPrevFloat(k) : colSource.getFloat(k);
            recordUpdater.updateRecordWithFloat(record, colValue);
        };
    }

    static <T> ColSourceToRecordAdapter<T, Integer> getIntColAdapter(RecordUpdater<T, Integer> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final int colValue = usePrev ? colSource.getPrevInt(k) : colSource.getInt(k);
            recordUpdater.updateRecordWithInt(record, colValue);
        };
    }

    static <T> ColSourceToRecordAdapter<T, Long> getLongColAdapter(RecordUpdater<T, Long> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final long colValue = usePrev ? colSource.getPrevLong(k) : colSource.getLong(k);
            recordUpdater.updateRecordWithLong(record, colValue);
        };
    }

    static <T> ColSourceToRecordAdapter<T, Double> getDoubleColAdapter(RecordUpdater<T, Double> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final double colValue = usePrev ? colSource.getPrevDouble(k) : colSource.getDouble(k);
            recordUpdater.updateRecordWithDouble(record, colValue);
        };
    }

    static <T, C> ColSourceToRecordAdapter<T, C> getObjColAdapter(RecordUpdater<T, C> recordUpdater) {
        return (colSource, k, usePrev, record) -> {
            final C colValue = usePrev ? colSource.getPrev(k) : colSource.get(k);
            recordUpdater.updateRecord(record, colValue);
        };
    }
}
