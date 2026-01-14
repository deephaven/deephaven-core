//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import io.deephaven.dataadapter.rec.updaters.*;

/**
 * Interface for updating a record of type T with data from the give index of an array (or other object).
 */
interface ArrayToRecordsAdapter<T> {

    @SuppressWarnings("unchecked")
    static <R, C> ArrayToRecordsAdapter<R> getArrayToRecordAdapter(final RecordUpdater<R, C> recordUpdater) {
        if (recordUpdater instanceof ByteRecordUpdater) {
            return getByteArrayAdapter((ByteRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof CharRecordUpdater) {
            return getCharArrayAdapter((CharRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof ShortRecordUpdater) {
            return getShortArrayAdapter((ShortRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof IntRecordUpdater) {
            return getIntArrayAdapter((IntRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof FloatRecordUpdater) {
            return getFloatArrayAdapter((FloatRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof LongRecordUpdater) {
            return getLongArrayAdapter((LongRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof DoubleRecordUpdater) {
            return getDoubleArrayAdapter((DoubleRecordUpdater<R>) recordUpdater);
        } else if (recordUpdater instanceof ObjRecordUpdater) {
            return getObjArrayAdapter((ObjRecordUpdater<R, ?>) recordUpdater);
        } else {
            throw new IllegalStateException("Unexpected updater type: " + recordUpdater.getClass());
        }
    }

    /**
     * Updates {@code records} with the data from array {@code arr}.
     *
     * @param records An array of records to update
     * @param arr An array of data to update te records with
     */
    void updateRecordsFromArr(int nRecords, T[] records, Object arr);

    static <R> ArrayToRecordsAdapter<R> getByteArrayAdapter(ByteRecordUpdater<R> colAdapter) {
        return (nRecords, records, arr) -> {
            byte[] arr2 = ((byte[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final byte colValue = arr2[idx];
                colAdapter.accept(records[idx], colValue);
            }
        };
    }

    static <R> ArrayToRecordsAdapter<R> getCharArrayAdapter(CharRecordUpdater<R> colAdapter) {
        return (nRecords, records, arr) -> {
            char[] arr2 = ((char[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final char colValue = arr2[idx];
                colAdapter.accept(records[idx], colValue);
            }
        };
    }

    static <R> ArrayToRecordsAdapter<R> getShortArrayAdapter(ShortRecordUpdater<R> colAdapter) {
        return (nRecords, records, arr) -> {
            short[] arr2 = ((short[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final short colValue = arr2[idx];
                colAdapter.accept(records[idx], colValue);
            }
        };
    }

    static <R> ArrayToRecordsAdapter<R> getFloatArrayAdapter(FloatRecordUpdater<R> colAdapter) {
        return (nRecords, records, arr) -> {
            float[] arr2 = ((float[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final float colValue = arr2[idx];
                colAdapter.accept(records[idx], colValue);
            }
        };
    }

    static <R> ArrayToRecordsAdapter<R> getIntArrayAdapter(IntRecordUpdater<R> colAdapter) {
        return (nRecords, records, arr) -> {
            int[] arr2 = ((int[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final int colValue = arr2[idx];
                colAdapter.accept(records[idx], colValue);
            }
        };
    }

    static <R> ArrayToRecordsAdapter<R> getLongArrayAdapter(LongRecordUpdater<R> colAdapter) {
        return (nRecords, records, arr) -> {
            long[] arr2 = ((long[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final long colValue = arr2[idx];
                colAdapter.accept(records[idx], colValue);
            }
        };
    }

    static <R> ArrayToRecordsAdapter<R> getDoubleArrayAdapter(DoubleRecordUpdater<R> colAdapter) {
        return (nRecords, records, arr) -> {
            double[] arr2 = ((double[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final double colValue = arr2[idx];
                colAdapter.accept(records[idx], colValue);
            }
        };
    }

    static <R> ArrayToRecordsAdapter<R> getBooleanArrayAdapter(ObjRecordUpdater<R, Boolean> colAdapter) {
        return getObjArrayAdapter(colAdapter);
    }

    static <R, T> ArrayToRecordsAdapter<R> getObjArrayAdapter(ObjRecordUpdater<R, T> colAdapter) {
        return (nRecords, records, arr) -> {
            // noinspection unchecked
            T[] arr2 = ((T[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final T colValue = arr2[idx];
                colAdapter.accept(records[idx], colValue);
            }
        };
    }
}
