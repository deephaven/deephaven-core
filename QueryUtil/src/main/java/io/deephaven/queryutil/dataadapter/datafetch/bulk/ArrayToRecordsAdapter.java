package io.deephaven.queryutil.dataadapter.datafetch.bulk;

import io.deephaven.queryutil.dataadapter.rec.RecordUpdater;

/**
 * Interface for updating a record of type T with data from the give index of an array (or other object).
 */
public interface ArrayToRecordsAdapter<T> {

    @SuppressWarnings("unchecked")
    static <R, C> ArrayToRecordsAdapter<R> getArrayToRecordAdapter(final RecordUpdater<R, C> recordUpdater) {
        final Class<C> colType = recordUpdater.getSourceType();
        if (byte.class.equals(colType)) {
            return getByteArrayAdapter((RecordUpdater<R, Byte>) recordUpdater);
        } else if (char.class.equals(colType)) {
            return getCharArrayAdapter((RecordUpdater<R, Character>) recordUpdater);
        } else if (short.class.equals(colType)) {
            return getShortArrayAdapter((RecordUpdater<R, Short>) recordUpdater);
        } else if (int.class.equals(colType)) {
            return getIntArrayAdapter((RecordUpdater<R, Integer>) recordUpdater);
        } else if (float.class.equals(colType)) {
            return getFloatArrayAdapter((RecordUpdater<R, Float>) recordUpdater);
        } else if (long.class.equals(colType)) {
            return getLongArrayAdapter((RecordUpdater<R, Long>) recordUpdater);
        } else if (double.class.equals(colType)) {
            return getDoubleArrayAdapter((RecordUpdater<R, Double>) recordUpdater);
        } else {
            return getObjArrayAdapter(recordUpdater);
        }
    }

    /**
     * Updates {@code records} with the data from array {@code arr}.
     *
     * @param records An array of records to update
     * @param arr An array of data to update te records with
     */
    void updateRecordsFromArr(int nRecords, T[] records, Object arr);

    static <T> ArrayToRecordsAdapter<T> getByteArrayAdapter(RecordUpdater<T, Byte> colAdapter) {
        return (nRecords, records, arr) -> {
            byte[] arr2 = ((byte[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final byte colValue = arr2[idx];
                colAdapter.updateRecordWithByte(records[idx], colValue);
            }
        };
    }

    static <T> ArrayToRecordsAdapter<T> getCharArrayAdapter(RecordUpdater<T, Character> colAdapter) {
        return (nRecords, records, arr) -> {
            char[] arr2 = ((char[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final char colValue = arr2[idx];
                colAdapter.updateRecordWithChar(records[idx], colValue);
            }
        };
    }

    static <T> ArrayToRecordsAdapter<T> getShortArrayAdapter(RecordUpdater<T, Short> colAdapter) {
        return (nRecords, records, arr) -> {
            short[] arr2 = ((short[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final short colValue = arr2[idx];
                colAdapter.updateRecordWithShort(records[idx], colValue);
            }
        };
    }

    static <T> ArrayToRecordsAdapter<T> getFloatArrayAdapter(RecordUpdater<T, Float> colAdapter) {
        return (nRecords, records, arr) -> {
            float[] arr2 = ((float[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final float colValue = arr2[idx];
                colAdapter.updateRecordWithFloat(records[idx], colValue);
            }
        };
    }

    static <T> ArrayToRecordsAdapter<T> getIntArrayAdapter(RecordUpdater<T, Integer> colAdapter) {
        return (nRecords, records, arr) -> {
            int[] arr2 = ((int[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final int colValue = arr2[idx];
                colAdapter.updateRecordWithInt(records[idx], colValue);
            }
        };
    }

    static <T> ArrayToRecordsAdapter<T> getLongArrayAdapter(RecordUpdater<T, Long> colAdapter) {
        return (nRecords, records, arr) -> {
            long[] arr2 = ((long[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final long colValue = arr2[idx];
                colAdapter.updateRecordWithLong(records[idx], colValue);
            }
        };
    }

    static <T> ArrayToRecordsAdapter<T> getDoubleArrayAdapter(RecordUpdater<T, Double> colAdapter) {
        return (nRecords, records, arr) -> {
            double[] arr2 = ((double[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final double colValue = arr2[idx];
                colAdapter.updateRecordWithDouble(records[idx], colValue);
            }
        };
    }

    static <T> ArrayToRecordsAdapter<T> getBooleanArrayAdapter(RecordUpdater<T, Boolean> colAdapter) {
        return (nRecords, records, arr) -> {
            Boolean[] arr2 = ((Boolean[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final Boolean colValue = arr2[idx];
                colAdapter.updateRecord(records[idx], colValue);
            }
        };
    }

    static <T, C> ArrayToRecordsAdapter<T> getObjArrayAdapter(RecordUpdater<T, C> colAdapter) {
        return (nRecords, records, arr) -> {
            // noinspection unchecked
            C[] arr2 = ((C[]) arr);
            for (int idx = 0; idx < nRecords; idx++) {
                final C colValue = arr2[idx];
                colAdapter.updateRecord(records[idx], colValue);
            }
        };
    }
}
