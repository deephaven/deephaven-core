//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec;

import io.deephaven.dataadapter.datafetch.bulk.TableDataArrayRetriever;

/**
 * @param <T> The record data type
 */
public interface MultiRowRecordAdapter<T> {

    /**
     * Creates a new record of type {@code T}.
     *
     * @return A new instance of {@code T}
     */
    T createEmptyRecord();

    /**
     * Creates an array to hold {@code nRecords} instances of {@code T}. The returned array will be empty.
     *
     * @param nRecords Target array size
     * @return An empty array
     */
    T[] createEmptyRecordsArr(int nRecords);

    TableDataArrayRetriever getTableDataArrayRetriever();

    /**
     * Populates the {@code recordsArray} with records built from the data in the parallel {@code dataArrays}. (The
     * elements of {@code dataArrays are typed arrays with the same length as {@code recordsArray}).
     * <p>
     * @param recordsArray The array to populate with records
     * 
     * @param dataArrays The array of data to store in the records
     */
    void populateRecords(T[] recordsArray, Object[] dataArrays);

    /**
     * @param recordDataArrs The arrays of data from each column
     * @param nRecords The number of records (length of each array in recordDataArrs)
     */
    T[] createRecordsFromData(final Object[] recordDataArrs, final int nRecords);

}
