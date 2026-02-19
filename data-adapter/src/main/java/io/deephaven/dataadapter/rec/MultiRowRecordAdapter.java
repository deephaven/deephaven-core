//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec;

import io.deephaven.dataadapter.datafetch.bulk.TableDataArrayRetriever;

/**
 * An adapter to convert rows from a table into instances of {@code T}. Each MultiRowRecordAdapter instance is tied to
 * one specific table object (through its {@link #getTableDataArrayRetriever() table data array retriever}).
 *
 * @param <T> The record data type
 */
public interface MultiRowRecordAdapter<T> {

    /**
     * Creates a new record of type {@code T}, to be populated by {@link #populateRecords(T[], Object[])} (or, from
     * {@link io.deephaven.dataadapter.KeyedRecordAdapter}, {@link io.deephaven.dataadapter.rec.updaters.RecordUpdater
     * RecordUpdaters}).
     *
     * @return A new instance of {@code T}
     */
    T createEmptyRecord();

    /**
     * Creates a typed array to hold {@code nRecords} instances of {@code T}. The returned array will be empty.
     *
     * @param nRecords Target array size.
     * @return An empty array with component type {@code T} and length {@code nRecords}.
     */
    T[] createEmptyRecordsArr(int nRecords);

    /**
     * The table data array retriever used to fetch data from the column sources.
     *
     * @return The table data array retriever for this adapter.
     */
    TableDataArrayRetriever getTableDataArrayRetriever();

    /**
     * Populates the {@code recordsArray} with records built from the data in the parallel {@code dataArrays}. (The
     * elements of {@code dataArrays} are typed arrays with the same length as {@code recordsArray}).
     * 
     * @param recordsArray The array to populate with records
     * @param dataArrays The array of data to store in the records
     */
    void populateRecords(T[] recordsArray, Object[] dataArrays);

    /**
     * @param recordDataArrs The arrays of data from each column
     * @param nRecords The number of records (length of each array in recordDataArrs)
     */
    T[] createRecordsFromData(final Object[] recordDataArrs, final int nRecords);

}
