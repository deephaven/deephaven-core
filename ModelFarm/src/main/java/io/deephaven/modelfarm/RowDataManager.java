/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.db.v2.DynamicTable;

/**
 * An interface for accessing and querying data contained in rows of a dynamic table.
 *
 * @param <KEYTYPE> unique ID key type
 * @param <DATATYPE> data type
 */
public interface RowDataManager<KEYTYPE, DATATYPE> {

    /**
     * Gets the source table.
     *
     * @return source table.
     */
    DynamicTable table();

    /**
     * Creates a new data instance.
     *
     * @return new data instance.
     */
    DATATYPE newData();

    /**
     * Gets the current unique identifier value for a row.
     * <p>
     * This function should only be called during an update loop or while holding the LTM lock.
     *
     * @param index table row index.
     * @return current unique identifier for a row.
     */
    KEYTYPE uniqueIdCurrent(final long index);

    /**
     * Gets the previous unique identifier value for a row. One column of each table is designated as a unique
     * identifier for data rows.
     * <p>
     * This function should only be called during an update loop or while holding the LTM lock.
     *
     * @param index table row index.
     * @return previous underlying id.
     */
    KEYTYPE uniqueIdPrev(final long index);

    /**
     * Populates a data object with data from a table row.
     * <p>
     * This method should be called while the LTM lock is held. This can occur either during the update loop or the LTM
     * lock can be acquired outside the update loop. If the LTM lock is not held, the loaded data can be inconsistent or
     * corrupt.
     *
     * @param data data structure to populate
     * @param index table index of the row to load data from
     * @param usePrev use data from the previous table update
     */
    void loadData(final DATATYPE data, final long index, final boolean usePrev);

    /**
     * Interface for loading data for extra columns.
     *
     * @param <DATATYPE> data type
     */
    @FunctionalInterface
    interface LoadExtraColumns<DATATYPE> {
        /**
         * Load data for extra columns.
         *
         * @param data data structure to populate
         * @param index table index of the row to load data from
         * @param usePrev use data from the previous table update
         */
        void load(DATATYPE data, long index, boolean usePrev);
    }

}
