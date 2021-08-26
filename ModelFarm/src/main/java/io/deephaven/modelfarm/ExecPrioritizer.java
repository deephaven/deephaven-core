/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

/**
 * An interface for prioritizing the execution of table rows.
 *
 * @param <KEYTYPE> unique ID key type
 * @param <DATATYPE> data type
 */
public interface ExecPrioritizer<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE extends RowDataManager<KEYTYPE, DATATYPE>> {

    /**
     * Compute the priority for processing the data at the indicated table index.
     *
     * @param dataManager interface for accessing and querying data contained in rows of a dynamic table.
     * @param index index of the data in the fit data manager source table.
     * @return priority of processing the data at the indicated index. Higher numbers are higher priority.
     */
    int priority(final ROWDATAMANAGERTYPE dataManager, final long index);

    /**
     * A new execution happened using this row data.
     *
     * @param data data used in the execution.
     */
    void execHappened(final DATATYPE data);

}
