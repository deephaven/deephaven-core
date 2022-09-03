/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.engine.rowset.RowSet;

/**
 * A writable source that allows parallel population.
 */
public interface WritableSourceWithPrepareForParallelPopulation {
    /**
     * Does the specified WritableColumnSource provide the prepareForParallelPopulation function?
     *
     * @param wcs the WritableColumnSource to check
     * @return true if prepareForParallelPopulation can be called on wcs
     */
    static boolean supportsParallelPopulation(WritableColumnSource<?> wcs) {
        return wcs instanceof WritableSourceWithPrepareForParallelPopulation;
    }

    /**
     * Prepare this column source such that:
     * <ul>
     * <li>all values in rowSet may be accessed using getPrev</li>
     * <li>all values in rowSet may be populated in parallel</li>
     * </ul>
     *
     * Further operations in this cycle need not check for previous when writing data to the column source; you must
     * provide a row set that contains every row that may be written to this column source.
     *
     * @param rowSet the rowset of values that will change on this cycle
     */
    void prepareForParallelPopulation(RowSet rowSet);
}
