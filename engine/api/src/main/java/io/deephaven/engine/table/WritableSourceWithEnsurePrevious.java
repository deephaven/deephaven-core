package io.deephaven.engine.table;

import io.deephaven.engine.rowset.RowSet;

/**
 * A writable source that allows setting all previous values with one operation rather than on each set.
 */
public interface WritableSourceWithEnsurePrevious {
    /**
     * Does the specified WritableColumnSource provide the ensurePrevious function?
     *
     * @param wcs the WritableColumnSource to check
     * @return true if ensurePrevious can be called on wcs
     */
    static boolean providesEnsurePrevious(WritableColumnSource<?> wcs) {
        return wcs instanceof WritableSourceWithEnsurePrevious;
    }

    /**
     * Prepare this column source such that all values in rowSet may be accessed using getPrev. Further operations in
     * this cycle need not check for previous when writing data to the column source; you must provide a row set that
     * contains every row that may be written to this column source.
     *
     * @param rowSet the rowset of values that will change on this cycle
     */
    void ensurePrevious(RowSet rowSet);
}
