package io.deephaven.engine.table;

import io.deephaven.engine.rowset.RowSet;

public interface WritableSourceWithEnsurePrevious {
    static boolean providesEnsurePrevious(WritableColumnSource wcs) {
        return wcs instanceof WritableSourceWithEnsurePrevious;
    }

    /**
     * Prepare this column source such that all values in rowSet may be accessed using getPrev.  Further operations in
     * this cycle need not check for previous when writing data to the column source; you must provide a row set that
     * contains every row that may be written to this column source.
     *
     * @param rowSet the rowset of values that will change on this cycle
     */
    void ensurePrevious(RowSet rowSet);
}
