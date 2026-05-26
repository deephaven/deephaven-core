//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import io.deephaven.dataadapter.ContextHolder;
import io.deephaven.dataadapter.datafetch.bulk.AbstractTableDataArrayRetrieverImpl;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import org.jetbrains.annotations.NotNull;

/**
 * An efficient reader for a Python table listener to extract columnar data based on the {@link RowSequence} in the
 * {@link TableUpdate}
 */
public class PythonTableDataReader {

    /**
     * Factory method for instance of {@link ContextHolder}
     *
     * @param chunkCapacity
     * @param columnSources
     * @return {@link ContextHolder}
     */
    public static ContextHolder makeContext(final int chunkCapacity,
            @NotNull final ColumnSource<?>... columnSources) {
        return new ContextHolder(chunkCapacity, columnSources);
    }

    /**
     * Copy data from a table by chunks into a 2D array
     *
     * @param context the context used in filling the output array
     * @param rowSeq indices of the rows of the table to put into the 2D array
     * @param columnSources columns of data to put into the 2D array
     * @return a 2D array
     */
    public static Object[] readChunkColumnMajor(
            @NotNull final ContextHolder context,
            @NotNull final RowSequence rowSeq,
            @NotNull final ColumnSource<?>[] columnSources,
            final boolean prev) {
        final int nRows = rowSeq.intSize();

        final Object[] arrays = AbstractTableDataArrayRetrieverImpl.createDataArrays(rowSeq.intSize(), columnSources);

        // TODO: why not just create the Context in this method?

        AbstractTableDataArrayRetrieverImpl.fillDataArrays(
                prev,
                rowSeq.asRowSet(),
                columnSources,
                arrays,
                null,
                context,
                0);

        return arrays;
    }
}
