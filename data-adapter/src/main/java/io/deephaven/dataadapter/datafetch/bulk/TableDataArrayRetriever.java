//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import gnu.trove.list.TLongList;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * A TableDataArrayRetriever is a utility for retrieving multiple rows of data from a fixed set of columns in a
 * {@link io.deephaven.engine.table.Table}.
 * <p>
 * There are two steps to retrieve data with a TableDataArrayRetriever:
 * <ol>
 * <li><i>Without holding the LTM lock</i>, {@link #createDataArrays create arrays} to hold the required rows of
 * data.</li>
 * <li><i>While holding the LTM lock</i> (or using {@link io.deephaven.engine.table.impl.remote.ConstructSnapshot
 * ConstructSnapshot}), {@link #fillDataArrays fill the arrays} with data.</li>
 * </ol>
 */
public interface TableDataArrayRetriever {

    /**
     * Get the names of the columns this TableDataArrayRetriever was created for.
     *
     * @return The column sources whose data is retrieved.
     */
    List<String> getColumnNames();

    /**
     * Create arrays to hold {@code len} rows of data. This should be called <b>without</b> the LiveTableMonitor lock.
     *
     * @param len The length of the arrays (i.e. the number of rows of data that will be retrieved)
     * @return An array consisting of typed arrays, corresponding to the column sources for which the
     *         TableDataArrayRetriever was created.
     */
    Object[] createDataArrays(int len);

    default void fillDataArrays(boolean usePrev, Object[] recordDataArrs, RowSet tableIndex) {
        fillDataArrays(usePrev, tableIndex, recordDataArrs, null);
    }

    /**
     * Fills the {@code dataArrs} with data from column sources, for the positions given by {@code tableIndex}. This
     * <b>must</b> be called <b>with</b> the LiveTableMonitor lock or under
     * {@link io.deephaven.engine.table.impl.remote.ConstructSnapshot ConstructSnapshot}.
     *
     * @param usePrev Whether to retrieve prev data instead of current
     * @param tableIndex Index of rows for which to retrieve data
     * @param dataArrs Arrays to populate with table data (created by {@link #createDataArrays})
     * @param keyConsumer Consumer that will be passed all keys in {@code tableIndex}
     */
    void fillDataArrays(boolean usePrev, RowSet tableIndex, Object[] dataArrs, @NotNull TLongList keyConsumer);

    static TableDataArrayRetriever makeDefault(final List<String> columnNames, Table table) {
        return new TableDataArrayRetrieverImpl(columnNames, table);
    }
}
