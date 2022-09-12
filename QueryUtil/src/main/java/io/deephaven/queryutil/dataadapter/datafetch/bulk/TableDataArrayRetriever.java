package io.deephaven.queryutil.dataadapter.datafetch.bulk;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.queryutil.dataadapter.datafetch.bulk.gen.AbstractGeneratedTableDataArrayRetriever;
import io.deephaven.queryutil.dataadapter.datafetch.bulk.gen.TableDataRetrieverGenerator;
import io.deephaven.queryutil.dataadapter.datafetch.bulk.simple.SimpleTableDataArrayRetriever;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.function.LongConsumer;

/**
 * A TableDataArrayRetriever is a utility for retrieving multiple rows of data from a fixed set of
 * columns in a {@link io.deephaven.engine.table.Table}.
 * <p>
 * There are two steps to retrieve data with a TableDataArrayRetriever:
 * <ol>
 *     <li><i>Without holding the LTM lock</i>, {@link #createDataArrays create arrays} to hold the required rows of data.</li>
 *     <li><i>While holding the LTM lock</i> (or using {@link io.deephaven.engine.table.impl.remote.ConstructSnapshot ConstructSnapshot}),
 *     {@link #fillDataArrays fill the arrays} with data.</li>
 * </ol>
 *
 * <p>
 * Created by rbasralian on 3/7/22
 */
public interface TableDataArrayRetriever {

    /**
     * Get the column sources this TableDataArrayRetriever was created for.
     *
     * @return The column sources whose data is retrieved.
     */
    List<ColumnSource<?>> getColumnSources();

    /**
     * Create arrays to hold {@code len} rows of data. This should be called <b>without</b> the LiveTableMonitor
     * lock.
     *
     * @param len The length of the arrays (i.e. the number of rows of data that will be retrieved)
     * @return An array consisting of typed arrays, corresponding to the column sources for which the
     * TableDataArrayRetriever was created.
     */
    Object[] createDataArrays(int len);

    default void fillDataArrays(boolean usePrev, Object[] recordDataArrs, RowSet tableIndex) {
        fillDataArrays(usePrev, tableIndex, recordDataArrs, null);
    }

    /**
     * Fills the {@code dataArrs} with data from {@link #getColumnSources()}, for the positions given by
     * {@code tableIndex}. This <b>must</b> be called <b>with</b> the LiveTableMonitor lock or
     * under {@link io.deephaven.engine.table.impl.remote.ConstructSnapshot ConstructSnapshot}.
     *
     * @param usePrev     Whether to retrieve prev data instead of current
     * @param tableIndex  Index of rows for which to retrieve data
     * @param dataArrs    Arrays to populate with table data (created by {@link #createDataArrays})
     * @param keyConsumer Consumer that will be passed all keys in {@code tableIndex}
     */
    void fillDataArrays(boolean usePrev, RowSet tableIndex, Object[] dataArrs, LongConsumer keyConsumer);

    static TableDataArrayRetriever makeDefault(final ColumnSource<?>... columnSources) {
        final boolean useGeneratedTDAR = true;
        if (useGeneratedTDAR) {
            return makeGenerated(columnSources);
        } else {
            return makeSimple(columnSources);
        }
    }

    @NotNull
    static SimpleTableDataArrayRetriever makeSimple(ColumnSource<?>[] columnSources) {
        try {
            return new SimpleTableDataArrayRetriever(columnSources);
        } catch (Exception ex) {
            throw new RuntimeException("Could not create table data retriever", ex);
        }
    }

    @NotNull
    static AbstractGeneratedTableDataArrayRetriever makeGenerated(ColumnSource<?>[] columnSources) {
        try {
            final TableDataRetrieverGenerator tableDataRetrieverGenerator = new TableDataRetrieverGenerator(columnSources);
            //noinspection unchecked
            final Class<? extends AbstractGeneratedTableDataArrayRetriever> tableDataRetrieverClass =
                    (Class<? extends AbstractGeneratedTableDataArrayRetriever>) tableDataRetrieverGenerator.generate();

            final Constructor<? extends AbstractGeneratedTableDataArrayRetriever> tableDataRetrieverConstructor = tableDataRetrieverClass.getConstructor(ColumnSource[].class);
            return tableDataRetrieverConstructor.newInstance((Object) columnSources);
        } catch (Exception ex) {
            throw new RuntimeException("Could not create generated table data retriever", ex);
        }
    }
}
