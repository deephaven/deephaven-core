//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.updaters.RecordUpdater;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Map;

/**
 * Utility to retrieve rows of table data in bulk and convert them to records of type {@code T}.
 *
 * @param <T> The data structure used to represent table rows
 */
public class DefaultMultiRowRecordAdapter<T> extends BaseMultiRowRecordAdapter<T> {

    protected final RecordAdapterDescriptor<T> descriptor;

    /**
     * An array of array-to-record adapters (parallel to the array of {@link TableDataArrayRetriever#getColumnSources()
     * tableDataArrayRetriever colum sources}) used to update a record of type {@code T} with values from arrays of data
     * retrieved from the column sources via the TableDataArrayRetriever.
     */
    @SuppressWarnings("rawtypes")
    private final ArrayToRecordsAdapter[] arrayToRecordAdapters;

    public static <T> DefaultMultiRowRecordAdapter<T> create(Table sourceTable, RecordAdapterDescriptor<T> descriptor) {
        return new DefaultMultiRowRecordAdapter<>(sourceTable, descriptor);
    }

    private DefaultMultiRowRecordAdapter(@NotNull final Table t, @NotNull final RecordAdapterDescriptor<T> descriptor) {
        super(t, descriptor);
        this.descriptor = descriptor;

        final Map<String, RecordUpdater<T, ?>> columnAdapters = descriptor.getColumnAdapters();

        // noinspection rawtypes
        final ColumnSource[] columnSources = new ColumnSource[nCols];
        arrayToRecordAdapters = new ArrayToRecordsAdapter[nCols];

        int ii = 0;
        for (Map.Entry<String, RecordUpdater<T, ?>> en : columnAdapters.entrySet()) {
            final String colName = en.getKey();

            columnSources[ii] = t.getColumnSource(colName);

            // Create adapter to run value from array through recordUpdater
            final RecordUpdater<T, ?> recordUpdater = en.getValue();
            arrayToRecordAdapters[ii] = ArrayToRecordsAdapter.getArrayToRecordAdapter(recordUpdater);

            final Class<?> expectedType = recordUpdater.getSourceType();
            final Class<?> colType = columnSources[ii].getType();
            final boolean recordAdapterMatchesData = expectedType.isAssignableFrom(colType);
            if (!recordAdapterMatchesData) {
                throw new IllegalArgumentException("Type mismatch for column " + ii + " (" + colName
                        + "): RecordUpdater expected type " + expectedType.getCanonicalName() +
                        " but column has type " + colType);
            }

            ii++;
        }

    }

    public void populateRecords(final T[] results, final Object[] recordDataArrs) {
        // Populate the records with the data, one column at a time.
        for (int ii = 0; ii < nCols; ii++) {
            Object arr = recordDataArrs[ii];
            // noinspection unchecked
            arrayToRecordAdapters[ii].updateRecordsFromArr(results.length, results, arr);
        }
    }

    @NotNull
    public T createEmptyRecord() {
        return descriptor.getEmptyRecord();
    }

    @NotNull
    public T[] createEmptyRecordsArr(final int nRecords) {
        // noinspection unchecked
        return (T[]) Array.newInstance(recordType, nRecords);
    }
}
