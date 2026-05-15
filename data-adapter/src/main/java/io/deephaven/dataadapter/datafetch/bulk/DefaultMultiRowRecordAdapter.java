//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import io.deephaven.base.verify.Assert;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.updaters.RecordUpdater;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
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
     * An array of array-to-record adapters (parallel to the {@link TableDataArrayRetriever#getColumnNames
     * TableDataArrayRetriever's columns}) used to update a record of type {@code T} with values from arrays of data
     * retrieved from the column sources via the TableDataArrayRetriever.
     * <p>
     * The order of the columns is determined by the {@link #descriptor}.
     */
    @SuppressWarnings("rawtypes")
    private final ArrayToRecordsAdapter[] arrayToRecordAdapters;

    public static <T> DefaultMultiRowRecordAdapter<T> create(Table sourceTable, RecordAdapterDescriptor<T> descriptor) {
        final TableDataArrayRetriever tableDataArrayRetriever = TableDataArrayRetriever.makeDefault(
                descriptor.getColumnNames(),
                sourceTable);

        return new DefaultMultiRowRecordAdapter<>(
                sourceTable.getDefinition(),
                descriptor,
                tableDataArrayRetriever);
    }


    public static <T> DefaultMultiRowRecordAdapter<T> create(PartitionedTable sourceTable,
            RecordAdapterDescriptor<T> descriptor) {
        final TableDataArrayRetriever tableDataArrayRetriever =
                new PartitionedTableDataArrayRetrieverImpl(sourceTable, descriptor.getColumnNames());

        return new DefaultMultiRowRecordAdapter<>(
                sourceTable.constituentDefinition(),
                descriptor,
                tableDataArrayRetriever);
    }

    private DefaultMultiRowRecordAdapter(@NotNull final TableDefinition tableDefinition,
            @NotNull final RecordAdapterDescriptor<T> descriptor,
            final TableDataArrayRetriever tableDataArrayRetriever) {
        super(descriptor, tableDataArrayRetriever);
        this.descriptor = descriptor;

        final Map<String, RecordUpdater<T, ?>> columnAdapters = descriptor.getColumnAdapters();

        arrayToRecordAdapters = new ArrayToRecordsAdapter[nCols];

        int ii = 0;
        for (Map.Entry<String, RecordUpdater<T, ?>> en : columnAdapters.entrySet()) {
            final String colName = en.getKey();

            // Create adapter to run value from array through recordUpdater
            final RecordUpdater<T, ?> recordUpdater = en.getValue();
            arrayToRecordAdapters[ii] = ArrayToRecordsAdapter.getArrayToRecordAdapter(recordUpdater);

            final Class<?> expectedType = recordUpdater.getSourceType();
            final Class<?> colType = tableDefinition.getColumn(colName).getDataType();
            final boolean recordAdapterMatchesData = expectedType.isAssignableFrom(colType);
            if (!recordAdapterMatchesData) {
                throw new IllegalArgumentException("Type mismatch for column " + ii + " (" + colName
                        + "): RecordAdapter expects type " + expectedType.getCanonicalName() +
                        " but column has type " + colType);
            }

            ii++;
        }
    }

    public void populateRecords(final T[] results, final Object[] dataArrs) {
        final int nResults = results.length;

        // Populate the records with the data, one column at a time.
        for (int ii = 0; ii < nCols; ii++) {
            Object arr = dataArrs[ii];
            Assert.eq(nResults, "nResults", Array.getLength(arr), "Array.getLength(arr)");

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
