//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import io.deephaven.base.Reference;
import io.deephaven.dataadapter.datafetch.bulk.TableDataArrayRetriever;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class TableToRecordAdapter<T> {

    private final @NotNull MultiRowRecordAdapter<T> recordAdapter;
    private final @NotNull ConstructSnapshot.SnapshotControl snapshotControl;
    private final @NotNull TableDataArrayRetriever tableDataArrayRetriever;

    /**
     * Supplier of latest row set, for use under snapshot function. This is used to look up the row keys corresponding
     * to positional indices of rows in the table.
     */
    @NotNull
    private final RowSetSupplier tableRowSetSupplier;

    public TableToRecordAdapter(
            @NotNull final Table table,
            @NotNull final RecordAdapterDescriptor<T> recordAdapterDescriptor) {
        recordAdapter = recordAdapterDescriptor.createMultiRowRecordAdapter(table);
        snapshotControl = ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), (BaseTable<?>) table);
        tableDataArrayRetriever = recordAdapter.getTableDataArrayRetriever();
        tableRowSetSupplier = usePrev -> {
            RowSet rowSet = table.getRowSet();
            if (usePrev) {
                rowSet = table.getRowSet().prev();
            }
            return rowSet;
        };
    }

    /**
     * Get records from the table, based on all rows.
     * 
     * @return An array of objects of type {@code T} representing the data in the table.
     */
    public T[] getRecords() {
        return getRecords(tableRowSetSupplier);
    }

    /**
     * Get records from the table, based on a specific row position.
     * 
     * @param rowPosition The position of the row to retrieve. Must be between {@code 0} and {@code table.size() - 1}.
     * @return An object of type {@code T} representing the data at the specified row position.
     */
    public T getRecordByPosition(final long rowPosition) {
        return getRecordsByPosition(new long[] {rowPosition})[0];
    }

    /**
     * Get records from the table, based on specific row positions.
     * 
     * @param rowPositions The positions of the rows to retrieve. All positions must be between {@code 0} and
     *        {@code table.size() - 1}.
     * @return An array of objects of type {@code T} representing the data at the specified row positions.
     */
    public T[] getRecordsByPosition(final long[] rowPositions) {
        return getRecords(usePrev -> {
            final RowSet rowSet = tableRowSetSupplier.get(usePrev);
            final RowSetBuilderRandom rowSetBuilder = RowSetFactory.builderRandom();
            rowSet.getKeysForPositions(Arrays.stream(rowPositions).iterator(), rowSetBuilder::addKey);
            return rowSetBuilder.build();
        });
    }

    private T[] getRecords(final RowSetSupplier rowSetSupplier) {
        final Reference<DataRetrievalResult> dataArrayRef = new Reference<>(null);

        final ConstructSnapshot.SnapshotFunction snapshotFunction = (usePrev, beforeClockValue) -> {
            final RowSet rowSet = rowSetSupplier.get(usePrev);
            final int nRecords = rowSet.intSize();

            final Object[] dataArrays = tableDataArrayRetriever.createDataArrays(nRecords);
            tableDataArrayRetriever.fillDataArrays(usePrev, dataArrays, rowSet);

            dataArrayRef.setValue(new DataRetrievalResult(nRecords, dataArrays));

            return true;
        };
        ConstructSnapshot.callDataSnapshotFunction(TableToRecordAdapter.class.getName(), snapshotControl,
                snapshotFunction);

        final DataRetrievalResult result = dataArrayRef.getValue();
        return recordAdapter.createRecordsFromData(result.dataArrays, result.nRecords);
    }

    private static class DataRetrievalResult {
        private final int nRecords;
        private final Object[] dataArrays;

        DataRetrievalResult(int nRecords, Object[] dataArrays) {
            this.nRecords = nRecords;
            this.dataArrays = dataArrays;
        }
    }

    private interface RowSetSupplier {
        RowSet get(boolean usePrev);
    }

}
