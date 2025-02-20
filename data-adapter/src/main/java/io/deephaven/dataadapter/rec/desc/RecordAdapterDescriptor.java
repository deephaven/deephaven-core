//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.desc;

import io.deephaven.api.agg.Partition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.dataadapter.datafetch.bulk.DefaultMultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.updaters.RecordUpdater;
import io.deephaven.dataadapter.rec.updaters.ObjRecordUpdater;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;

/**
 * Object that describes how to map data from Deephaven table columns into instances of {@code T}.
 */
public interface RecordAdapterDescriptor<T> {

    /**
     * Creates a RecordAdapterDescriptor for a record adapter that stores the {@code columns} in a HashMap.
     *
     * @param columns The columns to include in the map.
     * @return A RecordAdapterDescriptor that converts each row of a table into a HashMaps.
     */
    @NotNull
    static RecordAdapterDescriptor<Map<String, Object>> createGenericRecordAdapterDescriptor(
            @NotNull final Table sourceTable,
            @NotNull final List<String> columns) {
        final int nCols = columns.size();
        final RecordAdapterDescriptorBuilder<Map<String, Object>> descriptorBuilder =
                RecordAdapterDescriptorBuilder.create(() -> new HashMap<>(nCols));

        for (String colName : columns) {
            final ColumnSource<?> colSource = sourceTable.getColumnSource(colName);
            final Class<?> colType = colSource.getType();
            final RecordUpdater<Map<String, Object>, ?> updater = ObjRecordUpdater.getBoxingUpdater(
                    colType,
                    ObjRecordUpdater.getObjectUpdater((map, val) -> map.put(colName, val)));

            descriptorBuilder.addColumnAdapter(colName, updater);
        }
        return descriptorBuilder.build();
    }

    /**
     * Returns a map of each column name to the RecordUpdater used to populate a record of type {@code T} with data from
     * that column.
     *
     * @return The map of column names to record updaters.
     */
    Map<String, RecordUpdater<T, ?>> getColumnAdapters();

    default List<String> getColumnNames() {
        return new ArrayList<>(getColumnAdapters().keySet());
    }

    /**
     * Returns an empty record.
     *
     * @return An empty record to be populated with data from a table.
     */
    @NotNull
    T getEmptyRecord();

    default MultiRowRecordAdapter<T> createMultiRowRecordAdapter(Table sourceTable) {
        return getMultiRowAdapterSupplier().apply(sourceTable, this);
    }

    default MultiRowRecordAdapter<T>  createMultiRowRecordAdapter(PartitionedTable sourcePartitionedTable) {
        return getMultiRowPartitionedTableAdapterSupplier().apply(sourcePartitionedTable, this);
    }

    BiFunction<Table, RecordAdapterDescriptor<T>, MultiRowRecordAdapter<T>> getMultiRowAdapterSupplier();

    BiFunction<PartitionedTable, RecordAdapterDescriptor<T>, MultiRowRecordAdapter<T>> getMultiRowPartitionedTableAdapterSupplier();

}
