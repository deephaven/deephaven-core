//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.json;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.updaters.RecordUpdater;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * A RecordAdapterDescriptor for JSON records, which are represented as {@link ObjectNode} instances. This differs from
 * the default produced by {@link io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptorBuilder} in that its
 * {@link #getMultiRowAdapterSupplier()} and {@link #getMultiRowPartitionedTableAdapterSupplier()} generate
 * {@link MultiRowRecordAdapter MultiRowRecordAdapters} specific to the columns defined in the descriptor.
 */
public class JsonRecordAdapterDescriptor implements RecordAdapterDescriptor<ObjectNode> {

    private final Map<String, RecordUpdater<ObjectNode, ?>> columnAdapters;

    public JsonRecordAdapterDescriptor(final Map<String, RecordUpdater<ObjectNode, ?>> columnAdapters) {
        this.columnAdapters = columnAdapters;
    }

    @Override
    public Map<String, RecordUpdater<ObjectNode, ?>> getColumnAdapters() {
        return columnAdapters;
    }

    @Override
    public @NotNull ObjectNode getEmptyRecord() {
        return JsonNodeFactory.instance.objectNode();
    }

    @Override
    public BiFunction<Table, RecordAdapterDescriptor<ObjectNode>, MultiRowRecordAdapter<ObjectNode>> getMultiRowAdapterSupplier() {
        return (table, descriptor) -> {
            final Class<? extends BaseJsonRecordAdapter> c = new JsonRecordAdapterGenerator(descriptor).generate();
            try {
                return c.getConstructor(Table.class, RecordAdapterDescriptor.class).newInstance(table, descriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not instantiate generated JsonRecordAdapter", e);
            }
        };
    }

    @Override
    public BiFunction<PartitionedTable, RecordAdapterDescriptor<ObjectNode>, MultiRowRecordAdapter<ObjectNode>> getMultiRowPartitionedTableAdapterSupplier() {
        return (partitionedTable, descriptor) -> {
            final Class<? extends BaseJsonRecordAdapter> c = new JsonRecordAdapterGenerator(descriptor).generate();
            try {
                return c.getConstructor(PartitionedTable.class, RecordAdapterDescriptor.class)
                        .newInstance(partitionedTable, descriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not instantiate generated JsonRecordAdapter", e);
            }
        };
    }
}
