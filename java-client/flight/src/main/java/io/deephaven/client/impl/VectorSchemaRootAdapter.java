package io.deephaven.client.impl;

import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for creating {@link VectorSchemaRoot}.
 */
public class VectorSchemaRootAdapter {

    /**
     * Convert a {@code table} into a {@link FieldVector}.
     *
     * @param table the table
     * @param allocator the allocator
     * @return the vector schema root
     */
    public static VectorSchemaRoot of(NewTable table, BufferAllocator allocator) {
        final List<FieldVector> fieldVectors = new ArrayList<>(table.numColumns());
        for (Column<?> column : table) {
            fieldVectors.add(FieldVectorAdapter.of(column, allocator));
        }
        final Schema schema = new Schema(fieldVectors.stream().map(ValueVector::getField).collect(Collectors.toList()));
        return new VectorSchemaRoot(schema, fieldVectors, table.size());
    }
}
