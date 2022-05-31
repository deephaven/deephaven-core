package io.deephaven.client.impl;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.TableHeader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for creating {@link Schema}.
 */
public class SchemaAdapter {

    /**
     * Convert a {@code header} into a {@link Schema}.
     *
     * @param header the header
     * @return the schema
     */
    public static Schema of(TableHeader header) {
        final List<Field> fields = new ArrayList<>(header.numColumns());
        for (ColumnHeader<?> columnHeader : header) {
            fields.add(FieldAdapter.of(columnHeader));
        }
        return new Schema(fields);
    }
}
