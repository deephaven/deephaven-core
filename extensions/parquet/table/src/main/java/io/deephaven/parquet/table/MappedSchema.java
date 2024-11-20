//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

import java.util.Map;

import static io.deephaven.parquet.table.TypeInfos.getTypeInfo;

/**
 * Represents the results of a successful mapping between a {@link TableDefinition} and a {@link MessageType}.
 */
class MappedSchema {

    @VisibleForTesting
    static final String SCHEMA_NAME = "root";

    static MappedSchema create(
            final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            final TableDefinition definition,
            final RowSet rowSet,
            final Map<String, ? extends ColumnSource<?>> columnSourceMap,
            final ParquetInstructions instructions) {
        final MessageTypeBuilder builder = Types.buildMessage();
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            builder.addField(createType(computedCache, columnDefinition, rowSet, columnSourceMap, instructions));
        }
        final MessageType schema = builder.named(SCHEMA_NAME);
        return new MappedSchema(definition, schema);
    }

    @VisibleForTesting
    static Type createType(
            final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            final ColumnDefinition<?> columnDefinition,
            final RowSet rowSet,
            final Map<String, ? extends ColumnSource<?>> columnSourceMap,
            final ParquetInstructions instructions) {
        final TypeInfos.TypeInfo typeInfo =
                getTypeInfo(computedCache, columnDefinition, rowSet, columnSourceMap, instructions);
        return typeInfo.createSchemaType(columnDefinition, instructions);
    }

    private final TableDefinition tableDefinition;
    private final MessageType parquetSchema;

    private MappedSchema(TableDefinition tableDefinition, MessageType parquetSchema) {
        this.tableDefinition = tableDefinition;
        this.parquetSchema = parquetSchema;
    }

    TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    MessageType getParquetSchema() {
        return parquetSchema;
    }
}
