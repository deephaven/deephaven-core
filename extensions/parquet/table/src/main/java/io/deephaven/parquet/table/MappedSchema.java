//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableDefinition;
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

    static MappedSchema create(
            final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            final TableDefinition definition,
            final RowSet rowSet,
            final Map<String, ? extends ColumnSource<?>> columnSourceMap,
            final ParquetInstructions instructions) {
        final MessageTypeBuilder builder = Types.buildMessage();
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            final TypeInfos.TypeInfo typeInfo =
                    getTypeInfo(computedCache, columnDefinition, rowSet, columnSourceMap, instructions);
            final Type schemaType = typeInfo.createSchemaType(columnDefinition, instructions);
            builder.addField(schemaType);
        }
        final MessageType schema = builder.named("root");
        return new MappedSchema(definition, schema);
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
