package io.deephaven.db.v2.parquet;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

import java.util.Map;

import static io.deephaven.db.v2.parquet.TypeInfos.getTypeInfo;

/**
 * Represents the results of a successful mapping between a {@link TableDefinition} and a {@link MessageType}.
 */
class MappedSchema {

    static MappedSchema create(
            final Map<String, Map<ParquetTableWriter.CacheTags, Object>> computedCache,
            final TableDefinition definition,
            final Index index,
            final Map<String, ? extends ColumnSource<?>> columnSourceMap,
            final ParquetInstructions instructions,
            final ColumnDefinition... extraColumns) {
        final MessageTypeBuilder builder = Types.buildMessage();
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            TypeInfos.TypeInfo typeInfo = getTypeInfo(computedCache, columnDefinition, index, columnSourceMap, instructions);
            Type schemaType = typeInfo.createSchemaType(columnDefinition, instructions);
            builder.addField(schemaType);
        }
        for (final ColumnDefinition<?> extraColumn : extraColumns) {
            builder.addField(getTypeInfo(computedCache, extraColumn, index, columnSourceMap, instructions).createSchemaType(extraColumn, instructions));
        }
        MessageType schema = builder.named("root");
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
