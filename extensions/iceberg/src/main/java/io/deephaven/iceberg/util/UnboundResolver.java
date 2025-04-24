//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.immutables.value.Value;

import java.util.Map;

/**
 * Used to build a simple resolver using a {@link Schema} and {@link TableDefinition} names. Useful when implementing
 * simple column renames when mapping Iceberg fields to Deephaven columns.
 */
@Value.Immutable
@BuildableStyle
public abstract class UnboundResolver extends ResolverProviderImpl implements ResolverProvider {

    public static Builder builder() {
        return ImmutableUnboundResolver.builder();
    }

    /**
     * The table definition to use for to build the {@link Resolver}. Any partitioning columns in the table definition
     * will be internally mapped to partition fields from the latest spec of the Iceberg table.
     *
     * @see Resolver#definition()
     */
    public abstract TableDefinition definition();

    /**
     * The map from Deephaven column names to instructions for mapping to Iceberg columns. Users can use this to provide
     * the schema field ID to map corresponding Iceberg column to the Deephaven column.
     */
    public abstract Map<String, ColumnInstructions> columnInstructions();

    /**
     * The schema to use for inference. By default, is {@link SchemaProvider#fromCurrent()}.
     */
    @Value.Default
    public SchemaProvider schema() {
        return SchemaProvider.fromCurrent();
    }

    @Override
    final Resolver resolver(Table table) {
        final Schema schema = ((SchemaProviderInternal.SchemaProviderImpl) schema()).getSchema(table);
        final Map<String, ColumnInstructions> columnInstructionsMap = columnInstructions();
        final Resolver.Builder builder = Resolver.builder()
                .schema(schema)
                .definition(definition());
        if (!definition().getPartitioningColumns().isEmpty()) {
            builder.spec(table.spec());
        }
        for (final ColumnDefinition<?> columnDefinition : definition().getColumns()) {
            final String dhColumnName = columnDefinition.getName();
            ColumnInstructions instructions = columnInstructionsMap.get(dhColumnName);
            if (instructions == null) {
                final Types.NestedField icebergField = schema.findField(dhColumnName);
                if (icebergField == null) {
                    throw new IllegalArgumentException(
                            String.format("Column `%s` from deephaven table definition not found in Iceberg schema",
                                    dhColumnName));
                }
                instructions = ColumnInstructions.schemaField(icebergField.fieldId());
            } else {
                final int fieldId = instructions.schemaFieldId().getAsInt();
                if (schema.findField(fieldId) == null) {
                    throw new IllegalArgumentException(
                            String.format("Field ID %d derived from column instructions map for column %s not found " +
                                    "in provided schema", fieldId, dhColumnName));
                }
            }
            builder.putColumnInstructions(dhColumnName, instructions);
        }
        return builder.build();
    }

    public interface Builder {
        Builder schema(SchemaProvider schema);

        Builder definition(TableDefinition definition);

        Builder putColumnInstructions(String key, ColumnInstructions value);

        Builder putAllColumnInstructions(Map<String, ? extends ColumnInstructions> entries);

        UnboundResolver build();
    }

    @Value.Check
    final void verifySchemaIdPresentInMapping() {
        if (columnInstructions().isEmpty()) {
            return;
        }
        for (final Map.Entry<String, ColumnInstructions> entry : columnInstructions().entrySet()) {
            final String columnNameFromMap = entry.getKey();
            final ColumnInstructions instructions = entry.getValue();
            if (instructions.schemaFieldId().isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("Column `%s` from column instructions map does not have schema field id",
                                columnNameFromMap));
            }
        }
    }
}
