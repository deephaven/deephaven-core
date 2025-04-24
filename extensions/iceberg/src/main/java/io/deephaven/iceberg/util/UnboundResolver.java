//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.transforms.Transform;
import org.immutables.value.Value;

import java.util.Map;

/**
 * This is a resolver provider that will build a {@link Resolver} without needing to refer to an explicit Iceberg
 * {@link Schema}. This is useful when the caller knows the definition of the table they want to load, and can provide
 * an explicit mapping between the Deephaven columns and Iceberg fields. This is provided as a convenience for cases
 * where explicitly building a {@link Resolver} would be more tedious.
 */
@Value.Immutable
@BuildableStyle
public abstract class UnboundResolver extends ResolverProviderImpl implements ResolverProvider {

    public static Builder builder() {
        return ImmutableUnboundResolver.builder();
    }

    /**
     * The table definition.
     *
     * <p>
     * Callers should take care and only use {@link ColumnDefinition.ColumnType#Partitioning Partitioning} columns when
     * they know the Iceberg table will always have {@link Transform#isIdentity() identity} partitions for said columns.
     * In the general case, Iceberg partitions may evolve over time, which can break the assumptions Deephaven makes
     * about partitioning columns.
     *
     * @see Resolver#definition()
     */
    public abstract TableDefinition definition();

    /**
     * The map from Deephaven column names to instructions for mapping to Iceberg columns. Any columns from
     * {@link #definition()} not in this map will be assumed to be exact name matches for the fields in the
     * {@link Schema}.
     *
     * @see Resolver#columnInstructions()
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
                instructions = ColumnInstructions.schemaFieldName(dhColumnName);
            }
            // else {}, pass along the instructions as-is
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
    final void checkKeys() {
        definition().checkHasColumns(columnInstructions().keySet());
    }
}
