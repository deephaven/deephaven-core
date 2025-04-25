//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.ColumnDefinition;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.immutables.value.Value;

import java.util.Set;


/**
 * This provides a consolidated set of inference options for use in {@link LoadTableOptions}. This is useful when the
 * caller does not know the structure of the table to be loaded, and wants the resulting Deephaven definition (and
 * mapping to the Iceberg fields) to be inferred. This is a counterpart to the more advanced
 * {@link InferenceInstructions}, which requires the callers to be more explicit about the {@link Schema}.
 */
@Value.Immutable
@BuildableStyle
public abstract class InferenceResolver extends ResolverProviderImpl implements ResolverProvider {

    public static Builder builder() {
        return ImmutableInferenceResolver.builder();
    }

    /**
     * If {@link ColumnDefinition.ColumnType#Partitioning Partitioning} columns should be inferred based on the
     * {@link Table#spec() latest spec}. By default, is {@code false}.
     *
     * <p>
     * <b>Warning</b>: inferring partition columns for general-purpose use is dangerous. This is only meant to be
     * applied in situations where caller knows that the {@link Table#spec() latest spec} is safe to use for inference.
     *
     * @see InferenceInstructions#spec() InferenceInstructions for more details.
     */
    @Value.Default
    public boolean inferPartitioningColumns() {
        return false;
    }

    /**
     * If inference should fail if any of the Iceberg fields fail to map to Deephaven columns. By default, is
     * {@code false}.
     */
    @Value.Default
    public boolean failOnUnsupportedTypes() {
        return false;
    }

    /**
     * The namer factory. Defaults to {@code fieldName("_")}, which will create Deephaven column name by joining
     * together the {@link Types.NestedField#name() field names} with an underscore and
     * {@link NameValidator#legalizeColumnName(String, Set) legalize} the name if necessary.
     *
     * @see InferenceInstructions.Namer.Factory#fieldName(String)
     */
    @Value.Default
    public InferenceInstructions.Namer.Factory namerFactory() {
        return InferenceInstructions.defaultNamerFactory();
    }

    /**
     * The schema to use for inference. By default, is {@link SchemaProvider#fromCurrent()}.
     */
    @Value.Default
    public SchemaProvider schema() {
        return SchemaProvider.fromCurrent();
    }

    public interface Builder {

        Builder inferPartitioningColumns(boolean inferPartitioningColumns);

        Builder failOnUnsupportedTypes(boolean failOnUnsupportedTypes);

        Builder namerFactory(InferenceInstructions.Namer.Factory namerFactory);

        Builder schema(SchemaProvider schema);

        InferenceResolver build();
    }

    @Override
    final Resolver resolver(Table table) throws TypeInference.UnsupportedType {
        InferenceInstructions.Builder builder = inferenceBuilder()
                .schema(((SchemaProviderInternal.SchemaProviderImpl) schema()).getSchema(table));
        if (inferPartitioningColumns()) {
            builder.spec(table.spec());
        }
        return Resolver.infer(builder.build());
    }

    private InferenceInstructions.Builder inferenceBuilder() {
        return InferenceInstructions.builder()
                .namerFactory(namerFactory())
                .failOnUnsupportedTypes(failOnUnsupportedTypes());
    }
}
