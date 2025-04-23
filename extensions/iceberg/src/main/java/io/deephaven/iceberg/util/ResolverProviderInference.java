//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.apache.iceberg.Table;
import org.immutables.value.Value;


@Value.Immutable
@BuildableStyle
public abstract class ResolverProviderInference extends ResolverProviderImpl implements ResolverProvider {

    public static Builder builder() {
        return ImmutableResolverProviderInference.builder();
    }

    @Value.Default
    public boolean usePartitioningColumns() {
        return false;
    }

    @Value.Default
    public boolean failOnUnsupportedTypes() {
        return false;
    }

    @Value.Default
    public InferenceInstructions.Namer.Factory namerFactory() {
        return InferenceInstructions.Namer.Factory.fieldName("_");
    }

    public interface Builder {

        Builder usePartitioningColumns(boolean usePartitioningColumns);

        Builder failOnUnsupportedTypes(boolean failOnUnsupportedTypes);

        Builder namerFactory(InferenceInstructions.Namer.Factory namerFactory);

        ResolverProviderInference build();
    }

    @Override
    final Resolver resolver(Table table) throws TypeInference.UnsupportedType {
        InferenceInstructions.Builder builder = inferenceBuilder()
                .schema(table.schema());
        if (usePartitioningColumns()) {
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
