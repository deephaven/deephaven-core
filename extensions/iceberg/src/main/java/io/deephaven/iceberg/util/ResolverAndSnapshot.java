//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.internal.NameMappingUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;

import static io.deephaven.iceberg.internal.PartitionSpecHelper.partitionForInference;


final class ResolverAndSnapshot {

    static ResolverAndSnapshot create(
            @NotNull final Table table,
            @Nullable final Resolver explicitResolver,
            @Nullable final Snapshot explicitSnapshot,
            final boolean withPartitionInference,
            final boolean withNameMapping) {
        if (explicitResolver != null && explicitSnapshot != null) {
            return new ResolverAndSnapshot(explicitResolver, explicitSnapshot);
        }
        final Resolver resolver;
        final Snapshot snapshot;
        if (explicitResolver == null) {
            snapshot = explicitSnapshot == null
                    ? table.currentSnapshot()
                    : explicitSnapshot;
            {
                final InferenceInstructions instructions;
                {
                    final Schema schema = explicitSnapshot == null
                            ? table.schema()
                            : table.schemas().get(explicitSnapshot.schemaId());
                    if (!withPartitionInference) {
                        instructions = InferenceInstructions.of(schema);
                    } else {
                        final PartitionSpec partitionSpec = partitionForInference(table, snapshot);
                        if (partitionSpec.isUnpartitioned()) {
                            instructions = InferenceInstructions.of(schema);
                        } else {
                            instructions = InferenceInstructions.builder()
                                    .schema(schema)
                                    .spec(partitionSpec)
                                    .build();
                        }
                    }
                }
                Resolver.Builder builder;
                try {
                    builder = Resolver.inferBuilder(instructions);
                } catch (Inference.UnsupportedType e) {
                    throw new RuntimeException(e);
                }
                if (withNameMapping) {
                    NameMappingUtil.readNameMappingDefault(table).ifPresent(builder::nameMapping);
                }
                resolver = builder.build();
            }
        } else {
            Assert.eqNull(explicitSnapshot, "explicitSnapshot");
            resolver = explicitResolver;
            snapshot = table.currentSnapshot();
        }
        return new ResolverAndSnapshot(resolver, snapshot);
    }

    private final Resolver resolver;
    private final Snapshot snapshot;

    private ResolverAndSnapshot(final Resolver di, final Snapshot snapshot) {
        this.resolver = Objects.requireNonNull(di);
        this.snapshot = snapshot;
    }

    public Resolver resolver() {
        return resolver;
    }

    public Optional<Snapshot> snapshot() {
        return Optional.ofNullable(snapshot);
    }
}
