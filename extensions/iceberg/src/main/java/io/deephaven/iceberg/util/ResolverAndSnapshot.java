//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.internal.NameMappingUtil;
import io.deephaven.iceberg.internal.PartitionSpecHelper;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;


final class ResolverAndSnapshot {

    static ResolverAndSnapshot create(
            @NotNull final Table table,
            @Nullable final Resolver explicitResolver,
            @Nullable final Snapshot explicitSnapshot,
            final boolean inferWithPartitionSpec,
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
                    if (!inferWithPartitionSpec) {
                        instructions = InferenceInstructions.of(schema);
                    } else {
                        final PartitionSpec bestSpecForPartitionInference = partitionForInference(table, snapshot);
                        if (bestSpecForPartitionInference.isUnpartitioned()) {
                            instructions = InferenceInstructions.of(schema);
                        } else {
                            bestSpecForPartitionInference.toUnbound().bind(schema);
                            instructions = InferenceInstructions.builder()
                                    .schema(schema)
                                    .spec(bestSpecForPartitionInference)
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

    private static PartitionSpec partitionForInference(@NotNull final Table table, @Nullable final Snapshot snapshot) {
        // We could improve partitioning inference logic at the expense of much additional code.
        // As it is, this logic is probably good enough for 99% of the cases.
        // It does not handle the case where the data is partitioned in 'interesting' ways (such that
        // no single partition spec is safe to use for inference). And in those cases, we probably want
        // the callers be explicit and not do inference anyways.
        //
        // Right now, we are finding the newest partition spec that is referenced by the snapshot such
        // that that partition spec is an unordered subset of all the other partition specs referenced
        // by the snapshot.
        //
        // For example:
        // (S1 = [I1, I2], S2 = [I2, I3, I1]) would result in using the S1 spec for inference
        // (S1 = [I1, I2], S2 = [I1, I2, I3], S3 = [I2, I1]) would result in using the S3 spec for
        // inference
        //
        // This logic fails to account for cases where some "virtual" partition spec (ie, a theoretical
        // new partition spec built off of existing partition fields) would be most appropriate. For
        // example:
        //
        // (S1 = [I1, I2, I3], S2 = [I3, I2, I4]) currently returns "unpartitioned" because we if we
        // returned S1, we are lying (not all the data is partitioned by I1), and if we returned S2, we
        // are lying (not all the data is partitioned by I4).
        //
        // As mentioned earlier, we could improve this logic with additional code, likely by passing
        // down the partition spec to use along with exclusions (ie, from the previous example, "use S2,
        // but skip I4"). We could also theoretically create a "virtual" partition spec [I3, I2], just
        // for use by the resolver (not actually created by Iceberg), but the APIs for constructing
        // PartitionSpec `org.apache.iceberg.PartitionSpec.Builder` are lacking in their specificity;
        // they don't allow us to actually construct PartitionSpec with the field ids we require.
        return PartitionSpecHelper.newestUnorderedSubset(
                PartitionSpecHelper.referencedSpecs(table.io(), table.specs(), snapshot).iterator());
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
