//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionSpecHelper {

    public static Optional<PartitionField> find(PartitionSpec spec, int partitionFieldId) {
        PartitionField found = null;
        for (final PartitionField partitionField : spec.fields()) {
            if (partitionField.fieldId() == partitionFieldId) {
                if (found != null) {
                    throw new IllegalStateException(String
                            .format("Found multiple partition fields with partition field id %d", partitionFieldId));
                }
                found = partitionField;
            }
        }
        return Optional.ofNullable(found);
    }

    public static PartitionField get(PartitionSpec spec, int partitionFieldId) throws SchemaHelper.PathException {
        final Optional<PartitionField> partitionField = find(spec, partitionFieldId);
        if (partitionField.isEmpty()) {
            throw new SchemaHelper.PathException(
                    String.format("Unable to find partition field id %d", partitionFieldId));
        }
        return partitionField.get();
    }

    public static Optional<PartitionField> findIdentityForSchemaFieldId(PartitionSpec spec, int schemaFieldId) {
        for (final PartitionField partitionField : spec.fields()) {
            if (!partitionField.transform().isIdentity()) {
                continue;
            }
            if (partitionField.sourceId() == schemaFieldId) {
                return Optional.of(partitionField);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns all of the {@link PartitionSpec} referenced by {@code snapshot}. Does not perform any de-duplication.
     */
    public static Stream<PartitionSpec> referencedSpecs(
            @NotNull final FileIO io,
            @NotNull final Map<Integer, PartitionSpec> specsById,
            @Nullable final Snapshot snapshot) {
        return snapshot == null
                ? Stream.empty()
                : snapshot.dataManifests(io)
                        .stream()
                        .mapToInt(ManifestFile::partitionSpecId)
                        .mapToObj(specsById::get)
                        .peek(Objects::requireNonNull);
    }


    /**
     * Returns the {@link PartitionSpec#specId() highest-valued} spec from {@code it} that is an
     * {@link #isUnorderedSubset(PartitionSpec, PartitionSpec) unordered subset} of all the other specs from {@code it}.
     * When no such spec from {@code it} exists, {@link PartitionSpec#unpartitioned()} will be returned.
     */
    public static PartitionSpec newestUnorderedSubset(Iterator<PartitionSpec> it) {
        if (!it.hasNext()) {
            return PartitionSpec.unpartitioned();
        }
        PartitionSpec current = it.next();
        while (it.hasNext()) {
            final PartitionSpec next = it.next();
            if (current.equals(next)) {
                continue;
            }
            // Note: it's possible for both of these to be true even though they aren't equal, given the unordered
            // qualifier
            final boolean currentIsSubset = isUnorderedSubset(current, next);
            final boolean nextIsSubset = isUnorderedSubset(next, current);
            if (!currentIsSubset && !nextIsSubset) {
                // There is no universal, unordered subset (besides unpartitioned)
                return PartitionSpec.unpartitioned();
            }
            if (currentIsSubset && nextIsSubset) {
                // Use the partition spec with the highest id, likely means it's the newer spec
                if (next.specId() > current.specId()) {
                    current = next;
                }
            } else if (nextIsSubset) {
                current = next;
            }
        }
        return current;
    }

    /**
     * True if {@code other} contains all of the {@link PartitionSpec#fields()} from {@code spec} (in any order).
     */
    public static boolean isUnorderedSubset(PartitionSpec spec, PartitionSpec other) {
        for (final PartitionField field : spec.fields()) {
            final PartitionField otherField = find(other, field.fieldId()).orElse(null);
            if (!field.equals(otherField)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a spec that is "safe" to use for partition inference when considering only the data files referenced by
     * {@code snapshot}.
     */
    public static PartitionSpec partitionForInference(
            @NotNull final Table table,
            @Nullable final Snapshot snapshot) {
        return partitionForInference(table, snapshot == null ? List.of() : List.of(snapshot));
    }

    /**
     * Returns a spec that is "safe" to use for partition inference when considering only the data files referenced by
     * {@code snapshots}.
     */
    public static PartitionSpec partitionForInference(
            @NotNull final Table table,
            @NotNull final Collection<Snapshot> snapshots) {
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
        try (final Stream<PartitionSpec> partitionSpecStream = snapshots
                .stream()
                .flatMap(snapshot -> referencedSpecs(table.io(), table.specs(), snapshot))) {
            return newestUnorderedSubset(partitionSpecStream.iterator());
        }
    }

    public static PartitionSpec limit(PartitionSpec spec, Schema schema) {

        final List<PartitionField> contained = new ArrayList<>(spec.fields().size());
        for (final PartitionField field : spec.fields()) {
            final Types.NestedField sourceField = schema.findField(field.sourceId());
            if (sourceField == null) {
                continue;
            }
            contained.add(field);
        }
        PartitionSpec.builderFor(schema);
        return null;
    }

    /**
     * {@link Partitioning#groupingKeyType(Schema, Collection)}
     */
    static List<PartitionField> commonDataFilePartitionFields(
            @NotNull final FileIO io,
            @NotNull final Map<Integer, PartitionSpec> specsById,
            @Nullable final Snapshot snapshot) {
        if (snapshot == null) {
            return List.of();
        }
        final Set<PartitionField> intersection = new HashSet<>();
        boolean first = true;
        for (ManifestFile manifestFile : snapshot.dataManifests(io)) {
            final PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
            // We don't expect there to be a null spec; but to be safe, if we get one, we'll treat it as empty.
            final List<PartitionField> fields = spec == null ? List.of() : spec.fields();
            if (first) {
                intersection.addAll(fields);
                first = false;
            } else {
                intersection.retainAll(fields);
            }
            if (intersection.isEmpty()) {
                break;
            }
        }
        return intersection
                .stream()
                .sorted(Comparator.comparingInt(PartitionField::fieldId))
                .collect(Collectors.toUnmodifiableList());
    }

    public static PartitionSpec virtualSpec(
            @NotNull final Schema schema,
            @NotNull final FileIO io,
            @NotNull final Map<Integer, PartitionSpec> specsById,
            @Nullable final Snapshot snapshot) {
        // commonDataFilePartitionFields()
        // .stream()
        // .filter(PartitionSpecHelper::isIdentity)
        // .map(pf -> SchemaHelper.findFieldPath(schema, pf))
        final PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);



        // todo: should we set a negative spec id?
        commonDataFilePartitionFields(io, specsById, snapshot)
                .stream()
                // .filter(PartitionSpecHelper::isIdentity)
                .map(pf -> SchemaHelper.findFieldPath(schema, pf))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(SchemaHelper::toFieldName)
                .forEachOrdered(builder::identity);
        return builder.build();
    }

    private static void what(PartitionSpec.Builder builder, Schema schema, PartitionField pf) {

        final List<Types.NestedField> fieldPath = SchemaHelper.findFieldPath(schema, pf).orElse(null);
        if (fieldPath == null) {
            return;
        }
        final String name = SchemaHelper.toFieldName(fieldPath);
        builder.identity("what");


    }

    private static boolean isIdentity(PartitionField pf) {
        return pf.transform().isIdentity();
    }
}
