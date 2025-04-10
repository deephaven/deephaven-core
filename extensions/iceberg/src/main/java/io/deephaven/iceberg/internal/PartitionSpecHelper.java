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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
                // There is no universal, unordered subset
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

    public static boolean isUnorderedSubset(PartitionSpec spec, PartitionSpec other) {
        // if (spec.equals(other)) {
        // return true;
        // }
        for (final PartitionField field : spec.fields()) {
            final PartitionField otherField = find(other, field.fieldId()).orElse(null);
            if (!field.equals(otherField)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isIdentity(PartitionField pf) {
        return pf.transform().isIdentity();
    }
}
