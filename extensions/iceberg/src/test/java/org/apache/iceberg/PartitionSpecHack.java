//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package org.apache.iceberg;

import java.util.List;
import java.util.stream.Collectors;

public final class PartitionSpecHack {

    // Note: we might like this to migrate to Core+, as we don't need to serialize in Core?

    /**
     * Returns a partition spec that is compatible with {@code schema}; that is, one where only source ids present in
     * {@code schema} are retained by the returned spec. In the case where the {@code spec} is already compatible with
     * {@code schema}, {@code spec} will be returned. In the case where no spec fields are compatible,
     * {@link PartitionSpec#unpartitioned()} will be returned. Otherwise, a new partition spec will be returned (with
     * spec id {@code -1}), limited to only the compatible partition fields.
     *
     * <p>
     * This method is unsafe to use in the case where {@code spec} actually needs to be limited, as it relies on
     * {@link #newPartitionSpec(int, Schema, List)}.
     *
     * <p>
     * This method serves as a helper to normalize serialization use cases, where we only want / need to serialize the
     * relevant subset of partition fields via {@link PartitionSpecParser#toJson(PartitionSpec)} (so we can safely
     * {@link PartitionSpecParser#fromJson(Schema, String)} without needing to do extra serialization of
     * {@link PartitionSpec#schema()}).
     */
    public static PartitionSpec compatibleSpec(Schema schema, PartitionSpec spec) {
        final List<PartitionField> partitionFields = spec.fields().stream()
                .filter(pf -> hasFieldId(schema, pf.sourceId()))
                .collect(Collectors.toList());
        return partitionFields.size() == spec.fields().size()
                ? spec
                : partitionFields.isEmpty()
                        ? PartitionSpec.unpartitioned()
                        : newPartitionSpec(-1, schema, partitionFields);
    }

    /**
     * Creates a new partition spec using {@code fields}, bound to {@code schema}. This method is unsafe, as it uses
     * internal Iceberg APIs that may break in the future.
     */
    public static PartitionSpec newPartitionSpec(int newSpecId, Schema schema, List<PartitionField> fields) {
        return newPartitionSpecBuilder(newSpecId, schema, fields).build();
    }

    public static PartitionSpec newPartitionSpecUnchecked(int newSpecId, Schema schema, List<PartitionField> fields) {
        return newPartitionSpecBuilder(newSpecId, schema, fields).buildUnchecked();
    }

    private static PartitionSpec.Builder newPartitionSpecBuilder(int newSpecId, Schema schema,
            List<PartitionField> fields) {
        final PartitionSpec.Builder builder = PartitionSpec
                .builderFor(schema)
                .withSpecId(newSpecId);
        for (final PartitionField field : fields) {
            builder.add(field.sourceId(), field.fieldId(), field.name(), field.transform());
        }
        return builder;
    }

    private static boolean hasFieldId(Schema schema, int fieldId) {
        return schema.findField(fieldId) != null;
    }
}
