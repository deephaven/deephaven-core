//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

import java.util.List;
import java.util.OptionalInt;

/**
 * The instructions for mapping a Deephaven column in {@link Resolver#columnInstructions()}.
 */
@Value.Immutable
@BuildableStyle
public abstract class ColumnInstructions {

    /**
     * Create an unmapped column instructions, where neither schema field, nor partition field, is referenced.
     *
     * @return the column instructions
     */
    public static ColumnInstructions unmapped() {
        return ImmutableColumnInstructions.builder().build();
    }

    /**
     * Create column instructions for a schema field.
     *
     * @param fieldId the field id
     * @return the column instructions
     */
    public static ColumnInstructions schemaField(int fieldId) {
        return ImmutableColumnInstructions.builder().schemaFieldId(fieldId).build();
    }

    /**
     * Create column instructions for a partition field.
     *
     * @param partitionFieldId the partition field id
     * @return the column instructions
     */
    public static ColumnInstructions partitionField(int partitionFieldId) {
        return ImmutableColumnInstructions.builder().partitionFieldId(partitionFieldId).build();
    }

    /**
     * The schema field id.
     */
    public abstract OptionalInt schemaFieldId();

    /**
     * The partition field id.
     */
    public abstract OptionalInt partitionFieldId();

    // Note: very likely there will be additions here to support future additions; codecs, conversions, etc.

    final boolean isUnmapped() {
        return schemaFieldId().isEmpty() && partitionFieldId().isEmpty();
    }

    final List<NestedField> schemaFieldPath(Schema schema) throws SchemaHelper.PathException {
        return SchemaHelper.fieldPath(schema, schemaFieldId().orElseThrow());
    }

    final PartitionField partitionField(PartitionSpec spec) throws SchemaHelper.PathException {
        return PartitionSpecHelper.get(spec, partitionFieldId().orElseThrow());
    }

    final PartitionField partitionFieldFromSchemaFieldId(PartitionSpec spec) throws SchemaHelper.PathException {
        final int fieldId = schemaFieldId().orElseThrow();
        final List<PartitionField> partitionFields = spec.getFieldsBySourceId(fieldId);
        if (partitionFields.isEmpty()) {
            throw new SchemaHelper.PathException(String
                    .format("No PartitionField with source field id %d exists in PartitionSpec %s", fieldId, spec));
        }
        if (partitionFields.size() > 1) {
            // Note: we are *not* being lenient and assuming the user meant identity transform when there are multiple
            // partition specs for a given source field id. In that case, the user should be precise and use
            // partitionFieldId.
            throw new SchemaHelper.PathException(String.format(
                    "Multiple PartitionFields with source field id %d exist in PartitionSpec %s. Must be more explicit and use partitionField.",
                    fieldId, spec));
        }
        return partitionFields.get(0);
    }

    @Value.Check
    final void checkType() {
        if (partitionFieldId().isPresent() && schemaFieldId().isPresent()) {
            throw new IllegalArgumentException(
                    "ColumnInstructions can't have both a schema field id and a partition field id");
        }
    }
}
