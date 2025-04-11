//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.iceberg.internal.PartitionSpecHelper;
import io.deephaven.iceberg.internal.SchemaHelper;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

import java.util.List;
import java.util.OptionalInt;

@Value.Immutable
@BuildableStyle
public abstract class ColumnInstructions {

    public static ColumnInstructions schemaField(int fieldId) {
        return ImmutableColumnInstructions.builder().schemaFieldId(fieldId).build();
    }

    // TODO: should we even allow user to configure this? we don't really want to expose partition columns to them
    // (except identity, but then it's already in the schema, so they don't _need_ this).
    // really, it means you want to use the *name* from the partition field as well
    public static ColumnInstructions partitionField(int partitionFieldId) {
        return ImmutableColumnInstructions.builder().partitionFieldId(partitionFieldId).build();
    }

    public abstract OptionalInt schemaFieldId();

    public abstract OptionalInt partitionFieldId();

    PartitionField partitionField(PartitionSpec spec) throws SchemaHelper.PathException {
        return PartitionSpecHelper.get(spec, partitionFieldId().orElseThrow());
    }

    PartitionField partitionFieldFromSchemaFieldId(PartitionSpec spec) throws SchemaHelper.PathException {
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

    List<NestedField> schemaFieldPath(Schema schema) throws SchemaHelper.PathException {
        return SchemaHelper.fieldPath(schema, schemaFieldId().orElseThrow());
    }

    // Note: very likely there will be additions here to support future additions; codecs, conversions, etc.

    @Value.Check
    final void checkBase() {
        if (partitionFieldId().isPresent() == schemaFieldId().isPresent()) {
            throw new IllegalArgumentException(
                    "ColumnInstructions must be schema based or partition based");
        }
    }
}
