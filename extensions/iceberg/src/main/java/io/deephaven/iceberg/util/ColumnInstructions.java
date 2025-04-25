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
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The instructions for mapping a Deephaven column in {@link Resolver#columnInstructions()}.
 */
@Value.Immutable
@BuildableStyle
public abstract class ColumnInstructions {

    /**
     * Create an unmapped column instructions, where neither schema field nor partition field is referenced.
     *
     * @return the column instructions
     */
    public static ColumnInstructions unmapped() {
        return ImmutableColumnInstructions.builder().build();
    }

    /**
     * Create column instructions for a schema field by {@link NestedField#fieldId() field id}.
     *
     * @param fieldId the field id
     * @return the column instructions
     */
    public static ColumnInstructions schemaField(int fieldId) {
        return ImmutableColumnInstructions.builder().schemaFieldId(fieldId).build();
    }

    /**
     * Create column instructions for a schema field by field name.
     *
     * <p>
     * Warning: referencing a field by name is discouraged, as it is not guaranteed to be a stable reference across
     * {@link Schema} evolution. This is provided as a convenience.
     *
     * @param fieldName the field name
     * @return the column instructions
     */
    public static ColumnInstructions schemaFieldName(String fieldName) {
        return ImmutableColumnInstructions.builder().schemaFieldName(fieldName).build();
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

    // Implementation note: it's important that all the fields to construct this be publicly accessible. See
    // implementation note on Resolver.

    /**
     * The schema field id.
     */
    public abstract OptionalInt schemaFieldId();

    /**
     * The schema field name.
     */
    public abstract Optional<String> schemaFieldName();

    /**
     * The partition field id.
     */
    public abstract OptionalInt partitionFieldId();

    /**
     * If this is an unmapped instructions. That is, there is no reference to a schema field nor partition field.
     */
    public final boolean isUnmapped() {
        return schemaFieldId().isEmpty() && schemaFieldName().isEmpty() && partitionFieldId().isEmpty();
    }

    // Note: very likely there will be additions here to support future additions; codecs, conversions, etc.

    final List<NestedField> schemaFieldPathById(Schema schema) throws SchemaHelper.PathException {
        return SchemaHelper.fieldPath(schema, schemaFieldId().orElseThrow());
    }

    final List<NestedField> schemaFieldPathByName(Schema schema) throws SchemaHelper.PathException {
        final NestedField field = fieldByName(schema);
        return SchemaHelper.fieldPath(schema, field.fieldId());
    }

    final PartitionField partitionField(PartitionSpec spec) throws SchemaHelper.PathException {
        return PartitionSpecHelper.get(spec, partitionFieldId().orElseThrow());
    }

    final PartitionField partitionFieldFromSchemaFieldId(PartitionSpec spec) throws SchemaHelper.PathException {
        return partitionFieldImpl(spec, schemaFieldId().orElseThrow());
    }

    final PartitionField partitionFieldFromSchemaFieldName(Schema schema, PartitionSpec spec)
            throws SchemaHelper.PathException {
        return partitionFieldImpl(spec, fieldByName(schema).fieldId());
    }

    final ColumnInstructions reassignWithSchemaField(int fieldId) {
        // May need to have more copy logic here in the future
        return ColumnInstructions.schemaField(fieldId);
    }

    final ColumnInstructions reassignWithPartitionField(int partitionFieldId) {
        // May need to have more copy logic here in the future
        return ColumnInstructions.partitionField(partitionFieldId);
    }

    private NestedField fieldByName(Schema schema) {
        final String fieldName = schemaFieldName().orElseThrow();
        final NestedField field = schema.findField(fieldName);
        if (field == null) {
            throw new Resolver.MappingException(String.format("Unable to find field by name: `%s`", fieldName));
        }
        return field;
    }

    private static PartitionField partitionFieldImpl(PartitionSpec spec, int fieldId)
            throws SchemaHelper.PathException {
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
}
