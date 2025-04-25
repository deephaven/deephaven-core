//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.SimpleStyle;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 * A path into a {@link Schema}.
 */
@Value.Immutable
@SimpleStyle
abstract class FieldPath {

    /**
     * Creates a field path that resolves fields via {@link NestedField#fieldId()}.
     *
     * @param fieldIdPath the field id path
     * @return the field path
     */
    public static FieldPath of(int... fieldIdPath) {
        return ImmutableFieldPath.of(fieldIdPath);
    }

    /**
     * Creates a field path that resolves fields via {@link NestedField#fieldId()}. This method ensures that the field
     * id path exists in {@code schema}.
     *
     * @param schema the schema
     * @param fieldIdPath the field id path
     * @return the field path
     */
    public static FieldPath of(Schema schema, int... fieldIdPath) throws SchemaHelper.PathException {
        // This checks that the fieldIdPath is correct
        SchemaHelper.fieldPath(schema, fieldIdPath);
        return of(fieldIdPath);
    }

    public static FieldPath get(Schema schema, int fieldId) throws SchemaHelper.PathException {
        final Optional<FieldPath> fieldPath = find(schema, fieldId);
        if (fieldPath.isEmpty()) {
            throw new SchemaHelper.PathException(String.format("Unable to find field id %d", fieldId));
        }
        return fieldPath.get();
    }

    public static FieldPath get(Schema schema, PartitionField partitionField) throws SchemaHelper.PathException {
        return find(schema, partitionField).orElseThrow(() -> new SchemaHelper.PathException(
                String.format("Unable to find partition field `%s` in schema", partitionField)));
    }

    public static Optional<FieldPath> find(Schema schema, int fieldId) {
        return find(new ArrayList<>(), schema.asStruct(), fieldId).map(FieldPath::fp);
    }

    public static Optional<FieldPath> find(Schema schema, PartitionField partitionField) {
        return find(schema, partitionField.sourceId());
    }

    private static FieldPath fp(List<NestedField> x) {
        return of(x.stream().mapToInt(NestedField::fieldId).toArray());
    }

    private static Optional<List<NestedField>> find(List<NestedField> context, Type.NestedType nestedType,
            int fieldId) {
        final NestedField field = nestedType.field(fieldId);
        if (field != null) {
            context.add(field);
            return Optional.of(context);
        }
        for (NestedField nestedField : nestedType.fields()) {
            final Type fieldType = nestedField.type();
            if (!fieldType.isNestedType()) {
                continue;
            }
            // push
            context.add(nestedField);
            final Optional<List<NestedField>> found = find(context, fieldType.asNestedType(), fieldId);
            if (found.isPresent()) {
                return found;
            }
            // pop
            context.remove(context.size() - 1);
        }
        return Optional.empty();
    }

    /**
     * Creates a field path that resolves fields via {@link NestedField#name()}. This method ensures that the field name
     * path exists in {@code schema}.
     *
     * @param schema the schema
     * @param fieldNamePath the field name path
     * @return the field path
     */
    public static FieldPath of(Schema schema, String... fieldNamePath) throws SchemaHelper.PathException {
        return fp(SchemaHelper.fieldPath(schema, fieldNamePath));
    }

    @Value.Parameter
    abstract int[] path();

    public final List<NestedField> resolve(Schema schema) throws SchemaHelper.PathException {
        return SchemaHelper.fieldPath(schema, path());
    }
}
