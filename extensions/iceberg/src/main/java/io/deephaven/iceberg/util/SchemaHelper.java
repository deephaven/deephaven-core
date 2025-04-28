//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

final class SchemaHelper {

    public static boolean hasFieldPath(Schema schema, int[] idPath) {
        try {
            fieldPath(schema, idPath);
        } catch (PathException e) {
            return false;
        }
        return true;
    }

    public static List<NestedField> fieldPath(Schema schema, int fieldId) throws PathException {
        final FieldPath fieldPath = FieldPath.get(schema, fieldId);
        try {
            return fieldPath.resolve(schema);
        } catch (PathException e) {
            // this should have failed during the get if it was not found
            throw new IllegalStateException(e);
        }
    }

    public static List<NestedField> fieldPath(Schema schema, int[] idPath) throws PathException {
        return path(schema.asStruct(), idPath);
    }

    public static List<NestedField> fieldPath(Schema schema, String[] namePath) throws PathException {
        return path(schema.asStruct(), namePath);
    }

    public static List<NestedField> fieldPath(Schema schema, PartitionField partitionField) throws PathException {
        final FieldPath fieldPath = FieldPath.get(schema, partitionField);
        try {
            return fieldPath.resolve(schema);
        } catch (PathException e) {
            // this should have failed during the get if it was not found
            throw new IllegalStateException(e);
        }
    }

    public static String toFieldName(Collection<? extends NestedField> context) {
        return context
                .stream()
                .map(NestedField::name)
                .collect(Collectors.joining("."));
    }

    private static List<NestedField> path(final Type.NestedType type, final int[] idPath) throws PathException {
        Type currentType = type;
        final List<NestedField> out = new ArrayList<>(idPath.length);
        for (final int fieldId : idPath) {
            if (!currentType.isNestedType()) {
                throw idPathTooLong(idPath, out);
            }
            final NestedField field = currentType.asNestedType().field(fieldId);
            if (field == null) {
                throw idPathNotFound(idPath, out);
            }
            out.add(field);
            currentType = field.type();
        }
        return out;
    }

    private static List<NestedField> path(final Type.NestedType type, final String[] namePath) throws PathException {
        Type currentType = type;
        final List<NestedField> out = new ArrayList<>(namePath.length);
        for (final String name : namePath) {
            if (!currentType.isNestedType()) {
                throw namePathTooLong(namePath, out);
            }
            final NestedField field = fieldByName(currentType.asNestedType(), name);
            if (field == null) {
                throw namePathNotFound(namePath, out);
            }
            out.add(field);
            currentType = field.type();
        }
        return out;
    }

    private static NestedField fieldByName(final Type.NestedType type, final String name) {
        // Iceberg itself does not provide this directly. They provide a _related_ method type.fieldType(String), and
        // it makes sense they don't provide the actual fieldByName because for Map/List, it's somewhat of an
        // implementation detail and callers should really be using ids.
        for (NestedField field : type.fields()) {
            if (name.equals(field.name())) {
                return field;
            }
        }
        return null;
    }

    public static class PathException extends Exception {

        public PathException(String message) {
            super(message);
        }
    }

    private static PathException idPathNotFound(int[] idPath, List<NestedField> context) {
        return new PathException(
                String.format("id path not found, path=%s, fieldName=`%s`", Arrays.toString(idPath),
                        toFieldName(context)));
    }

    private static PathException idPathTooLong(int[] idPath, List<NestedField> context) {
        return new PathException(
                String.format("id path too long, path=%s, fieldName=`%s`", Arrays.toString(idPath),
                        toFieldName(context)));
    }

    private static PathException namePathNotFound(String[] namePath, List<NestedField> context) {
        return new PathException(
                String.format("name path not found, path=%s, fieldName=`%s`", Arrays.toString(namePath),
                        toFieldName(context)));
    }

    private static PathException namePathTooLong(String[] namePath, List<NestedField> context) {
        return new PathException(
                String.format("name path too long, path=%s, fieldName=`%s`", Arrays.toString(namePath),
                        toFieldName(context)));
    }
}
