//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.base.verify.Assert;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;


public final class Inference {

    public interface Consumer {

        void onType(Collection<? extends Types.NestedField> path, Type<?> type);

        void onError(Collection<? extends Types.NestedField> path, Exception e);
    }

    /**
     * Depth-first traversal dictated by the order in the schema.
     */
    public static void of(Schema schema, Consumer consumer) {
        final List<Types.NestedField> prefix = new ArrayList<>();
        nestedType(prefix, schema.asStruct(), consumer);
    }

    public static Type<?> of(org.apache.iceberg.types.Type.PrimitiveType primitiveType) throws UnsupportedType {
        switch (primitiveType.typeId()) {
            case BOOLEAN:
                return of((Types.BooleanType) primitiveType);
            case INTEGER:
                return of((Types.IntegerType) primitiveType);
            case LONG:
                return of((Types.LongType) primitiveType);
            case FLOAT:
                return of((Types.FloatType) primitiveType);
            case DOUBLE:
                return of((Types.DoubleType) primitiveType);
            case DATE:
                return of((Types.DateType) primitiveType);
            case TIME:
                return of((Types.TimeType) primitiveType);
            case TIMESTAMP:
                return of((Types.TimestampType) primitiveType);
            case STRING:
                return of((Types.StringType) primitiveType);
            case BINARY:
                return of((Types.BinaryType) primitiveType);
            case FIXED:
                return of((Types.FixedType) primitiveType);
            case DECIMAL:
                return of((Types.DecimalType) primitiveType);
            case STRUCT:
            case LIST:
            case MAP:
                // Should never get here, we know it's a primitive type
                throw new IllegalStateException();
            // v3, likely the same as TIMESTAMP but should test
            case TIMESTAMP_NANO:
                // should be able to support UUID, maybe fixed length codec?
            case UUID:
            default:
                throw new UnsupportedType(primitiveType);
        }
    }

    static Type<?> of(@SuppressWarnings("unused") Types.BooleanType type) {
        return Type.booleanType().boxedType();
    }

    static Type<?> of(@SuppressWarnings("unused") Types.IntegerType type) {
        return Type.intType();
    }

    static Type<?> of(@SuppressWarnings("unused") Types.LongType type) {
        return Type.longType();
    }

    static Type<?> of(@SuppressWarnings("unused") Types.FloatType type) {
        return Type.floatType();
    }

    static Type<?> of(@SuppressWarnings("unused") Types.DoubleType type) {
        return Type.doubleType();
    }

    static Type<?> of(@SuppressWarnings("unused") Types.DateType type) {
        return Type.find(LocalDate.class);
    }

    static Type<?> of(@SuppressWarnings("unused") Types.TimeType type) {
        return Type.find(LocalTime.class);
    }

    static Type<?> of(Types.TimestampType type) {
        return type == Types.TimestampType.withZone()
                ? Type.instantType()
                : Type.find(LocalDateTime.class);
    }

    static Type<?> of(@SuppressWarnings("unused") Types.StringType type) {
        return Type.stringType();
    }

    static Type<?> of(@SuppressWarnings("unused") Types.BinaryType type) {
        return Type.byteType().arrayType();
    }

    static Type<?> of(@SuppressWarnings("unused") Types.FixedType type) {
        // TODO: should we have QST type that captures len?
        return Type.byteType().arrayType();
    }

    static Type<?> of(@SuppressWarnings("unused") Types.DecimalType type) {
        // todo: should we have QST type that captures scale / precision?
        return Type.find(BigDecimal.class);
    }

    private static void structField(
            List<Types.NestedField> prefix,
            Types.NestedField structField,
            Consumer consumer) {
        push(prefix, structField);
        try {
            nestedType(prefix, structField.type().asStructType(), consumer);
        } finally {
            pop(prefix, structField);
        }
    }

    private static void nestedType(
            List<Types.NestedField> path,
            org.apache.iceberg.types.Type.NestedType nestedType,
            Consumer consumer) {
        for (final Types.NestedField field : nestedType.fields()) {
            if (field.type().isPrimitiveType()) {
                primitiveField(path, field, consumer);
            } else if (field.type().isStructType()) {
                structField(path, field, consumer);
            } else {
                // map, list
                consumer.onError(path, new UnsupportedType(field.type()));
            }
        }
    }

    private static void primitiveField(
            List<Types.NestedField> prefix,
            Types.NestedField primitiveField,
            Consumer consumer) {
        push(prefix, primitiveField);
        try {
            primitiveType(prefix, primitiveField.type().asPrimitiveType(), consumer);
        } finally {
            pop(prefix, primitiveField);
        }
    }

    private static void primitiveType(
            List<Types.NestedField> path,
            org.apache.iceberg.types.Type.PrimitiveType primitiveType,
            Consumer consumer) {
        final Type<?> type;
        try {
            type = of(primitiveType);
        } catch (UnsupportedType e) {
            consumer.onError(path, e);
            return;
        }
        consumer.onType(path, type);
    }

    private static <T> void push(List<T> stack, T item) {
        stack.add(item);
    }

    private static <T> void pop(List<T> stack, T item) {
        Assert.eq(item, "item", stack.remove(stack.size() - 1));
    }

    public static abstract class Exception extends java.lang.Exception {
        public Exception() {}

        public Exception(String message) {
            super(message);
        }
    }

    public static final class UnsupportedType extends Exception {
        private final org.apache.iceberg.types.Type type;

        public UnsupportedType(org.apache.iceberg.types.Type type) {
            // type is more informative than type.typeId()
            super(String.format("Unsupported Iceberg type: `%s`", type));
            this.type = Objects.requireNonNull(type);
        }

        public org.apache.iceberg.types.Type type() {
            return type;
        }
    }
}
