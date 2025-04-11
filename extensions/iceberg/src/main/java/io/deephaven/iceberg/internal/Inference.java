//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.qst.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public final class Inference {

    public static Optional<Type<?>> of(org.apache.iceberg.types.Type.PrimitiveType primitiveType) {
        return Optional.ofNullable(ofImpl(primitiveType));
    }

    private static Type<?> ofImpl(org.apache.iceberg.types.Type.PrimitiveType primitiveType) {
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
                return null;
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

    public static abstract class Exception extends java.lang.Exception {
        public Exception() {}

        public Exception(String message) {
            super(message);
        }
    }

    public static final class UnsupportedType extends Exception {
        private final Schema schema;
        private final List<Types.NestedField> fieldPath;

        public UnsupportedType(Schema schema, List<Types.NestedField> fieldPath) {
            super(String.format("Unsupported Iceberg type `%s` at fieldName `%s`",
                    fieldPath.get(fieldPath.size() - 1).type(),
                    SchemaHelper.toFieldName(fieldPath)));
            this.schema = Objects.requireNonNull(schema);
            this.fieldPath = List.copyOf(fieldPath);
        }

        public Schema schema() {
            return schema;
        }

        public List<Types.NestedField> fieldPath() {
            return fieldPath;
        }

        public org.apache.iceberg.types.Type type() {
            return fieldPath.get(fieldPath.size() - 1).type();
        }
    }
}
