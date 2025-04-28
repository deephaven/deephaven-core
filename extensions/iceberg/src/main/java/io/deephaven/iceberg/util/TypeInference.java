//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public final class TypeInference {

    /**
     * Infers the "best" Deephaven type from the {@code icebergType}. If a value is returned, it is guaranteed to be
     * {@link TypeCompatibility#isCompatible(Type, org.apache.iceberg.types.Type) compatible} with {@code icebergType}.
     *
     * <p>
     * An empty return does not necessarily mean the {@code icebergType} has no Deephaven type that it can work with,
     * just that no "best" Deephaven type was inferred.
     *
     * @param icebergType the Iceberg type
     * @return the Deephaven type
     */
    public static Optional<Type<?>> of(org.apache.iceberg.types.Type icebergType) {
        if (!icebergType.isPrimitiveType()) {
            return Optional.empty();
        }
        // We may support List here in the future, or other.
        final Type<?> type = ofImpl(icebergType.asPrimitiveType());
        if (type == null) {
            return Optional.empty();
        }
        if (!TypeCompatibility.isCompatible(type, icebergType)) {
            throw new IllegalStateException(
                    String.format("Inference is inconsistent with stated type compatibilities, type=%s, icebergType=%s",
                            type, icebergType));
        }
        return Optional.of(type);
    }

    /**
     * Infers the "best" Iceberg type from the Deephaven {@code type}. If a value is returned, it is guaranteed to be
     * {@link TypeCompatibility#isCompatible(Type, org.apache.iceberg.types.Type) compatible} with {@code type}.
     *
     * <p>
     * An empty return does not necessarily mean the Deephaven {@code type} has no Iceberg type that it can work with.
     * For example, a {@link BigDecimal} Deephaven type may not infer a "best" {@link Types.DecimalType}, but it's
     * possible that a specific {@link Types.DecimalType} is compatible with a {@link BigDecimal} Deephaven type.
     *
     * @param type the Deephaven type
     * @param nextId the next id to use for created types
     * @return the Iceberg type
     */
    public static Optional<org.apache.iceberg.types.Type> of(
            @NotNull final Type<?> type,
            @NotNull final TypeUtil.NextID nextId) {
        Objects.requireNonNull(nextId);
        org.apache.iceberg.types.Type icebergType = type.walk(BestIcebergType.INSTANCE);
        if (icebergType == null) {
            return Optional.empty();
        }
        if (!TypeCompatibility.isCompatible(type, icebergType)) {
            throw new IllegalStateException(
                    String.format("Inference is inconsistent with stated type compatibilities, type=%s, icebergType=%s",
                            type, icebergType));
        }
        return Optional.of(icebergType);
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
            case BINARY:
            case FIXED:
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

    static Type<?> of(@SuppressWarnings("unused") Types.DecimalType type) {
        // We may want to have DH type that captures precision and scale
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

    private enum BestIcebergType implements
            Type.Visitor<org.apache.iceberg.types.Type>,
            PrimitiveType.Visitor<org.apache.iceberg.types.Type>,
            GenericType.Visitor<org.apache.iceberg.types.Type> {
        INSTANCE;

        @Override
        public org.apache.iceberg.types.Type visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<org.apache.iceberg.types.Type>) this);
        }

        @Override
        public org.apache.iceberg.types.Type visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<org.apache.iceberg.types.Type>) this);
        }


        @Override
        public org.apache.iceberg.types.Type visit(BoxedType<?> boxedType) {
            // handle boxed types the same as primitive
            return boxedType.primitiveType().walk((PrimitiveType.Visitor<org.apache.iceberg.types.Type>) this);
        }

        @Override
        public org.apache.iceberg.types.Type visit(StringType stringType) {
            return Types.StringType.get();
        }

        @Override
        public org.apache.iceberg.types.Type visit(InstantType instantType) {
            return Types.TimestampType.withZone();
        }

        @Override
        public org.apache.iceberg.types.Type visit(ArrayType<?, ?> arrayType) {
            return null;
        }

        @Override
        public org.apache.iceberg.types.Type visit(CustomType<?> customType) {
            if (LocalDateTime.class.equals(customType.clazz())) {
                return Types.TimestampType.withoutZone();
            }
            if (LocalDate.class.equals(customType.clazz())) {
                return Types.DateType.get();
            }
            if (LocalTime.class.equals(customType.clazz())) {
                return Types.TimeType.get();
            }
            return null;
        }

        @Override
        public org.apache.iceberg.types.Type visit(BooleanType booleanType) {
            return Types.BooleanType.get();
        }

        @Override
        public org.apache.iceberg.types.Type visit(IntType intType) {
            return Types.IntegerType.get();
        }

        @Override
        public org.apache.iceberg.types.Type visit(LongType longType) {
            return Types.LongType.get();
        }

        @Override
        public org.apache.iceberg.types.Type visit(FloatType floatType) {
            return Types.FloatType.get();
        }

        @Override
        public org.apache.iceberg.types.Type visit(DoubleType doubleType) {
            return Types.DoubleType.get();
        }

        @Override
        public org.apache.iceberg.types.Type visit(ByteType byteType) {
            // TODO(DH-18253): Add support to write more types to iceberg tables
            return null;
        }

        @Override
        public org.apache.iceberg.types.Type visit(CharType charType) {
            // TODO(DH-18253): Add support to write more types to iceberg tables
            return null;
        }

        @Override
        public org.apache.iceberg.types.Type visit(ShortType shortType) {
            // TODO(DH-18253): Add support to write more types to iceberg tables
            return null;
        }
    }
}
