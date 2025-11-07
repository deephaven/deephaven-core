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
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Type.TypeID;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;

public final class TypeCompatibility {

    // we may want to offer separate reading / writing checks if that is important in the future
    /**
     * Check if the given types are compatible. This is a general check, and specific reading or writing paths may
     * enforce additional restrictions.
     *
     * @param type the Deephaven type
     * @param icebergType the Iceberg type
     * @return {@code true} if the types are compatible, {@code false} otherwise
     */
    public static boolean isCompatible(Type<?> type, org.apache.iceberg.types.Type icebergType) {
        if (icebergType.isPrimitiveType()) {
            return PrimitiveCompat.of(type, icebergType.asPrimitiveType());
        } else if (icebergType.isListType()) {
            return ListCompat.of(type, icebergType.asListType());
        }
        return false;
    }

    private static final class PrimitiveCompat
            implements Type.Visitor<Boolean>, PrimitiveType.Visitor<Boolean>, GenericType.Visitor<Boolean> {

        public static boolean of(Type<?> type, org.apache.iceberg.types.Type.PrimitiveType icebergType) {
            return type.walk(new PrimitiveCompat(icebergType));
        }

        // Note: "primitive" for Iceberg is not equivalent to Java primitives.
        // It means it's not a "complex" type (struct, list, map).
        private final org.apache.iceberg.types.Type.PrimitiveType pt;

        private PrimitiveCompat(org.apache.iceberg.types.Type.PrimitiveType pt) {
            this.pt = Objects.requireNonNull(pt);
        }

        @Override
        public Boolean visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Boolean>) this);
        }

        @Override
        public Boolean visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Boolean>) this);
        }

        @Override
        public Boolean visit(BoxedType<?> boxedType) {
            // same rules as primitives
            return boxedType.primitiveType().walk((PrimitiveType.Visitor<Boolean>) this);
        }

        @Override
        public Boolean visit(StringType stringType) {
            return pt == Types.StringType.get();
        }

        @Override
        public Boolean visit(InstantType instantType) {
            return pt == Types.TimestampType.withZone();
        }

        @Override
        public Boolean visit(ArrayType<?, ?> arrayType) {
            if (pt.typeId() == TypeID.BINARY || pt.typeId() == TypeID.FIXED) {
                return byte.class.equals(arrayType.componentType().clazz());
            }
            return false;
        }

        @Override
        public Boolean visit(CustomType<?> customType) {
            final Class<?> clazz = customType.clazz();
            if (pt == Types.TimestampType.withoutZone()) {
                return LocalDateTime.class.equals(clazz);
            }
            if (pt == Types.DateType.get()) {
                return LocalDate.class.equals(clazz);
            }
            if (pt == Types.TimeType.get()) {
                return LocalTime.class.equals(clazz);
            }
            if (pt instanceof Types.DecimalType) {
                return BigDecimal.class.equals(clazz);
            }
            return false;
        }

        @Override
        public Boolean visit(BooleanType booleanType) {
            return pt == Types.BooleanType.get();
        }

        @Override
        public Boolean visit(ByteType byteType) {
            return isIntegral();
        }

        @Override
        public Boolean visit(CharType charType) {
            return isIntegral();
        }

        @Override
        public Boolean visit(ShortType shortType) {
            return isIntegral();
        }

        @Override
        public Boolean visit(IntType intType) {
            return isIntegral();
        }

        @Override
        public Boolean visit(LongType longType) {
            return isIntegral();
        }

        @Override
        public Boolean visit(FloatType floatType) {
            return isNumeric();
        }

        @Override
        public Boolean visit(DoubleType doubleType) {
            return isNumeric();
        }

        private boolean isNumeric() {
            return pt == Types.IntegerType.get()
                    || pt == Types.LongType.get()
                    || pt == Types.FloatType.get()
                    || pt == Types.DoubleType.get();
        }

        private boolean isIntegral() {
            return pt == Types.IntegerType.get()
                    || pt == Types.LongType.get();
        }
    }

    private static final class ListCompat implements Type.Visitor<Boolean>, GenericType.Visitor<Boolean> {

        private final org.apache.iceberg.types.Type.PrimitiveType elementType;

        public static boolean of(Type<?> type, Types.ListType listType) {
            final org.apache.iceberg.types.Type elementType = listType.elementType();
            if (elementType == null) {
                throw new IllegalArgumentException("ListType must have an element type");
            }
            if (!elementType.isPrimitiveType()) {
                // Not supported
                return false;
            }
            return type.walk(new ListCompat(elementType.asPrimitiveType()));
        }

        private ListCompat(@NotNull final org.apache.iceberg.types.Type.PrimitiveType elementType) {
            this.elementType = elementType;
        }

        @Override
        public Boolean visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Boolean>) this);
        }

        @Override
        public Boolean visit(ArrayType<?, ?> arrayType) {
            // This will cover ArrayType (both native and Vector) for all primitive (/boxed primitive) types.
            // For example, list<int> is compatible with int[], IntVector and ObjectVector<Integer>
            // Further constraints might be enforced by the actual reading/writing code
            final Type<?> componentType = arrayType.componentType();
            return componentType.walk(new PrimitiveCompat(elementType));
        }

        @Override
        public Boolean visit(BoxedType<?> boxedType) {
            return false;
        }

        @Override
        public Boolean visit(StringType stringType) {
            return false;
        }

        @Override
        public Boolean visit(InstantType instantType) {
            return false;
        }

        @Override
        public Boolean visit(CustomType<?> customType) {
            return false;
        }

        @Override
        public Boolean visit(PrimitiveType<?> primitiveType) {
            return false;
        }
    }
}
