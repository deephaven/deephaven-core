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
import io.deephaven.qst.type.GenericVectorType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;

final class IcebergPrimitiveCompat
        implements Type.Visitor<Boolean>, PrimitiveType.Visitor<Boolean>, GenericType.Visitor<Boolean> {



    // Note: "primitive" for Iceberg is not equivalent to Java primitives.
    // It means it's not a "complex" type (struct, list, map).
    private final org.apache.iceberg.types.Type.PrimitiveType pt;

    public IcebergPrimitiveCompat(org.apache.iceberg.types.Type.PrimitiveType pt) {
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
        // likely, should follow same rules as primitives?
        return boxedType.primitiveType().walk((PrimitiveType.Visitor<Boolean>) this);
    }

    @Override
    public Boolean visit(StringType stringType) {
        return pt == Types.StringType.get();
    }

    @Override
    public Boolean visit(InstantType instantType) {
        // todo: TZ nanos
        return pt == Types.TimestampType.withZone();
    }

    @Override
    public Boolean visit(ArrayType<?, ?> arrayType) {
        // todo: more advanced array walk later
        return Type.byteType().arrayType().equals(arrayType)
                && (pt == Types.BinaryType.get() || pt instanceof Types.FixedType);
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
        return false;
    }

    @Override
    public Boolean visit(CharType charType) {
        return false;
    }

    @Override
    public Boolean visit(ShortType shortType) {
        // todo: should we allow LongType?
        return isIntegral();
    }

    @Override
    public Boolean visit(IntType intType) {
        // todo: should we allow LongType?
        return isIntegral();
    }

    @Override
    public Boolean visit(LongType longType) {
        // todo: should we allow IntegerType?
        return isIntegral();
    }

    @Override
    public Boolean visit(FloatType floatType) {
        // todo: should we allow DoubleType / integral types?
        return isNumeric();
    }

    @Override
    public Boolean visit(DoubleType doubleType) {
        // todo: should we allow FloatType / integral types?
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
