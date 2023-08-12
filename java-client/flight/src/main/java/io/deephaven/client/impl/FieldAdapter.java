/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericType.Visitor;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Collections;
import java.util.Objects;

/**
 * Utilities for creating a {@link Field}.
 */
public class FieldAdapter implements Type.Visitor<Field>, PrimitiveType.Visitor<Field> {

    /**
     * Convert a {@code header} into a {@link Field}.
     *
     * @param header the header
     * @return the field
     */
    public static Field of(ColumnHeader<?> header) {
        return header.componentType().walk(new FieldAdapter(header.name()));
    }

    public static Field byteField(String name) {
        return field(name, MinorType.TINYINT.getType(), "byte");
    }

    public static Field booleanField(String name) {
        return field(name, MinorType.BIT.getType(), "boolean");
    }

    public static Field charField(String name) {
        return field(name, MinorType.UINT2.getType(), "char");
    }

    public static Field shortField(String name) {
        return field(name, MinorType.SMALLINT.getType(), "short");
    }

    public static Field intField(String name) {
        return field(name, MinorType.INT.getType(), "int");
    }

    public static Field longField(String name) {
        return field(name, MinorType.BIGINT.getType(), "long");
    }

    public static Field floatField(String name) {
        return field(name, MinorType.FLOAT4.getType(), "float");
    }

    public static Field doubleField(String name) {
        return field(name, MinorType.FLOAT8.getType(), "double");
    }

    public static Field stringField(String name) {
        return field(name, MinorType.VARCHAR.getType(), "java.lang.String");
    }

    public static Field instantField(String name) {
        return field(name, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), "java.time.Instant");
    }

    private static Field field(String name, ArrowType arrowType, String deephavenType) {
        return field(name,
                new FieldType(true, arrowType, null, Collections.singletonMap("deephaven:type", deephavenType)));
    }

    private static Field field(String name, FieldType type) {
        return new Field(name, type, null);
    }

    private final String name;

    private FieldAdapter(String name) {
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public Field visit(PrimitiveType<?> primitive) {
        return primitive.walk((PrimitiveType.Visitor<Field>) this);
    }

    @Override
    public Field visit(GenericType<?> generic) {
        return generic.walk(new Visitor<Field>() {
            @Override
            public Field visit(BoxedType<?> boxedType) {
                return FieldAdapter.this.visit(boxedType.primitiveType());
            }

            @Override
            public Field visit(StringType stringType) {
                return stringField(name);
            }

            @Override
            public Field visit(InstantType instantType) {
                return instantField(name);
            }

            @Override
            public Field visit(ArrayType<?, ?> arrayType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Field visit(CustomType<?> customType) {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Override
    public Field visit(ByteType byteType) {
        return byteField(name);
    }

    @Override
    public Field visit(BooleanType booleanType) {
        return booleanField(name);
    }

    @Override
    public Field visit(CharType charType) {
        return charField(name);
    }

    @Override
    public Field visit(ShortType shortType) {
        return shortField(name);
    }

    @Override
    public Field visit(IntType intType) {
        return intField(name);
    }

    @Override
    public Field visit(LongType longType) {
        return longField(name);
    }

    @Override
    public Field visit(FloatType floatType) {
        return floatField(name);
    }

    @Override
    public Field visit(DoubleType doubleType) {
        return doubleField(name);
    }
}
