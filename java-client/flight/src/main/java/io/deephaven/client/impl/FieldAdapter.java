package io.deephaven.client.impl;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
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
public class FieldAdapter implements Type.Visitor, PrimitiveType.Visitor {

    /**
     * Convert a {@code header} into a {@link Field}.
     *
     * @param header the header
     * @return the field
     */
    public static Field of(ColumnHeader<?> header) {
        return header.componentType().walk(new FieldAdapter(header.name())).out();
    }

    public static Field byteField(String name) {
        return field(name, MinorType.TINYINT.getType(), "byte");
    }

    public static Field booleanField(String name) {
        // TODO(deephaven-core#43): Do not reinterpret bool as byte
        return field(name, MinorType.TINYINT.getType(), "boolean");
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
        return field(name, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"),
                "io.deephaven.time.DateTime");
    }

    private static Field field(String name, ArrowType arrowType, String deephavenType) {
        return field(name,
                new FieldType(true, arrowType, null, Collections.singletonMap("deephaven:type", deephavenType)));
    }

    private static Field field(String name, FieldType type) {
        return new Field(name, type, null);
    }

    private final String name;

    private Field out;

    private FieldAdapter(String name) {
        this.name = Objects.requireNonNull(name);
    }

    Field out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(PrimitiveType<?> primitive) {
        primitive.walk((PrimitiveType.Visitor) this);
    }

    @Override
    public void visit(GenericType<?> generic) {
        generic.walk(new Visitor() {
            @Override
            public void visit(StringType stringType) {
                out = stringField(name);
            }

            @Override
            public void visit(InstantType instantType) {
                out = instantField(name);
            }

            @Override
            public void visit(ArrayType<?, ?> arrayType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(CustomType<?> customType) {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Override
    public void visit(ByteType byteType) {
        out = byteField(name);
    }

    @Override
    public void visit(BooleanType booleanType) {
        out = booleanField(name);
    }

    @Override
    public void visit(CharType charType) {
        out = charField(name);
    }

    @Override
    public void visit(ShortType shortType) {
        out = shortField(name);
    }

    @Override
    public void visit(IntType intType) {
        out = intField(name);
    }

    @Override
    public void visit(LongType longType) {
        out = longField(name);
    }

    @Override
    public void visit(FloatType floatType) {
        out = floatField(name);
    }

    @Override
    public void visit(DoubleType doubleType) {
        out = doubleField(name);
    }
}
