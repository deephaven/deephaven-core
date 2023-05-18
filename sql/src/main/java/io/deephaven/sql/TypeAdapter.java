/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.sql;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Objects;

final class TypeAdapter implements Type.Visitor, GenericType.Visitor, PrimitiveType.Visitor {

    public static RelDataType of(Type<?> type, RelDataTypeFactory typeFactory) {
        final TypeAdapter adapter = new TypeAdapter(typeFactory);
        type.walk(adapter);
        return Objects.requireNonNull(adapter.out);
    }

    public static RelDataType of(TableHeader tableHeader, RelDataTypeFactory typeFactory) {
        final Builder builder = new Builder(typeFactory);
        for (ColumnHeader<?> columnHeader : tableHeader) {
            builder.add(columnHeader.name(), of(columnHeader.componentType(), typeFactory));
        }
        return builder.build();
    }

    private final RelDataTypeFactory typeFactory;
    private RelDataType out;

    private TypeAdapter(RelDataTypeFactory typeFactory) {
        this.typeFactory = Objects.requireNonNull(typeFactory);
    }

    @Override
    public void visit(PrimitiveType<?> primitiveType) {
        primitiveType.walk((PrimitiveType.Visitor) this);
    }

    @Override
    public void visit(BooleanType booleanType) {
        out = create(SqlTypeName.BOOLEAN);
    }

    @Override
    public void visit(ByteType byteType) {
        out = create(SqlTypeName.TINYINT);
    }

    @Override
    public void visit(CharType charType) {
        out = create(SqlTypeName.CHAR);
    }

    @Override
    public void visit(ShortType shortType) {
        out = create(SqlTypeName.SMALLINT);
    }

    @Override
    public void visit(IntType intType) {
        out = create(SqlTypeName.INTEGER);
    }

    @Override
    public void visit(LongType longType) {
        out = create(SqlTypeName.BIGINT);
    }

    @Override
    public void visit(FloatType floatType) {
        out = create(SqlTypeName.FLOAT);
    }

    @Override
    public void visit(DoubleType doubleType) {
        out = create(SqlTypeName.DOUBLE);
    }

    @Override
    public void visit(GenericType<?> genericType) {
        genericType.walk((GenericType.Visitor) this);
    }

    @Override
    public void visit(StringType stringType) {
        out = create(SqlTypeName.VARCHAR);
    }

    @Override
    public void visit(InstantType instantType) {
        out = create(SqlTypeName.TIMESTAMP);
    }

    @Override
    public void visit(ArrayType<?, ?> arrayType) {
        // SQLTODO(array-type)
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CustomType<?> customType) {
        // SQLTODO(custom-type)
        throw new UnsupportedOperationException();
    }

    private RelDataType create(SqlTypeName typeName) {
        return nullable(typeFactory.createSqlType(typeName));
    }

    private RelDataType nullable(RelDataType relDataType) {
        return typeFactory.createTypeWithNullability(relDataType, true);
    }
}
