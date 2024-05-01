//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.TableHeader;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Objects;

final class TypeAdapter
        implements Type.Visitor<RelDataType>, GenericType.Visitor<RelDataType>, PrimitiveType.Visitor<RelDataType> {

    public static RelDataType of(Type<?> type, RelDataTypeFactory typeFactory) {
        return type.walk(new TypeAdapter(typeFactory));
    }

    public static RelDataType of(TableHeader tableHeader, RelDataTypeFactory typeFactory) {
        final Builder builder = new Builder(typeFactory);
        for (ColumnHeader<?> columnHeader : tableHeader) {
            builder.add(columnHeader.name(), of(columnHeader.componentType(), typeFactory));
        }
        return builder.build();
    }

    private final RelDataTypeFactory typeFactory;

    private TypeAdapter(RelDataTypeFactory typeFactory) {
        this.typeFactory = Objects.requireNonNull(typeFactory);
    }

    @Override
    public RelDataType visit(PrimitiveType<?> primitiveType) {
        return primitiveType.walk((PrimitiveType.Visitor<RelDataType>) this);
    }

    @Override
    public RelDataType visit(BooleanType booleanType) {
        return create(SqlTypeName.BOOLEAN);
    }

    @Override
    public RelDataType visit(ByteType byteType) {
        return create(SqlTypeName.TINYINT);
    }

    @Override
    public RelDataType visit(CharType charType) {
        return create(SqlTypeName.CHAR);
    }

    @Override
    public RelDataType visit(ShortType shortType) {
        return create(SqlTypeName.SMALLINT);
    }

    @Override
    public RelDataType visit(IntType intType) {
        return create(SqlTypeName.INTEGER);
    }

    @Override
    public RelDataType visit(LongType longType) {
        return create(SqlTypeName.BIGINT);
    }

    @Override
    public RelDataType visit(FloatType floatType) {
        return create(SqlTypeName.REAL);
    }

    @Override
    public RelDataType visit(DoubleType doubleType) {
        return create(SqlTypeName.DOUBLE);
    }

    @Override
    public RelDataType visit(GenericType<?> genericType) {
        return genericType.walk((GenericType.Visitor<RelDataType>) this);
    }

    @Override
    public RelDataType visit(BoxedType<?> boxedType) {
        return visit(boxedType.primitiveType());
    }

    @Override
    public RelDataType visit(StringType stringType) {
        return create(SqlTypeName.VARCHAR);
    }

    @Override
    public RelDataType visit(InstantType instantType) {
        return create(SqlTypeName.TIMESTAMP);
    }

    @Override
    public RelDataType visit(ArrayType<?, ?> arrayType) {
        // SQLTODO(array-type)
        return typeFactory.createJavaType(SqlTodoArrayType.class);
    }

    @Override
    public RelDataType visit(CustomType<?> customType) {
        // SQLTODO(custom-type)
        return typeFactory.createJavaType(SqlTodoCustomType.class);
    }

    private RelDataType create(SqlTypeName typeName) {
        return nullable(typeFactory.createSqlType(typeName));
    }

    private RelDataType nullable(RelDataType relDataType) {
        return typeFactory.createTypeWithNullability(relDataType, true);
    }

    // We currently need our type parsing to always succeed; right now, we implicitly inherit the user's scope, and if
    // they happen to have a table with column of CustomType or ArrayType, we need to have that not immediately fail.
    // Firstly, the user might not even be referencing that table / column. Secondly, the user might just be passing
    // the column through unchanged, in which case Calcite is happy to create the plan.
    // See https://github.com/deephaven/deephaven-core/issues/5443 for more context.

    static class SqlTodoType {
    }

    static class SqlTodoArrayType extends SqlTodoType {
    }

    static class SqlTodoCustomType extends SqlTodoType {
    }
}
