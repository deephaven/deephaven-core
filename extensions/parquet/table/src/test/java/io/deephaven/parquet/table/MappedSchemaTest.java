//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.table.ColumnDefinition;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MappedSchemaTest {

    private static final String FOO = "Foo";

    @Test
    public void intType() {
        PrimitiveType expected = Types.optional(PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(32))
                .named(FOO);
        assertThat(type(ColumnDefinition.ofInt(FOO))).isEqualTo(expected);
    }

    @Test
    public void stringType() {
        PrimitiveType expected = Types.optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(FOO);
        assertThat(type(ColumnDefinition.ofString(FOO))).isEqualTo(expected);
    }

    @Test
    public void intListType() {
        GroupType expected = Types.optionalList()
                .optionalElement(PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(32))
                .named(FOO);
        assertThat(type(ColumnDefinition.fromGenericType(FOO, int[].class, int.class))).isEqualTo(expected);
    }

    @Test
    public void stringListType() {
        GroupType expected = Types.optionalList()
                .optionalElement(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(FOO);
        assertThat(type(ColumnDefinition.fromGenericType(FOO, String[].class, String.class))).isEqualTo(expected);
    }

    private static Type type(ColumnDefinition<?> columnDefinition) {
        return MappedSchema.createType(null, columnDefinition, null, null, ParquetInstructions.EMPTY);
    }
}
