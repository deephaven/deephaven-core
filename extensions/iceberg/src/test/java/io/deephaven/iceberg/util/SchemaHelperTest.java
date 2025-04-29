//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

class SchemaHelperTest {

    @Test
    void fieldPathById() throws SchemaHelper.PathException {
        final NestedField i1 = NestedField.of(1, true, "I1", IntegerType.get());
        final NestedField i2 = NestedField.of(2, true, "I2", IntegerType.get());
        final NestedField i3 = NestedField.of(3, true, "I3", IntegerType.get());
        final NestedField i4 = NestedField.of(4, true, "I4", StructType.of(i2, i3));
        final NestedField i6 = NestedField.of(6, true, "I6", ListType.ofOptional(5, IntegerType.get()));
        final NestedField i9 =
                NestedField.of(9, true, "I9", MapType.ofOptional(7, 8, IntegerType.get(), IntegerType.get()));
        final Schema schema = new Schema(i1, i4, i6, i9);

        assertFieldPath(schema, new int[0]).isEmpty();

        assertFieldPath(schema, 1).containsExactly("I1");
        assertFieldPathError("id path too long, path=[1, 2], fieldName=`I1`", schema, 1, 2);

        assertFieldPath(schema, 4, 2).containsExactly("I4", "I2");
        assertFieldPath(schema, 4, 3).containsExactly("I4", "I3");
        assertFieldPathError("id path not found, path=[4, 1], fieldName=`I4`", schema, 4, 1);
        assertFieldPathError("id path too long, path=[4, 2, 1], fieldName=`I4.I2`", schema, 4, 2, 1);

        assertFieldPath(schema, 6).containsExactly("I6");
        assertFieldPath(schema, 6, 5).containsExactly("I6", "element");
        assertFieldPathError("id path not found, path=[6, 4], fieldName=`I6`", schema, 6, 4);
        assertFieldPathError("id path too long, path=[6, 5, 4], fieldName=`I6.element`", schema, 6, 5, 4);

        assertFieldPath(schema, 9).containsExactly("I9");
        assertFieldPath(schema, 9, 7).containsExactly("I9", "key");
        assertFieldPath(schema, 9, 8).containsExactly("I9", "value");
        assertFieldPathError("id path not found, path=[9, 6], fieldName=`I9`", schema, 9, 6);
        assertFieldPathError("id path too long, path=[9, 7, 6], fieldName=`I9.key`", schema, 9, 7, 6);
        assertFieldPathError("id path too long, path=[9, 8, 6], fieldName=`I9.value`", schema, 9, 8, 6);
    }

    @Test
    void fieldPathByName() throws SchemaHelper.PathException {
        final NestedField i1 = NestedField.of(1, true, "I1", IntegerType.get());
        final NestedField i2 = NestedField.of(2, true, "I2", IntegerType.get());
        final NestedField i3 = NestedField.of(3, true, "I3", IntegerType.get());
        final NestedField i4 = NestedField.of(4, true, "I4", StructType.of(i2, i3));
        final NestedField i6 = NestedField.of(6, true, "I6", ListType.ofOptional(5, IntegerType.get()));
        final NestedField i9 =
                NestedField.of(9, true, "I9", MapType.ofOptional(7, 8, IntegerType.get(), IntegerType.get()));
        final Schema schema = new Schema(i1, i4, i6, i9);

        assertFieldPath(schema, new String[0]).isEmpty();

        assertFieldPath(schema, "I1").containsExactly("I1");
        assertFieldPathError("name path too long, path=[I1, I2], fieldName=`I1`", schema, "I1", "I2");

        assertFieldPath(schema, "I4", "I2").containsExactly("I4", "I2");
        assertFieldPath(schema, "I4", "I3").containsExactly("I4", "I3");
        assertFieldPathError("name path not found, path=[I4, I1], fieldName=`I4`", schema, "I4", "I1");
        assertFieldPathError("name path too long, path=[I4, I2, I1], fieldName=`I4.I2`", schema, "I4", "I2", "I1");

        assertFieldPath(schema, "I6").containsExactly("I6");
        assertFieldPath(schema, "I6", "element").containsExactly("I6", "element");
        assertFieldPathError("name path not found, path=[I6, dne], fieldName=`I6`", schema, "I6", "dne");
        assertFieldPathError("name path too long, path=[I6, element, dne], fieldName=`I6.element`", schema, "I6",
                "element", "dne");

        assertFieldPath(schema, "I9").containsExactly("I9");
        assertFieldPath(schema, "I9", "key").containsExactly("I9", "key");
        assertFieldPath(schema, "I9", "value").containsExactly("I9", "value");
        assertFieldPathError("name path not found, path=[I9, dne], fieldName=`I9`", schema, "I9", "dne");
        assertFieldPathError("name path too long, path=[I9, key, dne], fieldName=`I9.key`", schema, "I9", "key",
                "dne");
        assertFieldPathError("name path too long, path=[I9, value, dne], fieldName=`I9.value`", schema, "I9",
                "value", "dne");
    }

    private static AbstractListAssert<?, List<? extends String>, String, ObjectAssert<String>> assertFieldPath(
            Schema schema, int... values) throws SchemaHelper.PathException {
        return assertThat(SchemaHelper.fieldPath(schema, values)).extracting(NestedField::name);
    }

    private static AbstractListAssert<?, List<? extends String>, String, ObjectAssert<String>> assertFieldPath(
            Schema schema, String... values) throws SchemaHelper.PathException {
        return assertThat(SchemaHelper.fieldPath(schema, values)).extracting(NestedField::name);
    }

    private static void assertFieldPathError(String message, Schema schema, int... values) {
        try {
            SchemaHelper.fieldPath(schema, values).forEach(x -> {
            });
            failBecauseExceptionWasNotThrown(SchemaHelper.PathException.class);
        } catch (SchemaHelper.PathException e) {
            assertThat(e).hasMessageContaining(message);
        }
    }

    private static void assertFieldPathError(String message, Schema schema, String... values) {
        try {
            SchemaHelper.fieldPath(schema, values).forEach(x -> {
            });
            failBecauseExceptionWasNotThrown(SchemaHelper.PathException.class);
        } catch (SchemaHelper.PathException e) {
            assertThat(e).hasMessageContaining(message);
        }
    }
}
