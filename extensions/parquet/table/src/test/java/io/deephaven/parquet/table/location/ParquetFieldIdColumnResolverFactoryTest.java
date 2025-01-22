//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ParquetFieldIdColumnResolverFactoryTest {

    // foo (42)
    private static final PrimitiveType FOO_42 = Types.required(INT32)
            .id(42)
            .named("foo");

    // bar (43), list, element
    private static final GroupType BAR_43 = Types.requiredList()
            .id(43)
            .requiredElement(INT32)
            .named("bar");

    // baz, list, element (44)
    private static final GroupType BAZ_44 = Types.requiredList()
            .requiredElement(INT32)
            .id(44)
            .named("baz");

    private static final ParquetFieldIdColumnResolverFactory FACTORY = ParquetFieldIdColumnResolverFactory.of(Map.of(
            "DeepFoo", 42,
            "DeepBar", 43,
            "DeepBaz", 44));

    private static String[] p(String... path) {
        return path;
    }

    @Test
    public void messageFields() {
        final MessageType schema = Types.buildMessage()
                .addFields(FOO_42, BAR_43, BAZ_44)
                .named("root");
        final ParquetColumnResolverMap expected = ParquetColumnResolverMap.builder()
                .putMap("DeepFoo", List.of("foo"))
                .putMap("DeepBar", List.of("bar", "list", "element"))
                .putMap("DeepBaz", List.of("baz", "list", "element"))
                .build();
        assertThat(FACTORY.of(schema)).isEqualTo(expected);
    }

    @Test
    public void messageGroupFields() {
        final MessageType schema = Types.buildMessage()
                .addFields(Types.repeatedGroup()
                        .addFields(FOO_42, BAR_43, BAZ_44)
                        .named("my_group"))
                .named("root");
        final ParquetColumnResolverMap expected = ParquetColumnResolverMap.builder()
                .putMap("DeepFoo", List.of("my_group", "foo"))
                .putMap("DeepBar", List.of("my_group", "bar", "list", "element"))
                .putMap("DeepBaz", List.of("my_group", "baz", "list", "element"))
                .build();
        assertThat(FACTORY.of(schema)).isEqualTo(expected);
    }

    @Test
    public void messageListElements() {
        final MessageType schema = Types.buildMessage()
                .addFields(
                        Types.requiredList().element(FOO_42).named("my_list1"),
                        Types.requiredList().element(BAR_43).named("my_list2"),
                        Types.requiredList().element(BAZ_44).named("my_list3"))
                .named("root");
        final ParquetColumnResolverMap expected = ParquetColumnResolverMap.builder()
                .putMap("DeepFoo", List.of("my_list1", "list", "foo"))
                .putMap("DeepBar", List.of("my_list2", "list", "bar", "list", "element"))
                .putMap("DeepBaz", List.of("my_list3", "list", "baz", "list", "element"))
                .build();
        assertThat(FACTORY.of(schema)).isEqualTo(expected);
    }

    @Test
    public void singleFieldMultipleIdsUnambiguous() {
        final ParquetFieldIdColumnResolverFactory factory = ParquetFieldIdColumnResolverFactory.of(Map.of(
                "Col1", 1,
                "Col2", 2));

        // BothCols (1), list, element (2)
        final MessageType schema = Types.buildMessage().addFields(Types.requiredList()
                .id(1)
                .requiredElement(INT32)
                .id(2)
                .named("BothCols"))
                .named("root");

        final ParquetColumnResolverMap expected = ParquetColumnResolverMap.builder()
                .putMap("Col1", List.of("BothCols", "list", "element"))
                .putMap("Col2", List.of("BothCols", "list", "element"))
                .build();

        assertThat(factory.of(schema)).isEqualTo(expected);
    }

    @Test
    public void singleFieldRepeatedIds() {
        final ParquetFieldIdColumnResolverFactory factory = ParquetFieldIdColumnResolverFactory.of(Map.of("Col1", 42));
        // X (42), list, element (42)
        final MessageType schema = Types.buildMessage().addFields(Types.requiredList()
                .id(42)
                .requiredElement(INT32)
                .id(42)
                .named("X"))
                .named("root");

        final ParquetColumnResolverMap expected = ParquetColumnResolverMap.builder()
                .putMap("Col1", List.of("X", "list", "element"))
                .build();

        // This resolution strategy is a little bit questionable... but it is unambiguous. If instead we needed to also
        // provide the user with a single resulting Type with said field id, this strategy would not work.
        assertThat(factory.of(schema)).isEqualTo(expected);
    }

    @Test
    public void ambiguousFields() {
        // X (1)
        // Y (1)
        // Z (2)
        final MessageType schema = Types.buildMessage()
                .addFields(
                        Types.required(INT32).id(1).named("X"),
                        Types.required(INT32).id(1).named("Y"),
                        Types.required(INT32).id(2).named("Z"))
                .named("root");
        try {
            ParquetFieldIdColumnResolverFactory.of(Map.of("Col1", 1)).of(schema);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Col1 -> 1 has multiple paths [X], [Y]");
        }
        // Does not fail if ambiguous id is not referenced
        {
            final ParquetColumnResolverMap expected = ParquetColumnResolverMap.builder()
                    .putMap("ColZ", List.of("Z"))
                    .build();
            final ParquetColumnResolverMap actual =
                    ParquetFieldIdColumnResolverFactory.of(Map.of("ColZ", 2)).of(schema);
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void ambiguousFieldsNested() {
        // X (1), list, element (2)
        // Y (2), list, element (1)
        // Z (3)
        final MessageType schema = Types.buildMessage()
                .addFields(
                        Types.requiredList().id(1).requiredElement(INT32).id(2).named("X"),
                        Types.requiredList().id(2).requiredElement(INT32).id(1).named("Y"),
                        Types.required(INT32).id(3).named("Z"))
                .named("root");
        System.out.println(schema);
        // Note: a different implementation _could_ take a different course of action here and proceed without error;
        // for example, an implementation could choose to consider the innermost (or outermost) field id for matching
        // purposes.
        try {
            ParquetFieldIdColumnResolverFactory.of(Map.of("Col1", 1)).of(schema);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Col1 -> 1 has multiple paths [X, list, element], [Y, list, element]");
        }
        // Does not fail if ambiguous id is not referenced
        {
            final ParquetColumnResolverMap expected = ParquetColumnResolverMap.builder()
                    .putMap("ColZ", List.of("Z"))
                    .build();
            final ParquetColumnResolverMap actual =
                    ParquetFieldIdColumnResolverFactory.of(Map.of("ColZ", 3)).of(schema);
            assertThat(actual).isEqualTo(expected);
        }
    }
}
