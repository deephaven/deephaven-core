//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ObjectValueTest {

    public static final ObjectValue OBJECT_AGE_FIELD = ObjectValue.builder()
            .putFields("age", IntValue.standard())
            .build();

    private static final ObjectValue OBJECT_NAME_AGE_FIELD = ObjectValue.builder()
            .putFields("name", StringValue.standard())
            .putFields("age", IntValue.standard())
            .build();

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(OBJECT_NAME_AGE_FIELD);
        assertThat(provider.outputTypes()).containsExactly(Type.stringType(), Type.intType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.stringType(), Type.intType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(OBJECT_NAME_AGE_FIELD.array());
        assertThat(provider.outputTypes()).containsExactly(Type.stringType().arrayType(), Type.intType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.stringType().arrayType(),
                Type.intType().arrayType());
    }

    @Test
    void ofAge() throws IOException {
        parse(OBJECT_AGE_FIELD, List.of(
                "",
                "null",
                "{}",
                "{\"age\": 42}",
                "{\"name\": \"Devin\", \"age\": 43}"),
                IntChunk.chunkWrap(
                        new int[] {QueryConstants.NULL_INT, QueryConstants.NULL_INT, QueryConstants.NULL_INT, 42, 43}));
    }

    @Test
    void ofNameAge() throws IOException {
        parse(OBJECT_NAME_AGE_FIELD, List.of(
                // "",
                // "null",
                // "{}",
                // "{\"age\": 42}",
                "{\"name\": \"Devin\", \"age\": 43}"),
                ObjectChunk.chunkWrap(new String[] {"Devin"}),
                IntChunk.chunkWrap(
                        new int[] {43}));
    }

    @Test
    void caseInsensitive() throws IOException {
        final ObjectValue options = ObjectValue.builder()
                .addFields(ObjectField.builder()
                        .name("Foo")
                        .options(IntValue.standard())
                        .caseSensitive(false)
                        .build())
                .build();
        parse(options, List.of("{\"Foo\": 42}", "{\"fOO\": 43}"),
                IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void caseSensitive() throws IOException {
        final ObjectField f1 = ObjectField.of("Foo", IntValue.standard());
        final ObjectField f2 = ObjectField.of("foo", IntValue.standard());
        final ObjectValue options = ObjectValue.builder()
                .addFields(f1)
                .addFields(f2)
                .build();
        parse(options, List.of("{\"Foo\": 42, \"foo\": 43}"),
                IntChunk.chunkWrap(new int[] {42}),
                IntChunk.chunkWrap(new int[] {43}));
    }

    @Test
    void alias() throws IOException {
        final ObjectValue options = ObjectValue.builder()
                .addFields(ObjectField.builder()
                        .name("FooBar")
                        .options(IntValue.standard())
                        .addAliases("Foo_Bar")
                        .build())
                .build();
        parse(options, List.of("{\"FooBar\": 42}", "{\"Foo_Bar\": 43}"),
                IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void caseInsensitiveAlias() throws IOException {
        final ObjectValue options = ObjectValue.builder()
                .addFields(ObjectField.builder()
                        .name("FooBar")
                        .options(IntValue.standard())
                        .addAliases("Foo_Bar")
                        .caseSensitive(false)
                        .build())
                .build();
        parse(options, List.of("{\"fooBar\": 42}", "{\"fOO_BAR\": 43}"),
                IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void caseSensitiveFields() {
        final ObjectField f1 = ObjectField.of("Foo", IntValue.standard());
        final ObjectField f2 = ObjectField.of("foo", IntValue.standard());
        final ObjectValue options = ObjectValue.builder()
                .addFields(f1)
                .addFields(f2)
                .build();
        assertThat(options.fields()).containsExactly(f1, f2);
    }

    @Test
    void caseInsensitiveOverlap() {
        final ObjectField f1 = ObjectField.of("Foo", IntValue.standard());
        final ObjectField f2 = ObjectField.builder()
                .name("foo")
                .options(IntValue.standard())
                .caseSensitive(false)
                .build();
        try {
            ObjectValue.builder()
                    .addFields(f1)
                    .addFields(f2)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Found overlapping field name 'foo'");
        }
    }

    @Test
    void objectFields() throws IOException {
        // { "prices": [1.1, 2.2, 3.3], "other": [2, 4, 8, 16] }
        parse(ObjectValue.builder()
                .putFields("prices", DoubleValue.standard().array())
                .putFields("other", LongValue.standard().array())
                .build(),
                "{ \"prices\": [1.1, 2.2, 3.3], \"other\": [2, 4, 8, 16] }",
                ObjectChunk
                        .chunkWrap(new Object[] {new double[] {1.1, 2.2, 3.3}}),
                ObjectChunk.chunkWrap(new Object[] {
                        new long[] {2, 4, 8, 16}}));
    }

    @Test
    void objectFieldsArrayGroup() throws IOException {
        // Note: array groups don't cause any difference wrt ObjectProcessor based destructuring
        // { "prices": [1.1, 2.2, 3.3], "sizes": [2, 4, 8] }
        parse(ObjectValue.builder()
                .addFields(ObjectField.builder()
                        .name("prices")
                        .options(DoubleValue.standard().array())
                        .arrayGroup("prices_and_sizes")
                        .build())
                .addFields(ObjectField.builder()
                        .name("sizes")
                        .options(LongValue.standard().array())
                        .arrayGroup("prices_and_sizes")
                        .build())
                .build(),
                "{ \"prices\": [1.1, 2.2, 3.3], \"sizes\": [2, 4, 8] }",
                ObjectChunk
                        .chunkWrap(new Object[] {new double[] {1.1, 2.2, 3.3}}),
                ObjectChunk.chunkWrap(new Object[] {
                        new long[] {2, 4, 8}}));
    }

    @Test
    void columnNames() {
        assertThat(JacksonProvider.of(OBJECT_NAME_AGE_FIELD).named(Type.stringType()).names())
                .containsExactly("name", "age");
    }

    @Test
    void columnNamesAlternateName() {
        final ObjectValue obj = ObjectValue.builder()
                .addFields(ObjectField.builder()
                        .name("MyName")
                        .addAliases("name")
                        .options(StringValue.standard())
                        .build())
                .addFields(ObjectField.of("age", IntValue.standard()))
                .build();
        assertThat(JacksonProvider.of(obj).named(Type.stringType()).names()).containsExactly("MyName", "age");
    }

    @Test
    void columnNamesWithFieldThatIsNotColumnNameCompatible() {
        final ObjectValue objPlusOneMinusOneCount = ObjectValue.builder()
                .putFields("+1", IntValue.standard())
                .putFields("-1", IntValue.standard())
                .build();
        assertThat(JacksonProvider.of(objPlusOneMinusOneCount).named(Type.stringType()).names())
                .containsExactly("column_1", "column_12");
    }

    @Test
    void fieldException() {
        try {
            process(OBJECT_NAME_AGE_FIELD, "{\"name\": \"Devin\", \"age\": 43.42}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unable to process field 'age'");
            assertThat(e).hasCauseInstanceOf(IOException.class);
            assertThat(e.getCause()).hasMessageContaining("Decimal not allowed");
            assertThat(e.getCause()).hasNoCause();
        }
    }
}
