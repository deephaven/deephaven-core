//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;

public class ObjectKvOptionsTest {

    private static final ObjectKvOptions STRING_INT_KV =
            ObjectKvOptions.standard(IntOptions.standard());

    private static final ObjectOptions NAME_AGE_OBJ = ObjectOptions.builder()
            .putFields("name", StringOptions.standard())
            .putFields("age", IntOptions.standard())
            .build();

    private static final ObjectKvOptions STRING_OBJ_KV = ObjectKvOptions.standard(NAME_AGE_OBJ);

    @Test
    void kvPrimitiveValue() throws IOException {
        parse(STRING_INT_KV, List.of(
                "{\"A\": 42, \"B\": null}"),
                ObjectChunk.chunkWrap(new Object[] {new String[] {"A", "B"}}),
                ObjectChunk.chunkWrap(new Object[] {new int[] {42, QueryConstants.NULL_INT}}));
    }

    @Test
    void kvObjectValue() throws IOException {
        parse(STRING_OBJ_KV, List.of(
                "{\"A\": {\"name\": \"Foo\", \"age\": 42}, \"B\": {}, \"C\": null}"),
                ObjectChunk.chunkWrap(new Object[] {new String[] {"A", "B", "C"}}),
                ObjectChunk.chunkWrap(new Object[] {new String[] {"Foo", null, null}}),
                ObjectChunk.chunkWrap(new Object[] {new int[] {42, QueryConstants.NULL_INT, QueryConstants.NULL_INT}}));
    }

    @Test
    void kvPrimitiveKey() throws IOException {
        parse(ObjectKvOptions.builder().key(IntOptions.lenient()).value(SkipOptions.lenient()).build(), List.of(
                "{\"42\": null, \"43\": null}"),
                ObjectChunk.chunkWrap(new Object[] {new int[] {42, 43}}));
    }

    @Test
    void kvObjectKey() throws IOException {
        parse(ObjectKvOptions.builder().key(InstantOptions.standard()).value(SkipOptions.lenient()).build(), List.of(
                "{\"2009-02-13T23:31:30.123456788Z\": null, \"2009-02-13T23:31:30.123456789Z\": null}"),
                ObjectChunk.chunkWrap(new Object[] {new long[] {1234567890123456788L, 1234567890123456789L}}));
    }

    @Test
    void columnNames() {
        assertThat(JacksonProvider.of(STRING_INT_KV).named(Type.stringType()).names()).containsExactly("Key",
                "Value");
    }

    @Test
    void columnNamesValueIsObject() {
        assertThat(JacksonProvider.of(STRING_OBJ_KV).named(Type.stringType()).names()).containsExactly("Key",
                "name", "age");
    }
}
