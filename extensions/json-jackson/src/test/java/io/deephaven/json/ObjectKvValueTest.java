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
import java.time.Instant;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;

public class ObjectKvValueTest {

    private static final ObjectEntriesValue STRING_INT_KV =
            ObjectEntriesValue.standard(IntValue.standard());

    private static final ObjectValue NAME_AGE_OBJ = ObjectValue.builder()
            .putFields("name", StringValue.standard())
            .putFields("age", IntValue.standard())
            .build();

    private static final ObjectEntriesValue STRING_OBJ_KV = ObjectEntriesValue.standard(NAME_AGE_OBJ);

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
        parse(ObjectEntriesValue.builder().key(IntValue.lenient()).value(SkipValue.lenient()).build(), List.of(
                "{\"42\": null, \"43\": null}"),
                ObjectChunk.chunkWrap(new Object[] {new int[] {42, 43}}));
    }

    @Test
    void kvObjectKey() throws IOException {
        parse(ObjectEntriesValue.builder().key(InstantValue.standard()).value(SkipValue.lenient()).build(), List.of(
                "{\"2009-02-13T23:31:30.123456788Z\": null, \"2009-02-13T23:31:30.123456789Z\": null}"),
                ObjectChunk.chunkWrap(new Object[] {new Instant[] {
                        Instant.parse("2009-02-13T23:31:30.123456788Z"),
                        Instant.parse("2009-02-13T23:31:30.123456789Z")}}));
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
