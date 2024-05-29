//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.InstantNumberValue.Format;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static io.deephaven.json.TestHelper.parse;

public class ArrayTest {

    @Test
    void primitive() throws IOException {
        // [1.1, null, 3.3]
        parse(DoubleValue.standard().array(),
                "[1.1, null, 3.3]",
                ObjectChunk.chunkWrap(new Object[] {new double[] {1.1, QueryConstants.NULL_DOUBLE, 3.3}}));
    }

    @Test
    void bool() throws IOException {
        // [true, false, null]
        parse(BoolValue.standard().array(),
                "[true, false, null]",
                ObjectChunk.chunkWrap(new Object[] {new Boolean[] {true, false, null}}));
    }

    @Test
    void tuple() throws IOException {
        // [[1, 1.1], null, [3, 3.3]]
        parse(TupleValue.of(IntValue.standard(), DoubleValue.standard()).array(),
                "[[1, 1.1], null, [3, 3.3]]",
                ObjectChunk.chunkWrap(new Object[] {new int[] {1, QueryConstants.NULL_INT, 3}}),
                ObjectChunk.chunkWrap(new Object[] {new double[] {1.1, QueryConstants.NULL_DOUBLE, 3.3}}));
    }

    @Test
    void object() throws IOException {
        // [{"int": 1, "double": 1.1}, null, {}, {"int": 4, "double": 4.4}]
        parse(ObjectValue.builder()
                .putFields("int", IntValue.standard())
                .putFields("double", DoubleValue.standard())
                .build()
                .array(),
                "[{\"int\": 1, \"double\": 1.1}, null, {}, {\"int\": 4, \"double\": 4.4}]",
                ObjectChunk
                        .chunkWrap(new Object[] {new int[] {1, QueryConstants.NULL_INT, QueryConstants.NULL_INT, 4}}),
                ObjectChunk.chunkWrap(new Object[] {
                        new double[] {1.1, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, 4.4}}));
    }

    // missing feature
    @Disabled
    @Test
    void typedObject() throws IOException {
        // [ {"type": "int", "value": 42}, {"type": "string", "value": "foo"} ]
        parse(TypedObjectValue.builder()
                .typeFieldName("type")
                .putObjects("int", ObjectValue.standard(Map.of("value", IntValue.standard())))
                .putObjects("string", ObjectValue.standard(Map.of("value", StringValue.standard())))
                .build()
                .array(),
                "[ {\"type\": \"int\", \"value\": 42}, {\"type\": \"string\", \"value\": \"foo\"} ]",
                ObjectChunk.chunkWrap(new Object[] {new String[] {"int", "string"}}),
                ObjectChunk
                        .chunkWrap(new Object[] {new int[] {42, QueryConstants.NULL_INT}}),
                ObjectChunk.chunkWrap(new Object[] {new String[] {null, "foo"}}));
    }

    @Test
    void instant() throws IOException {
        // ["2009-02-13T23:31:30.123456789Z", null]
        parse(InstantValue.standard().array(),
                "[\"2009-02-13T23:31:30.123456789Z\", null]",
                ObjectChunk.chunkWrap(
                        new Object[] {new Instant[] {Instant.parse("2009-02-13T23:31:30.123456789Z"), null}}));
    }

    @Test
    void instantNumber() throws IOException {
        // [1234567, null]
        parse(Format.EPOCH_SECONDS.standard(false).array(),
                "[1234567, null]",
                ObjectChunk.chunkWrap(new Object[] {new Instant[] {Instant.ofEpochSecond(1234567), null}}));
    }

    @Test
    void tupleSkip() throws IOException {
        // [[1, 1], [2, 2.2], [3, "foo"], [4, true], [5, false], [6, {}], [7, []], [8, null], null]
        parse(TupleValue.of(IntValue.standard(), SkipValue.lenient()).array(),
                "[[1, 1], [2, 2.2], [3, \"foo\"], [4, true], [5, false], [6, {}], [7, []], [8, null], null]",
                ObjectChunk.chunkWrap(new Object[] {new int[] {1, 2, 3, 4, 5, 6, 7, 8, QueryConstants.NULL_INT}}));
    }
}
