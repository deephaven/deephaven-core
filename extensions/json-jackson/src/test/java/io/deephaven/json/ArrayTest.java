//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static io.deephaven.json.TestHelper.parse;

public class ArrayTest {

    @Test
    void primitive() throws IOException {
        // [1.1, null, 3.3]
        parse(DoubleOptions.standard().array(),
                "[1.1, null, 3.3]",
                ObjectChunk.chunkWrap(new Object[] {new double[] {1.1, QueryConstants.NULL_DOUBLE, 3.3}}));
    }

    @Test
    void bool() throws IOException {
        // [true, false, null]
        parse(BoolOptions.standard().array(),
                "[true, false, null]",
                ObjectChunk.chunkWrap(new Object[] {new Boolean[] {true, false, null}}));
    }

    @Test
    void tuple() throws IOException {
        // [[1, 1.1], null, [3, 3.3]]
        parse(TupleOptions.of(IntOptions.standard(), DoubleOptions.standard()).array(),
                "[[1, 1.1], null, [3, 3.3]]",
                ObjectChunk.chunkWrap(new Object[] {new int[] {1, QueryConstants.NULL_INT, 3}}),
                ObjectChunk.chunkWrap(new Object[] {new double[] {1.1, QueryConstants.NULL_DOUBLE, 3.3}}));
    }

    @Test
    void object() throws IOException {
        // [{"int": 1, "double": 1.1}, null, {}, {"int": 4, "double": 4.4}]
        parse(ObjectOptions.builder()
                .putFields("int", IntOptions.standard())
                .putFields("double", DoubleOptions.standard())
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
        parse(TypedObjectOptions.builder()
                .typeFieldName("type")
                .putObjects("int", ObjectOptions.standard(Map.of("value", IntOptions.standard())))
                .putObjects("string", ObjectOptions.standard(Map.of("value", StringOptions.standard())))
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
        parse(InstantOptions.standard().array(),
                "[\"2009-02-13T23:31:30.123456789Z\", null]",
                ObjectChunk.chunkWrap(new Object[] {new long[] {1234567890123456789L, QueryConstants.NULL_LONG}}));
    }

    @Test
    void tupleSkip() throws IOException {
        // [[1, 1], [2, 2.2], [3, "foo"], [4, true], [5, false], [6, {}], [7, []], [8, null], null]
        parse(TupleOptions.of(IntOptions.standard(), SkipOptions.lenient()).array(),
                "[[1, 1], [2, 2.2], [3, \"foo\"], [4, true], [5, false], [6, {}], [7, []], [8, null], null]",
                ObjectChunk.chunkWrap(new Object[] {new int[] {1, 2, 3, 4, 5, 6, 7, 8, QueryConstants.NULL_INT}}));
    }
}
