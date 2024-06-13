//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;

public class RepeatedProcessorTests {

    @Test
    void arrayArrayPrimitive() throws IOException {
        // [[1.1], null, [], [2.2, 3.3]]
        parse(DoubleValue.standard().array().array(),
                "[[1.1], null, [], [2.2, 3.3]]",
                ObjectChunk.chunkWrap(new Object[] {
                        new double[][] {new double[] {1.1}, null, new double[0], new double[] {2.2, 3.3}}}));
    }

    @Test
    void arrayKvPrimitive() throws IOException {
        // [{"a": 1.1}, null, {}, {"b": 2.2, "c": 3.3}]
        parse(ObjectEntriesValue.builder().key(SkipValue.lenient()).value(DoubleValue.standard()).build().array(),
                "[{\"a\": 1.1}, null, {}, {\"b\": 2.2, \"c\": 3.3}]",
                ObjectChunk.chunkWrap(new Object[] {
                        new double[][] {new double[] {1.1}, null, new double[0], new double[] {2.2, 3.3}}}));
    }

    @Test
    void kvArrayPrimitive() throws IOException {
        // {"a": [1.1], "b": null, "c": [], "d": [2.2, 3.3]}
        parse(ObjectEntriesValue.standard(DoubleValue.standard().array()),
                "{\"a\": [1.1], \"b\": null, \"c\": [], \"d\": [2.2, 3.3]}",
                ObjectChunk.chunkWrap(new Object[] {
                        new String[] {"a", "b", "c", "d"}}),
                ObjectChunk.chunkWrap(new Object[] {
                        new double[][] {new double[] {1.1}, null, new double[0], new double[] {2.2, 3.3}}}));
    }

    @Test
    void tuple() throws IOException {
        // [[[1, 1.1]], null, [], [[2, 2.2], [3, 3.3]]]
        parse(TupleValue.of(IntValue.standard(), DoubleValue.standard()).array().array(),
                "[[[1, 1.1]], null, [], [[2, 2.2], [3, 3.3]]]",
                ObjectChunk.chunkWrap(new Object[] {new int[][] {new int[] {1}, null, new int[0], new int[] {2, 3}}}),
                ObjectChunk.chunkWrap(new Object[] {
                        new double[][] {new double[] {1.1}, null, new double[0], new double[] {2.2, 3.3}}}));
    }

    @Test
    void object() throws IOException {
        // [[{"int": 1, "double": 1.1}], null, [], [{"int": 2}, {"double": 3.3}], [{"int": 4, "double": 4.4}, {"int": 5,
        // "double": 5.5}]]
        parse(ObjectValue.builder()
                .putFields("int", IntValue.standard())
                .putFields("double", DoubleValue.standard())
                .build()
                .array()
                .array(),
                "[[{\"int\": 1, \"double\": 1.1}], null, [], [{\"int\": 2}, {\"double\": 3.3}], [{\"int\": 4, \"double\": 4.4}, {\"int\": 5, \"double\": 5.5}]]",
                ObjectChunk.chunkWrap(new Object[] {new int[][] {new int[] {1}, null, new int[0],
                        new int[] {2, QueryConstants.NULL_INT}, new int[] {4, 5}}}),
                ObjectChunk.chunkWrap(new Object[] {new double[][] {new double[] {1.1}, null, new double[0],
                        new double[] {QueryConstants.NULL_DOUBLE, 3.3}, new double[] {4.4, 5.5}}}));
    }


    @Test
    void differentNesting() throws IOException {
        // [ { "foo": [ { "bar": 41 }, {} ], "baz": 1.1 }, null, {}, { "foo": [] }, { "foo": [ { "bar": 43 } ], "baz":
        // 3.3 }]
        parse(ObjectValue.builder()
                .putFields("foo", ObjectValue.builder()
                        .putFields("bar", IntValue.standard())
                        .build()
                        .array())
                .putFields("baz", DoubleValue.standard())
                .build()
                .array(),
                "[ { \"foo\": [ { \"bar\": 41 }, {} ], \"baz\": 1.1 }, null, {}, { \"foo\": [] }, { \"foo\": [ { \"bar\": 43 } ], \"baz\": 3.3 }]",
                ObjectChunk.chunkWrap(new Object[] {
                        new int[][] {new int[] {41, QueryConstants.NULL_INT}, null, null, new int[0], new int[] {43}}}),
                ObjectChunk.chunkWrap(new Object[] {new double[] {1.1, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, 3.3}}));
    }
}
