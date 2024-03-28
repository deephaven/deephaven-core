//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.AnyOptions;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.IntOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.TestHelper;
import io.deephaven.json.TupleOptions;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JacksonAnyOptionsTest {

    @Test
    void anyMissing() throws IOException {
        checkAny("", MissingNode.getInstance());
    }

    @Test
    void anyNull() throws IOException {
        checkAny("null", NullNode.getInstance());
    }

    @Test
    void anyTrue() throws IOException {
        checkAny("true", BooleanNode.getTrue());
    }

    @Test
    void anyFalse() throws IOException {
        checkAny("false", BooleanNode.getFalse());
    }

    @Test
    void anyNumberInt() throws IOException {
        checkAny("42", IntNode.valueOf(42));
    }

    @Test
    void anyNumberFloat() throws IOException {
        checkAny("42.42", DoubleNode.valueOf(42.42));
    }

    @Test
    void anyNumberString() throws IOException {
        checkAny("\"my string\"", TextNode.valueOf("my string"));
    }

    @Test
    void anyObject() throws IOException {
        checkAny("{}", new ObjectNode(null, Map.of()));
        checkAny("{\"foo\": 42}", new ObjectNode(null, Map.of("foo", IntNode.valueOf(42))));
    }

    @Test
    void anyArray() throws IOException {
        checkAny("[]", new ArrayNode(null, List.of()));
        checkAny("[42]", new ArrayNode(null, List.of(IntNode.valueOf(42))));
    }

    @Test
    void anyInTuple() throws IOException {
        final TupleOptions options = TupleOptions.of(IntOptions.standard(), AnyOptions.of(), DoubleOptions.standard());
        TestHelper.parse(options, List.of("", "[42, {\"zip\": 43}, 44.44]"),
                IntChunk.chunkWrap(new int[] {QueryConstants.NULL_INT, 42}),
                ObjectChunk.chunkWrap(new TreeNode[] {MissingNode.getInstance(),
                        new ObjectNode(null, Map.of("zip", IntNode.valueOf(43)))}),
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 44.44}));
    }

    @Test
    void anyInObject() throws IOException {
        final ObjectOptions options = ObjectOptions.builder()
                .putFields("foo", IntOptions.standard())
                .putFields("bar", AnyOptions.of())
                .putFields("baz", DoubleOptions.standard())
                .build();
        TestHelper.parse(options, List.of("", "{\"foo\": 42, \"bar\": {\"zip\": 43}, \"baz\": 44.44}"),
                IntChunk.chunkWrap(new int[] {QueryConstants.NULL_INT, 42}),
                ObjectChunk.chunkWrap(new TreeNode[] {MissingNode.getInstance(),
                        new ObjectNode(null, Map.of("zip", IntNode.valueOf(43)))}),
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 44.44}));
    }

    private static void checkAny(String json, TreeNode expected) throws IOException {
        TestHelper.parse(AnyOptions.of(), json, ObjectChunk.chunkWrap(new TreeNode[] {expected}));
    }
}
