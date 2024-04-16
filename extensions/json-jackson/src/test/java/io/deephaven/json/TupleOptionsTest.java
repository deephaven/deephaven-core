//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;

public class TupleOptionsTest {

    private static final TupleValue STRING_INT_TUPLE =
            TupleValue.of(StringValue.standard(), IntValue.standard());

    private static final TupleValue STRING_SKIPINT_TUPLE =
            TupleValue.of(StringValue.standard(), IntValue.standard().skip());

    @Test
    void stringIntTuple() throws IOException {
        parse(STRING_INT_TUPLE, List.of(
                "[\"foo\", 42]",
                "[\"bar\", 43]"),
                ObjectChunk.chunkWrap(new String[] {"foo", "bar"}),
                IntChunk.chunkWrap(new int[] {42, 43}));

    }

    @Test
    void stringSkipIntTuple() throws IOException {
        parse(STRING_SKIPINT_TUPLE, List.of(
                "[\"foo\", 42]",
                "[\"bar\", 43]"),
                ObjectChunk.chunkWrap(new String[] {"foo", "bar"}));
    }
}
