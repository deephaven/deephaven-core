//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class TupleValueTest {

    private static final TupleValue STRING_INT_TUPLE =
            TupleValue.of(StringValue.standard(), IntValue.standard());

    private static final TupleValue STRING_SKIPINT_TUPLE =
            TupleValue.of(StringValue.standard(), IntValue.standard().skip());

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(STRING_INT_TUPLE);
        assertThat(provider.outputTypes()).containsExactly(Type.stringType(), Type.intType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.stringType(), Type.intType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(STRING_INT_TUPLE.array());
        assertThat(provider.outputTypes()).containsExactly(Type.stringType().arrayType(), Type.intType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.stringType().arrayType(),
                Type.intType().arrayType());
    }

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

    @Test
    void indexException() {
        try {
            process(STRING_INT_TUPLE, "[\"foo\", 43.43]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unable to process tuple ix 1");
            assertThat(e).hasCauseInstanceOf(IOException.class);
            assertThat(e.getCause()).hasMessageContaining("Decimal not allowed");
            assertThat(e.getCause()).hasNoCause();
        }
    }
}
