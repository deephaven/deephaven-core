//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.LongChunk;
import io.deephaven.json.InstantNumberValue.Format;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class InstantValueTest {

    private static final String XYZ_STR = "2009-02-13T23:31:30.123456789";
    private static final long XYZ_NANOS = 1234567890L * 1_000_000_000 + 123456789;

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(InstantValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.instantType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.instantType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(InstantValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.instantType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.instantType().arrayType());
    }

    @Test
    void iso8601() throws IOException {
        parse(InstantValue.standard(), "\"" + XYZ_STR + "Z\"", LongChunk.chunkWrap(new long[] {XYZ_NANOS}));
    }

    @Test
    void iso8601WithOffset() throws IOException {
        parse(InstantValue.standard(), "\"" + XYZ_STR + "+00:00\"", LongChunk.chunkWrap(new long[] {XYZ_NANOS}));
    }

    @Test
    void standardNull() throws IOException {
        parse(InstantValue.standard(), "null", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(InstantValue.standard(), "", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void strictNull() throws IOException {
        try {
            process(InstantValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void strictMissing() throws IOException {
        try {
            process(InstantValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void customNull() throws IOException {
        parse(InstantValue.builder().onNull(Instant.ofEpochMilli(0)).build(), "null",
                LongChunk.chunkWrap(new long[] {0}));
    }

    @Test
    void customMissing() throws IOException {
        parse(InstantValue.builder().onMissing(Instant.ofEpochMilli(0)).build(), "",
                LongChunk.chunkWrap(new long[] {0}));
    }
}
