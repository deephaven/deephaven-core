//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.LongChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class InstantOptionsTest {

    private static final String XYZ_STR = "2009-02-13T23:31:30.123456789";
    private static final long XYZ_NANOS = 1234567890L * 1_000_000_000 + 123456789;

    @Test
    void iso8601() throws IOException {
        parse(InstantOptions.standard(), "\"" + XYZ_STR + "Z\"", LongChunk.chunkWrap(new long[] {XYZ_NANOS}));
    }

    @Test
    void iso8601WithOffset() throws IOException {
        parse(InstantOptions.standard(), "\"" + XYZ_STR + "+00:00\"", LongChunk.chunkWrap(new long[] {XYZ_NANOS}));
    }

    @Test
    void standardNull() throws IOException {
        parse(InstantOptions.standard(), "null", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(InstantOptions.standard(), "", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(InstantOptions.strict(), "null", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(InstantOptions.strict(), "", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void customNull() throws IOException {
        parse(InstantOptions.builder().onNull(Instant.ofEpochMilli(0)).build(), "null",
                LongChunk.chunkWrap(new long[] {0}));
    }

    @Test
    void customMissing() throws IOException {
        parse(InstantOptions.builder().onMissing(Instant.ofEpochMilli(0)).build(), "",
                LongChunk.chunkWrap(new long[] {0}));
    }
}
