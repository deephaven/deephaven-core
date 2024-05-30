//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.LongChunk;
import io.deephaven.json.InstantNumberValue.Format;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;

public class InstantNumberTest {
    private static final long WITH_SECONDS = 1703292532000000000L;
    private static final long WITH_MILLIS = 1703292532123000000L;
    private static final long WITH_MICROS = 1703292532123456000L;
    private static final long WITH_NANOS = 1703292532123456789L;

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(Format.EPOCH_SECONDS.standard(false));
        assertThat(provider.outputTypes()).containsExactly(Type.instantType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.instantType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(Format.EPOCH_SECONDS.standard(false).array());
        assertThat(provider.outputTypes()).containsExactly(Type.instantType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.instantType().arrayType());
    }

    @Test
    void epochSeconds() throws IOException {
        parse(Format.EPOCH_SECONDS.standard(false), "1703292532", LongChunk.chunkWrap(new long[] {WITH_SECONDS}));
    }

    @Test
    void epochSecondsDecimal() throws IOException {
        parse(Format.EPOCH_SECONDS.standard(true), "1703292532.123456789",
                LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochSecondsString() throws IOException {
        parse(Format.EPOCH_SECONDS.lenient(false), "\"1703292532\"", LongChunk.chunkWrap(new long[] {WITH_SECONDS}));
    }

    @Test
    void epochSecondsStringDecimal() throws IOException {
        parse(Format.EPOCH_SECONDS.lenient(true), "\"1703292532.123456789\"",
                LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochMillis() throws IOException {
        parse(Format.EPOCH_MILLIS.standard(false), "1703292532123", LongChunk.chunkWrap(new long[] {WITH_MILLIS}));
    }

    @Test
    void epochMillisDecimal() throws IOException {
        parse(Format.EPOCH_MILLIS.standard(true), "1703292532123.456789", LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochMillisString() throws IOException {
        parse(Format.EPOCH_MILLIS.lenient(false), "\"1703292532123\"", LongChunk.chunkWrap(new long[] {WITH_MILLIS}));
    }

    @Test
    void epochMillisStringDecimal() throws IOException {
        parse(Format.EPOCH_MILLIS.lenient(true), "\"1703292532123.456789\"",
                LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochMicros() throws IOException {
        parse(Format.EPOCH_MICROS.standard(false), "1703292532123456", LongChunk.chunkWrap(new long[] {WITH_MICROS}));
    }

    @Test
    void epochMicrosDecimal() throws IOException {
        parse(Format.EPOCH_MICROS.standard(true), "1703292532123456.789", LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochMicrosString() throws IOException {
        parse(Format.EPOCH_MICROS.lenient(false), "\"1703292532123456\"",
                LongChunk.chunkWrap(new long[] {WITH_MICROS}));
    }

    @Test
    void epochMicrosStringDecimal() throws IOException {
        parse(Format.EPOCH_MICROS.lenient(true), "\"1703292532123456.789\"",
                LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochNanos_() throws IOException {
        parse(Format.EPOCH_NANOS.standard(false), "1703292532123456789", LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochNanosDecimal() throws IOException {
        parse(Format.EPOCH_NANOS.standard(true), "1703292532123456789.0", LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochNanosString() throws IOException {
        parse(Format.EPOCH_NANOS.lenient(false), "\"1703292532123456789\"",
                LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }

    @Test
    void epochNanosStringDecimal() throws IOException {
        parse(Format.EPOCH_NANOS.lenient(true), "\"1703292532123456789.0\"",
                LongChunk.chunkWrap(new long[] {WITH_NANOS}));
    }
}
