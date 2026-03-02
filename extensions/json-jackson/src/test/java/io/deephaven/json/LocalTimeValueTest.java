//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalTime;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class LocalTimeValueTest {

    private static final String XYZ_STR = "12:51:00";

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(LocalTimeValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.localTimeType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.localTimeType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(LocalTimeValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.localTimeType().arrayType());
        assertThat(provider.stringProcessor().outputTypes())
                .containsExactly(Type.localTimeType().arrayType());
    }

    @Test
    void iso8601() throws IOException {
        parse(LocalTimeValue.standard(), "\"" + XYZ_STR + "\"",
                ObjectChunk.chunkWrap(new LocalTime[] {LocalTime.of(12, 51, 0)}));
    }

    @Test
    void standardNull() throws IOException {
        parse(LocalTimeValue.standard(), "null", ObjectChunk.chunkWrap(new LocalTime[] {null}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(LocalTimeValue.standard(), "", ObjectChunk.chunkWrap(new LocalTime[] {null}));
    }

    @Test
    void strictNull() {
        try {
            process(LocalTimeValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void strictMissing() {
        try {
            process(LocalTimeValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void customNull() throws IOException {
        parse(LocalTimeValue.builder().onNull(LocalTime.of(12, 51)).build(), "null",
                ObjectChunk.chunkWrap(new LocalTime[] {LocalTime.of(12, 51)}));
    }

    @Test
    void customMissing() throws IOException {
        parse(LocalTimeValue.builder().onMissing(LocalTime.of(12, 51)).build(), "",
                ObjectChunk.chunkWrap(new LocalTime[] {LocalTime.of(12, 51)}));
    }
}
