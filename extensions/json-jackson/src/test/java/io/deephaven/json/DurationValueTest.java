//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class DurationValueTest {

    private static final String XYZ_STR = "PT1H30M";

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(DurationValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.durationType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.durationType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(DurationValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.durationType().arrayType());
        assertThat(provider.stringProcessor().outputTypes())
                .containsExactly(Type.durationType().arrayType());
    }

    @Test
    void iso8601() throws IOException {
        parse(DurationValue.standard(), "\"" + XYZ_STR + "\"",
                ObjectChunk.chunkWrap(new Duration[] {Duration.ofHours(1).plusMinutes(30)}));
    }

    @Test
    void standardNull() throws IOException {
        parse(DurationValue.standard(), "null", ObjectChunk.chunkWrap(new Duration[] {null}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(DurationValue.standard(), "", ObjectChunk.chunkWrap(new Duration[] {null}));
    }

    @Test
    void strictNull() {
        try {
            process(DurationValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void strictMissing() {
        try {
            process(DurationValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void customNull() throws IOException {
        parse(DurationValue.builder().onNull(Duration.ZERO).build(), "null",
                ObjectChunk.chunkWrap(new Duration[] {Duration.ZERO}));
    }

    @Test
    void customMissing() throws IOException {
        parse(DurationValue.builder().onMissing(Duration.ZERO).build(), "",
                ObjectChunk.chunkWrap(new Duration[] {Duration.ZERO}));
    }
}
