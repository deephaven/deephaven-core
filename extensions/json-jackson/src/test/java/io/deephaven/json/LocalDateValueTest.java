//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDate;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class LocalDateValueTest {

    private static final String XYZ_STR = "2009-02-13";

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(LocalDateValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.ofCustom(LocalDate.class));
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.ofCustom(LocalDate.class));
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(LocalDateValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.ofCustom(LocalDate.class).arrayType());
        assertThat(provider.stringProcessor().outputTypes())
                .containsExactly(Type.ofCustom(LocalDate.class).arrayType());
    }

    @Test
    void iso8601() throws IOException {
        parse(LocalDateValue.standard(), "\"" + XYZ_STR + "\"",
                ObjectChunk.chunkWrap(new LocalDate[] {LocalDate.of(2009, 2, 13)}));
    }

    @Test
    void standardNull() throws IOException {
        parse(LocalDateValue.standard(), "null", ObjectChunk.chunkWrap(new LocalDate[] {null}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(LocalDateValue.standard(), "", ObjectChunk.chunkWrap(new LocalDate[] {null}));
    }

    @Test
    void strictNull() {
        try {
            process(LocalDateValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void strictMissing() {
        try {
            process(LocalDateValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void customNull() throws IOException {
        parse(LocalDateValue.builder().onNull(LocalDate.ofEpochDay(0)).build(), "null",
                ObjectChunk.chunkWrap(new LocalDate[] {LocalDate.ofEpochDay(0)}));
    }

    @Test
    void customMissing() throws IOException {
        parse(LocalDateValue.builder().onMissing(LocalDate.ofEpochDay(0)).build(), "",
                ObjectChunk.chunkWrap(new LocalDate[] {LocalDate.ofEpochDay(0)}));
    }
}
