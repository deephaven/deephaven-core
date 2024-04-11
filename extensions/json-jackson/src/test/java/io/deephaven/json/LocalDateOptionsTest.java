//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDate;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class LocalDateOptionsTest {

    private static final String XYZ_STR = "2009-02-13";

    @Test
    void iso8601() throws IOException {
        parse(LocalDateOptions.standard(), "\"" + XYZ_STR + "\"",
                ObjectChunk.chunkWrap(new LocalDate[] {LocalDate.of(2009, 2, 13)}));
    }

    @Test
    void standardNull() throws IOException {
        parse(LocalDateOptions.standard(), "null", ObjectChunk.chunkWrap(new LocalDate[] {null}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(LocalDateOptions.standard(), "", ObjectChunk.chunkWrap(new LocalDate[] {null}));
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(LocalDateOptions.strict(), "null", ObjectChunk.chunkWrap(new LocalDate[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(LocalDateOptions.strict(), "", ObjectChunk.chunkWrap(new LocalDate[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void customNull() throws IOException {
        parse(LocalDateOptions.builder().onNull(LocalDate.ofEpochDay(0)).build(), "null",
                ObjectChunk.chunkWrap(new LocalDate[] {LocalDate.ofEpochDay(0)}));
    }

    @Test
    void customMissing() throws IOException {
        parse(LocalDateOptions.builder().onMissing(LocalDate.ofEpochDay(0)).build(), "",
                ObjectChunk.chunkWrap(new LocalDate[] {LocalDate.ofEpochDay(0)}));
    }
}
