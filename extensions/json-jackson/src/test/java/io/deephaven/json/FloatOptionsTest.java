//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class FloatOptionsTest {

    @Test
    void standard() throws IOException {
        parse(FloatOptions.standard(), List.of("42", "42.42"), FloatChunk.chunkWrap(new float[] {42, 42.42f}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(FloatOptions.standard(), "", FloatChunk.chunkWrap(new float[] {QueryConstants.NULL_FLOAT}));
    }

    @Test
    void standardNull() throws IOException {
        parse(FloatOptions.standard(), "null", FloatChunk.chunkWrap(new float[] {QueryConstants.NULL_FLOAT}));
    }

    @Test
    void customMissing() throws IOException {
        parse(FloatOptions.builder().onMissing(-1.0f).build(), "", FloatChunk.chunkWrap(new float[] {-1}));
    }

    @Test
    void strict() throws IOException {
        parse(FloatOptions.strict(), "42", FloatChunk.chunkWrap(new float[] {42}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(FloatOptions.strict(), "", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(FloatOptions.strict(), "null", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }

    @Test
    void standardString() throws IOException {
        try {
            parse(FloatOptions.standard(), "\"42\"", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_STRING'");
        }
    }

    @Test
    void standardTrue() throws IOException {
        try {
            parse(FloatOptions.standard(), "true", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_TRUE'");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(FloatOptions.standard(), "false", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_FALSE'");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(FloatOptions.standard(), "{}", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_OBJECT'");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(FloatOptions.standard(), "[]", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_ARRAY'");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(FloatOptions.lenient(), List.of("\"42\"", "\"42.42\""), FloatChunk.chunkWrap(new float[] {42, 42.42f}));
    }
}
