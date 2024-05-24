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
        parse(FloatValue.standard(), List.of("42", "42.42"), FloatChunk.chunkWrap(new float[] {42, 42.42f}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(FloatValue.standard(), "", FloatChunk.chunkWrap(new float[] {QueryConstants.NULL_FLOAT}));
    }

    @Test
    void standardNull() throws IOException {
        parse(FloatValue.standard(), "null", FloatChunk.chunkWrap(new float[] {QueryConstants.NULL_FLOAT}));
    }

    @Test
    void customMissing() throws IOException {
        parse(FloatValue.builder().onMissing(-1.0f).build(), "", FloatChunk.chunkWrap(new float[] {-1}));
    }

    @Test
    void strict() throws IOException {
        parse(FloatValue.strict(), "42", FloatChunk.chunkWrap(new float[] {42}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(FloatValue.strict(), "", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(FloatValue.strict(), "null", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardString() throws IOException {
        try {
            parse(FloatValue.standard(), "\"42\"", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() throws IOException {
        try {
            parse(FloatValue.standard(), "true", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(FloatValue.standard(), "false", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(FloatValue.standard(), "{}", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(FloatValue.standard(), "[]", FloatChunk.chunkWrap(new float[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(FloatValue.lenient(), List.of("\"42\"", "\"42.42\""), FloatChunk.chunkWrap(new float[] {42, 42.42f}));
    }
}
