//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class FloatValueTest {

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(FloatValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.floatType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.floatType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(FloatValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.floatType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.floatType().arrayType());
    }

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
    void strictMissing() {
        try {
            process(FloatValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(FloatValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardString() {
        try {
            process(FloatValue.standard(), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(FloatValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(FloatValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardObject() {
        try {
            process(FloatValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(FloatValue.standard(), "[]");
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
