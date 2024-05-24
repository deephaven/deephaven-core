//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class DoubleOptionsTest {

    @Test
    void standard() throws IOException {
        parse(DoubleValue.standard(), List.of("42", "42.42"), DoubleChunk.chunkWrap(new double[] {42, 42.42}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(DoubleValue.standard(), "", DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}));
    }

    @Test
    void standardNull() throws IOException {
        parse(DoubleValue.standard(), "null", DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}));
    }

    @Test
    void customMissing() throws IOException {
        parse(DoubleValue.builder().onMissing(-1.0).build(), "", DoubleChunk.chunkWrap(new double[] {-1}));
    }

    @Test
    void strict() throws IOException {
        parse(DoubleValue.strict(), "42", DoubleChunk.chunkWrap(new double[] {42}));
    }

    @Test
    void strictMissing() {
        try {
            process(DoubleValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(DoubleValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardString() {
        try {
            process(DoubleValue.standard(), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(DoubleValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(DoubleValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardObject() {
        try {
            process(DoubleValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(DoubleValue.standard(), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(DoubleValue.lenient(), List.of("\"42\"", "\"42.42\""), DoubleChunk.chunkWrap(new double[] {42, 42.42}));
    }
}
