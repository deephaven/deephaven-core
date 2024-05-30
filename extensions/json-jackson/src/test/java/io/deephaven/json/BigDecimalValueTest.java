//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class BigDecimalValueTest {

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(BigDecimalValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.ofCustom(BigDecimal.class));
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.ofCustom(BigDecimal.class));
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(BigDecimalValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.ofCustom(BigDecimal.class).arrayType());
        assertThat(provider.stringProcessor().outputTypes())
                .containsExactly(Type.ofCustom(BigDecimal.class).arrayType());
    }

    @Test
    void standard() throws IOException {
        parse(BigDecimalValue.standard(), "42.42", ObjectChunk.chunkWrap(new BigDecimal[] {new BigDecimal("42.42")}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(BigDecimalValue.standard(), "", ObjectChunk.chunkWrap(new BigDecimal[] {null}));
    }

    @Test
    void standardNull() throws IOException {
        parse(BigDecimalValue.standard(), "null", ObjectChunk.chunkWrap(new BigDecimal[] {null}));
    }

    @Test
    void customMissing() throws IOException {
        parse(BigDecimalValue.builder().onMissing(BigDecimal.valueOf(-1)).build(), "",
                ObjectChunk.chunkWrap(new BigDecimal[] {BigDecimal.valueOf(-1)}));
    }

    @Test
    void customNull() throws IOException {
        parse(BigDecimalValue.builder().onNull(BigDecimal.valueOf(-2)).build(), "null",
                ObjectChunk.chunkWrap(new BigDecimal[] {BigDecimal.valueOf(-2)}));
    }

    @Test
    void strict() throws IOException {
        parse(BigDecimalValue.strict(), "42.42", ObjectChunk.chunkWrap(new BigDecimal[] {new BigDecimal("42.42")}));
    }

    @Test
    void strictMissing() {
        try {
            process(BigDecimalValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(BigDecimalValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardString() {
        try {
            process(BigDecimalValue.standard(), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(BigDecimalValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(BigDecimalValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardObject() {
        try {
            process(BigDecimalValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(BigDecimalValue.standard(), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(BigDecimalValue.lenient(), List.of("\"42.42\"", "\"43.999\""),
                ObjectChunk.chunkWrap(new BigDecimal[] {new BigDecimal("42.42"), new BigDecimal("43.999")}));
    }
}
