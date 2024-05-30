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
import java.math.BigInteger;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class BigIntegerValueTest {

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(BigIntegerValue.standard(false));
        assertThat(provider.outputTypes()).containsExactly(Type.ofCustom(BigInteger.class));
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.ofCustom(BigInteger.class));
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(BigIntegerValue.standard(false).array());
        assertThat(provider.outputTypes()).containsExactly(Type.ofCustom(BigInteger.class).arrayType());
        assertThat(provider.stringProcessor().outputTypes())
                .containsExactly(Type.ofCustom(BigInteger.class).arrayType());
    }

    @Test
    void standard() throws IOException {
        parse(BigIntegerValue.standard(false), "42", ObjectChunk.chunkWrap(new BigInteger[] {BigInteger.valueOf(42)}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(BigIntegerValue.standard(false), "", ObjectChunk.chunkWrap(new BigInteger[] {null}));
    }

    @Test
    void standardNull() throws IOException {
        parse(BigIntegerValue.standard(false), "null", ObjectChunk.chunkWrap(new BigInteger[] {null}));
    }

    @Test
    void customMissing() throws IOException {
        parse(BigIntegerValue.builder().onMissing(BigInteger.valueOf(-1)).build(), "",
                ObjectChunk.chunkWrap(new BigInteger[] {BigInteger.valueOf(-1)}));
    }

    @Test
    void customNull() throws IOException {
        parse(BigIntegerValue.builder().onNull(BigInteger.valueOf(-2)).build(), "null",
                ObjectChunk.chunkWrap(new BigInteger[] {BigInteger.valueOf(-2)}));
    }

    @Test
    void strict() throws IOException {
        parse(BigIntegerValue.strict(false), "42", ObjectChunk.chunkWrap(new BigInteger[] {BigInteger.valueOf(42)}));
    }

    @Test
    void strictMissing() {
        try {
            process(BigIntegerValue.strict(false), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(BigIntegerValue.strict(false), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardString() {
        try {
            process(BigIntegerValue.standard(false), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(BigIntegerValue.standard(false), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(BigIntegerValue.standard(false), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(BigIntegerValue.standard(false), "42.42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not allowed");
        }
    }

    @Test
    void standardObject() {
        try {
            process(BigIntegerValue.standard(false), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(BigIntegerValue.standard(false), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(BigIntegerValue.lenient(false), List.of("\"42\"", "\"43\""),
                ObjectChunk.chunkWrap(new BigInteger[] {BigInteger.valueOf(42), BigInteger.valueOf(43)}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(BigIntegerValue.standard(true), List.of("42.42", "43.999"),
                ObjectChunk.chunkWrap(new BigInteger[] {BigInteger.valueOf(42), BigInteger.valueOf(43)}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(BigIntegerValue.lenient(true), List.of("\"42.42\"", "\"43.999\""),
                ObjectChunk.chunkWrap(new BigInteger[] {BigInteger.valueOf(42), BigInteger.valueOf(43)}));
    }
}
