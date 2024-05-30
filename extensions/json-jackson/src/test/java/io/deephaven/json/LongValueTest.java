//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.LongChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class LongValueTest {

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(LongValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.longType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.longType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(LongValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.longType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.longType().arrayType());
    }

    @Test
    void standard() throws IOException {
        parse(LongValue.standard(), List.of("42", "43"), LongChunk.chunkWrap(new long[] {42, 43}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(LongValue.standard(), "", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void standardNull() throws IOException {
        parse(LongValue.standard(), "null", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void customMissing() throws IOException {
        parse(LongValue.builder().onMissing(-1L).build(), "", LongChunk.chunkWrap(new long[] {-1}));
    }

    @Test
    void strict() throws IOException {
        parse(LongValue.strict(), "42", LongChunk.chunkWrap(new long[] {42}));
    }

    @Test
    void strictMissing() {
        try {
            process(LongValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(LongValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardUnderflow() {
        try {
            process(LongValue.standard(), BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE).toString());
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unable to process current value for LongValue");
            assertThat(e).hasCauseInstanceOf(IOException.class);
            assertThat(e.getCause()).hasMessageContaining(
                    "Numeric value (-9223372036854775809) out of range of long (-9223372036854775808 - 9223372036854775807)");
        }
    }

    @Test
    void standardOverflow() {
        try {
            process(LongValue.standard(), BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).toString());
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unable to process current value for LongValue");
            assertThat(e).hasCauseInstanceOf(IOException.class);
            assertThat(e.getCause()).hasMessageContaining(
                    "Numeric value (9223372036854775808) out of range of long (-9223372036854775808 - 9223372036854775807)");
        }
    }

    @Test
    void standardString() {
        try {
            process(LongValue.standard(), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(LongValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(LongValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(LongValue.standard(), "42.0");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not allowed");
        }
    }

    @Test
    void standardObject() {
        try {
            process(LongValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(LongValue.standard(), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(LongValue.lenient(), List.of("\"42\"", "\"43\""), LongChunk.chunkWrap(new long[] {42, 43}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(LongValue.builder().allowedTypes(JsonValueTypes.INT, JsonValueTypes.DECIMAL).build(),
                List.of("42.42", "43.999"),
                LongChunk.chunkWrap(new long[] {42, 43}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(LongValue.builder().allowedTypes(JsonValueTypes.numberLike()).build(),
                List.of("\"42.42\"", "\"43.999\""), LongChunk.chunkWrap(new long[] {42, 43}));
    }

    @Test
    void decimalStringLimitsNearMinValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(LongValue.builder().allowedTypes(JsonValueTypes.numberLike()).build(),
                    List.of(String.format("\"%d.0\"", Long.MIN_VALUE + i)),
                    LongChunk.chunkWrap(new long[] {Long.MIN_VALUE + i}));
        }
    }

    @Test
    void decimalStringLimitsNearMaxValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(LongValue.builder().allowedTypes(JsonValueTypes.numberLike()).build(),
                    List.of(String.format("\"%d.0\"", Long.MAX_VALUE - i)),
                    LongChunk.chunkWrap(new long[] {Long.MAX_VALUE - i}));
        }
    }
}
