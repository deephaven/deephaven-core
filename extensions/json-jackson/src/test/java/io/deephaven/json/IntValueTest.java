//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.IntChunk;
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

public class IntValueTest {

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(IntValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.intType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.intType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(IntValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.intType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.intType().arrayType());
    }

    @Test
    void standard() throws IOException {
        parse(IntValue.standard(), "42", IntChunk.chunkWrap(new int[] {42}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(IntValue.standard(), "", IntChunk.chunkWrap(new int[] {QueryConstants.NULL_INT}));
    }

    @Test
    void standardNull() throws IOException {
        parse(IntValue.standard(), "null", IntChunk.chunkWrap(new int[] {QueryConstants.NULL_INT}));
    }

    @Test
    void customMissing() throws IOException {
        parse(IntValue.builder().onMissing(-1).build(), "", IntChunk.chunkWrap(new int[] {-1}));
    }

    @Test
    void customNull() throws IOException {
        parse(IntValue.builder().onNull(-2).build(), "null", IntChunk.chunkWrap(new int[] {-2}));
    }

    @Test
    void strict() throws IOException {
        parse(IntValue.strict(), "42", IntChunk.chunkWrap(new int[] {42}));
    }

    @Test
    void strictMissing() {
        try {
            process(IntValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(IntValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardUnderflow() {
        try {
            process(IntValue.standard(), Long.toString(Integer.MIN_VALUE - 1L));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unable to process current value for IntValue");
            assertThat(e).hasCauseInstanceOf(IOException.class);
            assertThat(e.getCause()).hasMessageContaining(
                    "Numeric value (-2147483649) out of range of int (-2147483648 - 2147483647)");
        }
    }

    @Test
    void standardOverflow() {
        try {
            process(IntValue.standard(), Long.toString(Integer.MAX_VALUE + 1L));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unable to process current value for IntValue");
            assertThat(e).hasCauseInstanceOf(IOException.class);
            assertThat(e.getCause()).hasMessageContaining(
                    "Numeric value (2147483648) out of range of int (-2147483648 - 2147483647)");
        }
    }

    @Test
    void standardString() {
        try {
            process(IntValue.standard(), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(IntValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(IntValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(IntValue.standard(), "42.0");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not allowed");
        }
    }

    @Test
    void standardObject() {
        try {
            process(IntValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(IntValue.standard(), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(IntValue.lenient(), List.of("\"42\"", "\"43\""), IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(IntValue.builder()
                .allowedTypes(JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(), List.of("42.42", "43.999"), IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(IntValue.builder()
                .allowedTypes(JsonValueTypes.STRING, JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(),
                List.of("\"42.42\"", "\"43.999\""), IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void decimalStringLimitsNearMinValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(IntValue.builder().allowedTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Integer.MIN_VALUE + i)),
                    IntChunk.chunkWrap(new int[] {Integer.MIN_VALUE + i}));
        }
    }

    @Test
    void decimalStringLimitsNearMaxValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(IntValue.builder().allowedTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Integer.MAX_VALUE - i)),
                    IntChunk.chunkWrap(new int[] {Integer.MAX_VALUE - i}));
        }
    }
}
