//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ShortChunk;
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

public class ShortValueTest {

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(ShortValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.shortType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.shortType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(ShortValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.shortType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.shortType().arrayType());
    }

    @Test
    void standard() throws IOException {
        parse(ShortValue.standard(), "42", ShortChunk.chunkWrap(new short[] {42}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(ShortValue.standard(), "", ShortChunk.chunkWrap(new short[] {QueryConstants.NULL_SHORT}));
    }

    @Test
    void standardNull() throws IOException {
        parse(ShortValue.standard(), "null", ShortChunk.chunkWrap(new short[] {QueryConstants.NULL_SHORT}));
    }

    @Test
    void customMissing() throws IOException {
        parse(ShortValue.builder().onMissing((short) -1).build(), "", ShortChunk.chunkWrap(new short[] {-1}));
    }

    @Test
    void customNull() throws IOException {
        parse(ShortValue.builder().onNull((short) -2).build(), "null", ShortChunk.chunkWrap(new short[] {-2}));
    }

    @Test
    void strict() throws IOException {
        parse(ShortValue.strict(), "42", ShortChunk.chunkWrap(new short[] {42}));
    }

    @Test
    void strictMissing() {
        try {
            process(ShortValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(ShortValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardUnderflow() {
        try {
            process(ShortValue.standard(), Integer.toString(Short.MIN_VALUE - 1));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unable to process current value for ShortValue");
            assertThat(e).hasCauseInstanceOf(IOException.class);
            assertThat(e.getCause()).hasMessageContaining(
                    "Numeric value (-32769) out of range of Java short");
        }
    }

    @Test
    void standardOverflow() {
        try {
            process(ShortValue.standard(), Integer.toString(Short.MAX_VALUE + 1));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unable to process current value for ShortValue");
            assertThat(e).hasCauseInstanceOf(IOException.class);
            assertThat(e.getCause()).hasMessageContaining(
                    "Numeric value (32768) out of range of Java short");
        }
    }

    @Test
    void standardString() {
        try {
            process(ShortValue.standard(), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(ShortValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(ShortValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(ShortValue.standard(), "42.0");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not allowed");
        }
    }

    @Test
    void standardObject() {
        try {
            process(ShortValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(ShortValue.standard(), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(ShortValue.lenient(), List.of("\"42\"", "\"43\""), ShortChunk.chunkWrap(new short[] {42, 43}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(ShortValue.builder()
                .allowedTypes(JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(), List.of("42.42", "43.999"), ShortChunk.chunkWrap(new short[] {42, 43}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(ShortValue.builder()
                .allowedTypes(JsonValueTypes.STRING, JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(),
                List.of("\"42.42\"", "\"43.999\""), ShortChunk.chunkWrap(new short[] {42, 43}));
    }

    @Test
    void decimalStringLimitsNearMinValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(ShortValue.builder().allowedTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Short.MIN_VALUE + i)),
                    ShortChunk.chunkWrap(new short[] {(short) (Short.MIN_VALUE + i)}));
        }
    }

    @Test
    void decimalStringLimitsNearMaxValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(ShortValue.builder().allowedTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Short.MAX_VALUE - i)),
                    ShortChunk.chunkWrap(new short[] {(short) (Short.MAX_VALUE - i)}));
        }
    }
}
