//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ByteValueTest {

    @Test
    void standard() throws IOException {
        parse(ByteValue.standard(), "42", ByteChunk.chunkWrap(new byte[] {42}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(ByteValue.standard(), "", ByteChunk.chunkWrap(new byte[] {QueryConstants.NULL_BYTE}));
    }

    @Test
    void standardNull() throws IOException {
        parse(ByteValue.standard(), "null", ByteChunk.chunkWrap(new byte[] {QueryConstants.NULL_BYTE}));
    }

    @Test
    void customMissing() throws IOException {
        parse(ByteValue.builder().onMissing((byte) -1).build(), "", ByteChunk.chunkWrap(new byte[] {-1}));
    }

    @Test
    void customNull() throws IOException {
        parse(ByteValue.builder().onNull((byte) -2).build(), "null", ByteChunk.chunkWrap(new byte[] {-2}));
    }

    @Test
    void strict() throws IOException {
        parse(ByteValue.strict(), "42", ByteChunk.chunkWrap(new byte[] {42}));
    }

    @Test
    void strictMissing() {
        try {
            process(ByteValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(ByteValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardOverflow() {
        try {
            process(ByteValue.standard(), "2147483648");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining(
                    "Numeric value (2147483648) out of range of int (-2147483648 - 2147483647)");
        }
    }

    @Test
    void standardString() {
        try {
            process(ByteValue.standard(), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(ByteValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(ByteValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(ByteValue.standard(), "42.0");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not allowed");
        }
    }

    @Test
    void standardObject() {
        try {
            process(ByteValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(ByteValue.standard(), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(ByteValue.lenient(), List.of("\"42\"", "\"43\""), ByteChunk.chunkWrap(new byte[] {42, 43}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(ByteValue.builder()
                .allowedTypes(JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(), List.of("42.42", "43.999"), ByteChunk.chunkWrap(new byte[] {42, 43}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(ByteValue.builder()
                .allowedTypes(JsonValueTypes.STRING, JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(),
                List.of("\"42.42\"", "\"43.999\""), ByteChunk.chunkWrap(new byte[] {42, 43}));
    }

    @Test
    void decimalStringLimitsNearMinValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(ByteValue.builder().allowedTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Byte.MIN_VALUE + i)),
                    ByteChunk.chunkWrap(new byte[] {(byte) (Byte.MIN_VALUE + i)}));
        }
    }

    @Test
    void decimalStringLimitsNearMaxValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(ByteValue.builder().allowedTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Byte.MAX_VALUE - i)),
                    ByteChunk.chunkWrap(new byte[] {(byte) (Byte.MAX_VALUE - i)}));
        }
    }
}
