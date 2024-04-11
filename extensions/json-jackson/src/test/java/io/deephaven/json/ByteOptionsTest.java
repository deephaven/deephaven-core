//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import com.fasterxml.jackson.core.exc.InputCoercionException;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ByteOptionsTest {

    @Test
    void standard() throws IOException {
        parse(ByteOptions.standard(), "42", ByteChunk.chunkWrap(new byte[] {42}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(ByteOptions.standard(), "", ByteChunk.chunkWrap(new byte[] {QueryConstants.NULL_BYTE}));
    }

    @Test
    void standardNull() throws IOException {
        parse(ByteOptions.standard(), "null", ByteChunk.chunkWrap(new byte[] {QueryConstants.NULL_BYTE}));
    }

    @Test
    void customMissing() throws IOException {
        parse(ByteOptions.builder().onMissing((byte) -1).build(), "", ByteChunk.chunkWrap(new byte[] {-1}));
    }

    @Test
    void customNull() throws IOException {
        parse(ByteOptions.builder().onNull((byte) -2).build(), "null", ByteChunk.chunkWrap(new byte[] {-2}));
    }

    @Test
    void strict() throws IOException {
        parse(ByteOptions.strict(), "42", ByteChunk.chunkWrap(new byte[] {42}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(ByteOptions.strict(), "", ByteChunk.chunkWrap(new byte[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(ByteOptions.strict(), "null", ByteChunk.chunkWrap(new byte[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }

    @Test
    void standardOverflow() throws IOException {
        try {
            parse(ByteOptions.standard(), "2147483648", ByteChunk.chunkWrap(new byte[1]));
        } catch (InputCoercionException e) {
            assertThat(e).hasMessageContaining(
                    "Numeric value (2147483648) out of range of int (-2147483648 - 2147483647)");
        }
    }

    @Test
    void standardString() throws IOException {
        try {
            parse(ByteOptions.standard(), "\"42\"", ByteChunk.chunkWrap(new byte[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_STRING'");
        }
    }

    @Test
    void standardTrue() throws IOException {
        try {
            parse(ByteOptions.standard(), "true", ByteChunk.chunkWrap(new byte[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_TRUE'");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(ByteOptions.standard(), "false", ByteChunk.chunkWrap(new byte[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_FALSE'");
        }
    }

    @Test
    void standardFloat() throws IOException {
        try {
            parse(ByteOptions.standard(), "42.0", ByteChunk.chunkWrap(new byte[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NUMBER_FLOAT'");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(ByteOptions.standard(), "{}", ByteChunk.chunkWrap(new byte[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_OBJECT'");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(ByteOptions.standard(), "[]", ByteChunk.chunkWrap(new byte[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_ARRAY'");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(ByteOptions.lenient(), List.of("\"42\"", "\"43\""), ByteChunk.chunkWrap(new byte[] {42, 43}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(ByteOptions.builder()
                .allowedTypes(JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(), List.of("42.42", "43.999"), ByteChunk.chunkWrap(new byte[] {42, 43}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(ByteOptions.builder()
                .allowedTypes(JsonValueTypes.STRING, JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(),
                List.of("\"42.42\"", "\"43.999\""), ByteChunk.chunkWrap(new byte[] {42, 43}));
    }

    @Test
    void decimalStringLimitsNearMinValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(ByteOptions.builder().allowedTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Byte.MIN_VALUE + i)),
                    ByteChunk.chunkWrap(new byte[] {(byte) (Byte.MIN_VALUE + i)}));
        }
    }

    @Test
    void decimalStringLimitsNearMaxValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(ByteOptions.builder().allowedTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Byte.MAX_VALUE - i)),
                    ByteChunk.chunkWrap(new byte[] {(byte) (Byte.MAX_VALUE - i)}));
        }
    }
}
