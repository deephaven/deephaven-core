//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import com.fasterxml.jackson.core.exc.InputCoercionException;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ShortOptionsTest {

    @Test
    void standard() throws IOException {
        parse(ShortOptions.standard(), "42", ShortChunk.chunkWrap(new short[] {42}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(ShortOptions.standard(), "", ShortChunk.chunkWrap(new short[] {QueryConstants.NULL_SHORT}));
    }

    @Test
    void standardNull() throws IOException {
        parse(ShortOptions.standard(), "null", ShortChunk.chunkWrap(new short[] {QueryConstants.NULL_SHORT}));
    }

    @Test
    void customMissing() throws IOException {
        parse(ShortOptions.builder().onMissing((short) -1).build(), "", ShortChunk.chunkWrap(new short[] {-1}));
    }

    @Test
    void customNull() throws IOException {
        parse(ShortOptions.builder().onNull((short) -2).build(), "null", ShortChunk.chunkWrap(new short[] {-2}));
    }

    @Test
    void strict() throws IOException {
        parse(ShortOptions.strict(), "42", ShortChunk.chunkWrap(new short[] {42}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(ShortOptions.strict(), "", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(ShortOptions.strict(), "null", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }

    @Test
    void standardOverflow() throws IOException {
        try {
            parse(ShortOptions.standard(), "2147483648", ShortChunk.chunkWrap(new short[1]));
        } catch (InputCoercionException e) {
            assertThat(e).hasMessageContaining(
                    "Numeric value (2147483648) out of range of int (-2147483648 - 2147483647)");
        }
    }

    @Test
    void standardString() throws IOException {
        try {
            parse(ShortOptions.standard(), "\"42\"", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_STRING'");
        }
    }

    @Test
    void standardTrue() throws IOException {
        try {
            parse(ShortOptions.standard(), "true", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_TRUE'");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(ShortOptions.standard(), "false", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_FALSE'");
        }
    }

    @Test
    void standardFloat() throws IOException {
        try {
            parse(ShortOptions.standard(), "42.0", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NUMBER_FLOAT'");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(ShortOptions.standard(), "{}", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_OBJECT'");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(ShortOptions.standard(), "[]", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_ARRAY'");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(ShortOptions.lenient(), List.of("\"42\"", "\"43\""), ShortChunk.chunkWrap(new short[] {42, 43}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(ShortOptions.builder()
                .desiredTypes(JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(), List.of("42.42", "43.999"), ShortChunk.chunkWrap(new short[] {42, 43}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(ShortOptions.builder()
                .desiredTypes(JsonValueTypes.STRING, JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(),
                List.of("\"42.42\"", "\"43.999\""), ShortChunk.chunkWrap(new short[] {42, 43}));
    }

    @Test
    void decimalStringLimitsNearMinValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(ShortOptions.builder().desiredTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Short.MIN_VALUE + i)),
                    ShortChunk.chunkWrap(new short[] {(short) (Short.MIN_VALUE + i)}));
        }
    }

    @Test
    void decimalStringLimitsNearMaxValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(ShortOptions.builder().desiredTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Short.MAX_VALUE - i)),
                    ShortChunk.chunkWrap(new short[] {(short) (Short.MAX_VALUE - i)}));
        }
    }
}
