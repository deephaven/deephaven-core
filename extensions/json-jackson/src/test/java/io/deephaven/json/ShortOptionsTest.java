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
    void strictMissing() throws IOException {
        try {
            parse(ShortValue.strict(), "", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(ShortValue.strict(), "null", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardOverflow() throws IOException {
        try {
            parse(ShortValue.standard(), "2147483648", ShortChunk.chunkWrap(new short[1]));
        } catch (InputCoercionException e) {
            assertThat(e).hasMessageContaining(
                    "Numeric value (2147483648) out of range of int (-2147483648 - 2147483647)");
        }
    }

    @Test
    void standardString() throws IOException {
        try {
            parse(ShortValue.standard(), "\"42\"", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardTrue() throws IOException {
        try {
            parse(ShortValue.standard(), "true", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(ShortValue.standard(), "false", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFloat() throws IOException {
        try {
            parse(ShortValue.standard(), "42.0", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not allowed");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(ShortValue.standard(), "{}", ShortChunk.chunkWrap(new short[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(ShortValue.standard(), "[]", ShortChunk.chunkWrap(new short[1]));
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
