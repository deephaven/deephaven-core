//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import com.fasterxml.jackson.core.exc.InputCoercionException;
import io.deephaven.chunk.LongChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class LongOptionsTest {

    @Test
    void standard() throws IOException {
        parse(LongOptions.standard(), List.of("42", "43"), LongChunk.chunkWrap(new long[] {42, 43}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(LongOptions.standard(), "", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void standardNull() throws IOException {
        parse(LongOptions.standard(), "null", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void customMissing() throws IOException {
        parse(LongOptions.builder().onMissing(-1L).build(), "", LongChunk.chunkWrap(new long[] {-1}));
    }

    @Test
    void strict() throws IOException {
        parse(LongOptions.strict(), "42", LongChunk.chunkWrap(new long[] {42}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(LongOptions.strict(), "", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(LongOptions.strict(), "null", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }

    @Test
    void strictOverflow() throws IOException {
        try {
            parse(LongOptions.strict(), "9223372036854775808", LongChunk.chunkWrap(new long[1]));
        } catch (InputCoercionException e) {
            assertThat(e).hasMessageContaining(
                    "Numeric value (9223372036854775808) out of range of long (-9223372036854775808 - 9223372036854775807)");
        }
    }

    @Test
    void standardString() throws IOException {
        try {
            parse(LongOptions.standard(), "\"42\"", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_STRING'");
        }
    }

    @Test
    void standardTrue() throws IOException {
        try {
            parse(LongOptions.standard(), "true", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_TRUE'");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(LongOptions.standard(), "false", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_FALSE'");
        }
    }

    @Test
    void standardFloat() throws IOException {
        try {
            parse(LongOptions.standard(), "42.0", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NUMBER_FLOAT'");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(LongOptions.standard(), "{}", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_OBJECT'");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(LongOptions.standard(), "[]", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_ARRAY'");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(LongOptions.lenient(), List.of("\"42\"", "\"43\""), LongChunk.chunkWrap(new long[] {42, 43}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(LongOptions.builder().allowedTypes(JsonValueTypes.INT, JsonValueTypes.DECIMAL).build(),
                List.of("42.42", "43.999"),
                LongChunk.chunkWrap(new long[] {42, 43}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(LongOptions.builder().allowedTypes(JsonValueTypes.NUMBER_LIKE).build(),
                List.of("\"42.42\"", "\"43.999\""), LongChunk.chunkWrap(new long[] {42, 43}));
    }

    @Test
    void decimalStringLimitsNearMinValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(LongOptions.builder().allowedTypes(JsonValueTypes.NUMBER_LIKE).build(),
                    List.of(String.format("\"%d.0\"", Long.MIN_VALUE + i)),
                    LongChunk.chunkWrap(new long[] {Long.MIN_VALUE + i}));
        }
    }

    @Test
    void decimalStringLimitsNearMaxValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(LongOptions.builder().allowedTypes(JsonValueTypes.NUMBER_LIKE).build(),
                    List.of(String.format("\"%d.0\"", Long.MAX_VALUE - i)),
                    LongChunk.chunkWrap(new long[] {Long.MAX_VALUE - i}));
        }
    }
}
