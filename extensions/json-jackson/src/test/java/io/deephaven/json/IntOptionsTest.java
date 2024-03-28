//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class IntOptionsTest {

    @Test
    void standard() throws IOException {
        parse(IntOptions.standard(), "42", IntChunk.chunkWrap(new int[] {42}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(IntOptions.standard(), "", IntChunk.chunkWrap(new int[] {QueryConstants.NULL_INT}));
    }

    @Test
    void standardNull() throws IOException {
        parse(IntOptions.standard(), "null", IntChunk.chunkWrap(new int[] {QueryConstants.NULL_INT}));
    }

    @Test
    void customMissing() throws IOException {
        parse(IntOptions.builder().onMissing(-1).build(), "", IntChunk.chunkWrap(new int[] {-1}));
    }

    @Test
    void customNull() throws IOException {
        parse(IntOptions.builder().onNull(-2).build(), "null", IntChunk.chunkWrap(new int[] {-2}));
    }

    @Test
    void strict() throws IOException {
        parse(IntOptions.strict(), "42", IntChunk.chunkWrap(new int[] {42}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(IntOptions.strict(), "", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(IntOptions.strict(), "null", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }

    @Test
    void standardOverflow() throws IOException {
        try {
            parse(IntOptions.standard(), "2147483648", IntChunk.chunkWrap(new int[1]));
        } catch (InputCoercionException e) {
            assertThat(e).hasMessageContaining(
                    "Numeric value (2147483648) out of range of int (-2147483648 - 2147483647)");
        }
    }

    @Test
    void standardString() throws IOException {
        try {
            parse(IntOptions.standard(), "\"42\"", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_STRING'");
        }
    }

    @Test
    void standardTrue() throws IOException {
        try {
            parse(IntOptions.standard(), "true", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_TRUE'");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(IntOptions.standard(), "false", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_FALSE'");
        }
    }

    @Test
    void standardFloat() throws IOException {
        try {
            parse(IntOptions.standard(), "42.0", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NUMBER_FLOAT'");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(IntOptions.standard(), "{}", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_OBJECT'");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(IntOptions.standard(), "[]", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_ARRAY'");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(IntOptions.lenient(), List.of("\"42\"", "\"43\""), IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void allowDecimal() throws IOException {
        parse(IntOptions.builder()
                .desiredTypes(JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(), List.of("42.42", "43.999"), IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void allowDecimalString() throws IOException {
        parse(IntOptions.builder()
                .desiredTypes(JsonValueTypes.STRING, JsonValueTypes.INT, JsonValueTypes.DECIMAL)
                .build(),
                List.of("\"42.42\"", "\"43.999\""), IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void decimalStringLimitsNearMinValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(IntOptions.builder().desiredTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Integer.MIN_VALUE + i)),
                    IntChunk.chunkWrap(new int[] {Integer.MIN_VALUE + i}));
        }
    }

    @Test
    void decimalStringLimitsNearMaxValue() throws IOException {
        for (int i = 0; i < 100; ++i) {
            parse(IntOptions.builder().desiredTypes(JsonValueTypes.STRING, JsonValueTypes.DECIMAL, JsonValueTypes.INT)
                    .build(),
                    List.of(String.format("\"%d.0\"", Integer.MAX_VALUE - i)),
                    IntChunk.chunkWrap(new int[] {Integer.MAX_VALUE - i}));
        }
    }
}
