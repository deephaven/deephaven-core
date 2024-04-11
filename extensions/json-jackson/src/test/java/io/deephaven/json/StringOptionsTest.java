//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class StringOptionsTest {

    @Test
    void standard() throws IOException {
        parse(StringOptions.standard(), "\"foo\"", ObjectChunk.chunkWrap(new String[] {"foo"}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(StringOptions.standard(), "", ObjectChunk.chunkWrap(new String[] {null}));
    }

    @Test
    void standardNull() throws IOException {
        parse(StringOptions.standard(), "null", ObjectChunk.chunkWrap(new String[] {null}));
    }


    @Test
    void customMissing() throws IOException {
        parse(StringOptions.builder().onMissing("<missing>").build(), "",
                ObjectChunk.chunkWrap(new String[] {"<missing>"}));
    }

    @Test
    void customNull() throws IOException {
        parse(StringOptions.builder().onNull("<null>").build(), "null", ObjectChunk.chunkWrap(new String[] {"<null>"}));
    }

    @Test
    void strict() throws IOException {
        parse(StringOptions.strict(), "\"foo\"", ObjectChunk.chunkWrap(new String[] {"foo"}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(StringOptions.strict(), "", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(StringOptions.strict(), "null", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }


    @Test
    void standardInt() throws IOException {
        try {
            parse(StringOptions.standard(), "42", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NUMBER_INT'");
        }
    }

    @Test
    void standardFloat() throws IOException {
        try {
            parse(StringOptions.standard(), "42.42", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NUMBER_FLOAT'");
        }
    }


    @Test
    void standardTrue() throws IOException {
        try {
            parse(StringOptions.standard(), "true", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_TRUE'");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(StringOptions.standard(), "false", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_FALSE'");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(StringOptions.standard(), "{}", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_OBJECT'");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(StringOptions.standard(), "[]", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_ARRAY'");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(StringOptions.lenient(), List.of("42", "42.42", "true", "false"),
                ObjectChunk.chunkWrap(new String[] {"42", "42.42", "true", "false"}));
    }
}
