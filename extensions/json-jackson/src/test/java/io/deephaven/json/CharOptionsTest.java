//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.CharChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class CharOptionsTest {

    @Test
    void standard() throws IOException {
        parse(CharOptions.standard(), "\"c\"", CharChunk.chunkWrap(new char[] {'c'}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(CharOptions.standard(), "", CharChunk.chunkWrap(new char[] {QueryConstants.NULL_CHAR}));
    }

    @Test
    void standardNull() throws IOException {
        parse(CharOptions.standard(), "null", CharChunk.chunkWrap(new char[] {QueryConstants.NULL_CHAR}));
    }


    @Test
    void customMissing() throws IOException {
        parse(CharOptions.builder().onMissing('m').build(), "", CharChunk.chunkWrap(new char[] {'m'}));
    }

    @Test
    void customNull() throws IOException {
        parse(CharOptions.builder().onNull('n').build(), "null", CharChunk.chunkWrap(new char[] {'n'}));
    }

    @Test
    void strict() throws IOException {
        parse(CharOptions.strict(), "\"c\"", CharChunk.chunkWrap(new char[] {'c'}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(CharOptions.strict(), "", CharChunk.chunkWrap(new char[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected missing token");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(CharOptions.strict(), "null", CharChunk.chunkWrap(new char[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NULL'");
        }
    }


    @Test
    void standardInt() throws IOException {
        try {
            parse(CharOptions.standard(), "42", CharChunk.chunkWrap(new char[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NUMBER_INT'");
        }
    }

    @Test
    void standardFloat() throws IOException {
        try {
            parse(CharOptions.standard(), "42.42", CharChunk.chunkWrap(new char[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_NUMBER_FLOAT'");
        }
    }


    @Test
    void standardTrue() throws IOException {
        try {
            parse(CharOptions.standard(), "true", CharChunk.chunkWrap(new char[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_TRUE'");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(CharOptions.standard(), "false", CharChunk.chunkWrap(new char[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'VALUE_FALSE'");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(CharOptions.standard(), "{}", CharChunk.chunkWrap(new char[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_OBJECT'");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(CharOptions.standard(), "[]", CharChunk.chunkWrap(new char[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Unexpected token 'START_ARRAY'");
        }
    }
}
