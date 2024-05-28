//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.CharChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class CharValueTest {

    @Test
    void standard() throws IOException {
        parse(CharValue.standard(), "\"c\"", CharChunk.chunkWrap(new char[] {'c'}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(CharValue.standard(), "", CharChunk.chunkWrap(new char[] {QueryConstants.NULL_CHAR}));
    }

    @Test
    void standardNull() throws IOException {
        parse(CharValue.standard(), "null", CharChunk.chunkWrap(new char[] {QueryConstants.NULL_CHAR}));
    }

    @Test
    void customMissing() throws IOException {
        parse(CharValue.builder().onMissing('m').build(), "", CharChunk.chunkWrap(new char[] {'m'}));
    }

    @Test
    void customNull() throws IOException {
        parse(CharValue.builder().onNull('n').build(), "null", CharChunk.chunkWrap(new char[] {'n'}));
    }

    @Test
    void strict() throws IOException {
        parse(CharValue.strict(), "\"c\"", CharChunk.chunkWrap(new char[] {'c'}));
    }

    @Test
    void strictMissing() {
        try {
            process(CharValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(CharValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }


    @Test
    void standardInt() {
        try {
            process(CharValue.standard(), "42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Number int not expected");
        }
    }

    @Test
    void standardFloat() {
        try {
            process(CharValue.standard(), "42.42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not expected");
        }
    }


    @Test
    void standardTrue() {
        try {
            process(CharValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(CharValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardObject() {
        try {
            process(CharValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(CharValue.standard(), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }
}
