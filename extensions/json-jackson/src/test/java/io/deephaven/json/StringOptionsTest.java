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
        parse(StringValue.standard(), "\"foo\"", ObjectChunk.chunkWrap(new String[] {"foo"}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(StringValue.standard(), "", ObjectChunk.chunkWrap(new String[] {null}));
    }

    @Test
    void standardNull() throws IOException {
        parse(StringValue.standard(), "null", ObjectChunk.chunkWrap(new String[] {null}));
    }


    @Test
    void customMissing() throws IOException {
        parse(StringValue.builder().onMissing("<missing>").build(), "",
                ObjectChunk.chunkWrap(new String[] {"<missing>"}));
    }

    @Test
    void customNull() throws IOException {
        parse(StringValue.builder().onNull("<null>").build(), "null", ObjectChunk.chunkWrap(new String[] {"<null>"}));
    }

    @Test
    void strict() throws IOException {
        parse(StringValue.strict(), "\"foo\"", ObjectChunk.chunkWrap(new String[] {"foo"}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(StringValue.strict(), "", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(StringValue.strict(), "null", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }


    @Test
    void standardInt() throws IOException {
        try {
            parse(StringValue.standard(), "42", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Number int not allowed");
        }
    }

    @Test
    void standardFloat() throws IOException {
        try {
            parse(StringValue.standard(), "42.42", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not allowed");
        }
    }


    @Test
    void standardTrue() throws IOException {
        try {
            parse(StringValue.standard(), "true", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(StringValue.standard(), "false", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(StringValue.standard(), "{}", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(StringValue.standard(), "[]", ObjectChunk.chunkWrap(new String[1]));
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(StringValue.lenient(), List.of("42", "42.42", "true", "false"),
                ObjectChunk.chunkWrap(new String[] {"42", "42.42", "true", "false"}));
    }
}
