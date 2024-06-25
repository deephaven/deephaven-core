//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class StringValueTest {

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(StringValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.stringType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.stringType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(StringValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.stringType().arrayType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.stringType().arrayType());
    }

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
    void strictMissing() {
        try {
            process(StringValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(StringValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }


    @Test
    void standardInt() {
        try {
            process(StringValue.standard(), "42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Number int not allowed");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(StringValue.standard(), "42.42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not allowed");
        }
    }


    @Test
    void standardTrue() {
        try {
            process(StringValue.standard(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not allowed");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(StringValue.standard(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not allowed");
        }
    }

    @Test
    void standardObject() {
        try {
            process(StringValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(StringValue.standard(), "[]");
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
