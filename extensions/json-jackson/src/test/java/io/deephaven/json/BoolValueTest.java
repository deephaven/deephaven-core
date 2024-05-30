//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class BoolValueTest {

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(BoolValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.booleanType().boxedType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.booleanType().boxedType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(BoolValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.booleanType().boxedType().arrayType());
        assertThat(provider.stringProcessor().outputTypes())
                .containsExactly(Type.booleanType().boxedType().arrayType());
    }

    @Test
    void standard() throws IOException {
        parse(BoolValue.standard(), List.of("true", "false"), ByteChunk
                .chunkWrap(new byte[] {BooleanUtils.TRUE_BOOLEAN_AS_BYTE, BooleanUtils.FALSE_BOOLEAN_AS_BYTE}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(BoolValue.standard(), "", ByteChunk.chunkWrap(new byte[] {BooleanUtils.NULL_BOOLEAN_AS_BYTE}));
    }

    @Test
    void standardNull() throws IOException {
        parse(BoolValue.standard(), "null", ByteChunk.chunkWrap(new byte[] {BooleanUtils.NULL_BOOLEAN_AS_BYTE}));
    }

    @Test
    void customMissing() throws IOException {
        parse(BoolValue.builder().onMissing(true).build(), "",
                ByteChunk.chunkWrap(new byte[] {BooleanUtils.TRUE_BOOLEAN_AS_BYTE}));
        parse(BoolValue.builder().onMissing(false).build(), "",
                ByteChunk.chunkWrap(new byte[] {BooleanUtils.FALSE_BOOLEAN_AS_BYTE}));
    }

    @Test
    void customNull() throws IOException {
        parse(BoolValue.builder().onNull(true).build(), "null",
                ByteChunk.chunkWrap(new byte[] {BooleanUtils.TRUE_BOOLEAN_AS_BYTE}));
        parse(BoolValue.builder().onNull(false).build(), "null",
                ByteChunk.chunkWrap(new byte[] {BooleanUtils.FALSE_BOOLEAN_AS_BYTE}));
    }

    @Test
    void strict() throws IOException {
        parse(BoolValue.strict(), List.of("true", "false"), ByteChunk
                .chunkWrap(new byte[] {BooleanUtils.TRUE_BOOLEAN_AS_BYTE, BooleanUtils.FALSE_BOOLEAN_AS_BYTE}));
    }

    @Test
    void strictMissing() {
        try {
            process(BoolValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(BoolValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardString() {
        try {
            process(BoolValue.standard(), "\"42\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not allowed");
        }
    }

    @Test
    void standardInt() {
        try {
            process(BoolValue.standard(), "42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Number int not expected");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(BoolValue.standard(), "42.0");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not expected");
        }
    }

    @Test
    void standardObject() {
        try {
            process(BoolValue.standard(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(BoolValue.standard(), "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    @Test
    void lenientString() throws IOException {
        parse(BoolValue.lenient(), List.of("\"true\"", "\"false\"", "\"null\""),
                ByteChunk.chunkWrap(new byte[] {BooleanUtils.TRUE_BOOLEAN_AS_BYTE, BooleanUtils.FALSE_BOOLEAN_AS_BYTE,
                        BooleanUtils.NULL_BOOLEAN_AS_BYTE}));
    }
}
