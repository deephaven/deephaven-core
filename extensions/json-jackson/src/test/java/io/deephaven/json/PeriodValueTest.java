//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Period;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class PeriodValueTest {

    private static final String XYZ_STR = "P1Y2M3D";

    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(PeriodValue.standard());
        assertThat(provider.outputTypes()).containsExactly(Type.periodType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.periodType());
    }

    @Test
    void arrayProvider() {
        final JacksonProvider provider = JacksonProvider.of(PeriodValue.standard().array());
        assertThat(provider.outputTypes()).containsExactly(Type.periodType().arrayType());
        assertThat(provider.stringProcessor().outputTypes())
                .containsExactly(Type.periodType().arrayType());
    }

    @Test
    void iso8601() throws IOException {
        parse(PeriodValue.standard(), "\"" + XYZ_STR + "\"",
                ObjectChunk.chunkWrap(new Period[] {Period.of(1, 2, 3)}));
    }

    @Test
    void standardNull() throws IOException {
        parse(PeriodValue.standard(), "null", ObjectChunk.chunkWrap(new Period[] {null}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(PeriodValue.standard(), "", ObjectChunk.chunkWrap(new Period[] {null}));
    }

    @Test
    void strictNull() {
        try {
            process(PeriodValue.strict(), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void strictMissing() {
        try {
            process(PeriodValue.strict(), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void customNull() throws IOException {
        parse(PeriodValue.builder().onNull(Period.ZERO).build(), "null",
                ObjectChunk.chunkWrap(new Period[] {Period.ZERO}));
    }

    @Test
    void customMissing() throws IOException {
        parse(PeriodValue.builder().onMissing(Period.ZERO).build(), "",
                ObjectChunk.chunkWrap(new Period[] {Period.ZERO}));
    }
}
