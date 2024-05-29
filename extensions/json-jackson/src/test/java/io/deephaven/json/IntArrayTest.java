//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class IntArrayTest {

    @Test
    void standard() throws IOException {
        parse(IntValue.standard().array(), "[42, 43]", ObjectChunk.chunkWrap(new Object[] {new int[] {42, 43}}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(IntValue.standard().array(), "", ObjectChunk.chunkWrap(new Object[] {null}));
    }

    @Test
    void standardNull() throws IOException {
        parse(IntValue.standard().array(), "null", ObjectChunk.chunkWrap(new Object[] {null}));
    }

    @Test
    void strictMissing() {
        try {
            process(ArrayValue.strict(IntValue.standard()), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed");
        }
    }

    @Test
    void strictNull() {
        try {
            process(ArrayValue.strict(IntValue.standard()), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed");
        }
    }

    @Test
    void standardInt() {
        try {
            process(IntValue.standard().array(), "42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Number int not expected");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(IntValue.standard().array(), "42.42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not expected");
        }
    }

    @Test
    void standardString() {
        try {
            process(IntValue.standard().array(), "\"hello\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not expected");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(IntValue.standard().array(), "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(IntValue.standard().array(), "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardObject() {
        try {
            process(IntValue.standard().array(), "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Object not expected");
        }
    }

    @Test
    void doubleNestedArray() throws IOException {
        parse(IntValue.standard().array().array(), "[null, [], [42, 43]]",
                ObjectChunk.chunkWrap(new Object[] {new int[][] {
                        null,
                        new int[] {},
                        new int[] {42, 43}}}));
    }

    @Test
    void tripleNestedArray() throws IOException {
        parse(IntValue.standard().array().array().array(), "[null, [], [null, [], [42, 43]]]",
                ObjectChunk.chunkWrap(new Object[] {new int[][][] {
                        null,
                        new int[][] {},
                        new int[][] {
                                null,
                                new int[] {},
                                new int[] {42, 43}}
                }}));
    }

    @Test
    void quadNestedArray() throws IOException {
        parse(IntValue.standard().array().array().array().array(), "[null, [], [null, [], [null, [], [42, 43]]]]",
                ObjectChunk.chunkWrap(new Object[] {new int[][][][] {
                        null,
                        new int[][][] {},
                        new int[][][] {
                                null,
                                new int[][] {},
                                new int[][] {
                                        null,
                                        new int[] {},
                                        new int[] {42, 43}}
                        }}}));
    }
}
