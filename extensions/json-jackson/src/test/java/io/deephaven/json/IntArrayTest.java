//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

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

    @Test
    void innerStrict() throws IOException {
        parse(ArrayValue.standard(IntValue.strict()), List.of("", "null", "[42, 43]"),
                ObjectChunk.chunkWrap(new Object[] {null, null, new int[] {42, 43}}));
        try {
            process(ArrayValue.standard(IntValue.strict()), "[42, null]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed for IntValue");
        }
    }

    @Test
    void arrayStrict() throws IOException {
        parse(ArrayValue.strict(IntValue.standard()), "[42, null]",
                ObjectChunk.chunkWrap(new Object[] {new int[] {42, QueryConstants.NULL_INT}}));
        try {
            process(ArrayValue.strict(IntValue.standard()), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed for ArrayValue");
        }
        try {
            process(ArrayValue.strict(IntValue.standard()), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed for ArrayValue");
        }
    }

    @Test
    void doubleNestedArrayStrict() throws IOException {
        parse(ArrayValue.strict(ArrayValue.standard(IntValue.standard())), "[null, [], [42, null]]", ObjectChunk
                .chunkWrap(new Object[] {new int[][] {null, new int[] {}, new int[] {42, QueryConstants.NULL_INT}}}));
        try {
            process(ArrayValue.strict(ArrayValue.standard(IntValue.standard())), "");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Missing not allowed for ArrayValue");
        }
        try {
            process(ArrayValue.strict(ArrayValue.standard(IntValue.standard())), "null");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed for ArrayValue");
        }
    }

    @Test
    void doubleNestedInnerArrayStrict() throws IOException {
        parse(ArrayValue.standard(ArrayValue.strict(IntValue.standard())), List.of("", "null", "[[], [42, null]]"),
                ObjectChunk.chunkWrap(new Object[] {null, null,
                        new int[][] {new int[] {}, new int[] {42, QueryConstants.NULL_INT}}}));
        try {
            process(ArrayValue.standard(ArrayValue.strict(IntValue.standard())), "[[], [42, null], null]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Null not allowed for ArrayValue");
        }
    }
}
