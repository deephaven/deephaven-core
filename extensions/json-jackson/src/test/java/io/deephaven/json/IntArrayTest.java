//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;

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
