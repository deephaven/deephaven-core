//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.chunk.util.hashing.ObjectChunkDeepEquals;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.processor.ObjectProcessor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHelper {
    public static void parse(Value options, String json, Chunk<?>... expected) throws IOException {
        parse(options, List.of(json), expected);
    }

    public static void parse(Value options, List<String> jsonRows, Chunk<?>... expectedCols) throws IOException {
        parse(JacksonProvider.of(options).stringProcessor(), jsonRows, expectedCols);
    }

    public static <T> void parse(ObjectProcessor<? super T> processor, List<T> rows, Chunk<?>... expectedCols)
            throws IOException {
        final List<WritableChunk<?>> out = process(processor, rows);
        try {
            assertThat(out.size()).isEqualTo(expectedCols.length);
            assertThat(out.stream().map(Chunk::getChunkType).collect(Collectors.toList()))
                    .isEqualTo(Stream.of(expectedCols).map(Chunk::getChunkType).collect(Collectors.toList()));
            for (int i = 0; i < expectedCols.length; ++i) {
                check(out.get(i), expectedCols[i]);
            }
        } finally {
            for (WritableChunk<?> wc : out) {
                wc.close();
            }
        }
    }

    public static List<WritableChunk<?>> process(Value options, String jsonRows) throws IOException {
        return process(JacksonProvider.of(options).stringProcessor(), List.of(jsonRows));
    }


    public static <T> List<WritableChunk<?>> process(ObjectProcessor<? super T> processor, List<T> rows)
            throws IOException {
        final List<WritableChunk<?>> out = processor
                .outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(x -> x.makeWritableChunk(rows.size()))
                .collect(Collectors.toList());
        try {
            for (WritableChunk<?> wc : out) {
                wc.setSize(0);
            }
            try (final WritableObjectChunk<T, Any> in = WritableObjectChunk.makeWritableChunk(rows.size())) {
                int i = 0;
                for (T input : rows) {
                    in.set(i, input);
                    ++i;
                }
                try {
                    processor.processAll(in, out);
                } catch (UncheckedIOException e) {
                    throw e.getCause();
                }
                return out;
            }
        } catch (IOException | RuntimeException e) {
            for (WritableChunk<?> wc : out) {
                wc.close();
            }
            throw e;
        }
    }

    static void check(Chunk<?> actual, Chunk<?> expected) {
        assertThat(actual.getChunkType()).isEqualTo(expected.getChunkType());
        assertThat(actual.size()).isEqualTo(expected.size());
        assertThat(getChunkEquals(actual).equalReduce(actual, expected)).isTrue();
    }

    private static ChunkEquals getChunkEquals(Chunk<?> actual) {
        return actual.getChunkType() == ChunkType.Object
                ? ObjectChunkDeepEquals.INSTANCE
                : ChunkEquals.makeEqual(actual.getChunkType());
    }
}
