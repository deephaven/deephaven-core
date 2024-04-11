//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.BooleanChunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.processor.ObjectProcessor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHelper {
    public static void parse(ValueOptions options, String json, Chunk<?>... expected) throws IOException {
        parse(options, List.of(json), expected);
    }

    public static void parse(ValueOptions options, List<String> jsonRows, Chunk<?>... expectedCols) throws IOException {
        parse(JacksonProvider.of(options).stringProcessor(), jsonRows, expectedCols);
    }

    public static <T> void parse(ObjectProcessor<? super T> processor, List<T> rows, Chunk<?>... expectedCols)
            throws IOException {
        final List<WritableChunk<?>> out = processor
                .outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(x -> x.makeWritableChunk(rows.size()))
                .collect(Collectors.toList());
        try {
            assertThat(out.size()).isEqualTo(expectedCols.length);
            assertThat(out.stream().map(Chunk::getChunkType).collect(Collectors.toList()))
                    .isEqualTo(Stream.of(expectedCols).map(Chunk::getChunkType).collect(Collectors.toList()));
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
            }
            for (int i = 0; i < expectedCols.length; ++i) {
                check(out.get(i), expectedCols[i]);
            }
        } finally {
            for (WritableChunk<?> wc : out) {
                wc.close();
            }
        }
    }

    static void check(Chunk<?> actual, Chunk<?> expected) {
        assertThat(actual.getChunkType()).isEqualTo(expected.getChunkType());
        assertThat(actual.size()).isEqualTo(expected.size());
        switch (actual.getChunkType()) {
            case Boolean:
                check(actual.asBooleanChunk(), expected.asBooleanChunk());
                break;
            case Char:
                check(actual.asCharChunk(), expected.asCharChunk());
                break;
            case Byte:
                check(actual.asByteChunk(), expected.asByteChunk());
                break;
            case Short:
                check(actual.asShortChunk(), expected.asShortChunk());
                break;
            case Int:
                check(actual.asIntChunk(), expected.asIntChunk());
                break;
            case Long:
                check(actual.asLongChunk(), expected.asLongChunk());
                break;
            case Float:
                check(actual.asFloatChunk(), expected.asFloatChunk());
                break;
            case Double:
                check(actual.asDoubleChunk(), expected.asDoubleChunk());
                break;
            case Object:
                check(actual.asObjectChunk(), expected.asObjectChunk());
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private static void check(BooleanChunk<?> actual, BooleanChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static void check(CharChunk<?> actual, CharChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static void check(ByteChunk<?> actual, ByteChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static void check(ShortChunk<?> actual, ShortChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static void check(IntChunk<?> actual, IntChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static void check(LongChunk<?> actual, LongChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static void check(FloatChunk<?> actual, FloatChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static void check(DoubleChunk<?> actual, DoubleChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static void check(ObjectChunk<?, ?> actual, ObjectChunk<?, ?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }
}
