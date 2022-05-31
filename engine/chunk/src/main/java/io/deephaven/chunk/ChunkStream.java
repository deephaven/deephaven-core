package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ChunkStream {
    public static DoubleStream of(DoubleChunk<? extends Any> chunk, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }

    public static IntStream of(IntChunk<? extends Any> chunk, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }

    public static LongStream of(LongChunk<? extends Any> chunk, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }
}
