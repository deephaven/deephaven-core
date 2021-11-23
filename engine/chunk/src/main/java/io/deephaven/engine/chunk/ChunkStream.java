package io.deephaven.engine.chunk;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ChunkStream {
    public static DoubleStream of(DoubleChunk<? extends Attributes.Any> chunk, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }

    public static IntStream of(IntChunk<? extends Attributes.Any> chunk, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }

    public static LongStream of(LongChunk<? extends Attributes.Any> chunk, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }
}
