package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.utils.ChunkUtils;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ChunkStream {
    public static DoubleStream of(DoubleChunk<? extends Attributes.Any> chunk, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }

    public static IntStream of(IntChunk<? extends Attributes.Any> chunk, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }

    public static LongStream of(LongChunk<? extends Attributes.Any> chunk, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(chunk.size, offset, capacity);
        return Arrays.stream(chunk.data, chunk.offset + offset, chunk.offset + offset + capacity);
    }
}
