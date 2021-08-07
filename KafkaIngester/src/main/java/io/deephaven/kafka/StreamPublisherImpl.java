package io.deephaven.kafka;

import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.jetbrains.annotations.NotNull;

import java.util.function.IntFunction;
import java.util.function.Supplier;

public class StreamPublisherImpl implements StreamPublisher {
    private StreamConsumer streamConsumer;
    private WritableChunk[] chunks;
    private java.util.function.Supplier<WritableChunk[]> chunkFactory;
    private IntFunction<ChunkType> chunkTypeIntFunction;

    public ChunkType chunkType(int index) {
        return chunkTypeIntFunction.apply(index);
    }

    public void setChunkFactory(Supplier<WritableChunk[]> chunkFactory, IntFunction<ChunkType> chunkTypeIntFunction) {
        this.chunkFactory = chunkFactory;
        this.chunkTypeIntFunction = chunkTypeIntFunction;
    }

    public WritableChunk [] getChunks() {
        if (chunks == null) {
            chunks = chunkFactory.get();
        }
        return chunks;
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (streamConsumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        streamConsumer = consumer;
    }

    @Override
    public void flush() {
        if (chunks != null) {
            streamConsumer.accept(chunks);
            chunks = null;
        }
    }
}
