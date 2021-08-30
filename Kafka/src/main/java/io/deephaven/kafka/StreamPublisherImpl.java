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
    private Supplier<WritableChunk[]> chunkFactory;
    private IntFunction<ChunkType> chunkTypeIntFunction;

    /**
     * You must set the chunk factory and consumer before allowing other threads or objects to
     * interact with the StreamPublisherImpl.
     *
     * @param chunkFactory a supplier of WritableChunks that is acceptable to our consumer
     * @param chunkTypeIntFunction a function from column index to ChunkType
     */
    public void setChunkFactory(Supplier<WritableChunk[]> chunkFactory,
        IntFunction<ChunkType> chunkTypeIntFunction) {
        if (this.chunkFactory != null) {
            throw new IllegalStateException(
                "Can not reset the chunkFactory for a StreamPublisherImpl");
        }
        this.chunkFactory = chunkFactory;
        this.chunkTypeIntFunction = chunkTypeIntFunction;
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (streamConsumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        streamConsumer = consumer;
    }

    public ChunkType chunkType(int index) {
        return chunkTypeIntFunction.apply(index);
    }

    public synchronized WritableChunk[] getChunks() {
        if (chunks == null) {
            chunks = chunkFactory.get();
        }
        return chunks;
    }


    @Override
    public synchronized void flush() {
        if (chunks != null) {
            streamConsumer.accept(chunks);
            chunks = null;
        }
    }

    /**
     * Run the provided Runnable under our lock, preventing flush from taking our chunks while
     * filling them.
     *
     * @param runnable the runnable to run
     */
    public synchronized void doLocked(Runnable runnable) {
        runnable.run();
    }
}
