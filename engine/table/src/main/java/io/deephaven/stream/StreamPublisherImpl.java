/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.stream;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import org.jetbrains.annotations.NotNull;

import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class StreamPublisherImpl implements StreamPublisher {
    private static final int CHUNK_SIZE = 4096;
    private StreamConsumer streamConsumer;
    private WritableChunk[] chunks;
    private Supplier<WritableChunk[]> chunkFactory;
    private IntFunction<ChunkType> chunkTypeIntFunction;

    /**
     * You must set the chunk factory and consumer before allowing other threads or objects to interact with the
     * StreamPublisherImpl.
     *
     * @param chunkFactory a supplier of WritableChunks that is acceptable to our consumer
     * @param chunkTypeIntFunction a function from column index to ChunkType
     */
    public void setChunkFactory(Supplier<WritableChunk[]> chunkFactory, IntFunction<ChunkType> chunkTypeIntFunction) {
        if (this.chunkFactory != null) {
            throw new IllegalStateException("Can not reset the chunkFactory for a StreamPublisherImpl");
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
     * Run the provided LongSupplier under our lock, preventing flush from taking our chunks while filling them.
     *
     * @param fun the LongSupplier to run
     * @return the return of the execution of the provided LongSupplier.
     */
    public synchronized long doLocked(LongSupplier fun) {
        return fun.getAsLong();
    }

    /**
     * Create a StreamToTableAdapter from this StreamPublisher, {@link #register} the adapter rwith this publisher, and
     * set this publisher's chunk factory to create the appropriate chunks for the result table.
     * 
     * @param tableDefinition Table definition for destiniation table.
     * @param updateSourceRegistrar Update source registrar (e.g.
     *        {@link io.deephaven.engine.updategraph.UpdateGraphProcessor#DEFAULT}).
     * @param name The name of the StreamToTableAdapter.
     * @return A StreamToTableAdapter to which this StreamPublisher will publish data.
     */
    public StreamToTableAdapter createStreamToTableAdapter(TableDefinition tableDefinition,
            UpdateSourceRegistrar updateSourceRegistrar, String name) {
        final StreamToTableAdapter streamToTableAdapter =
                new StreamToTableAdapter(tableDefinition, this, updateSourceRegistrar, name);

        setChunkFactory(() -> streamToTableAdapter.makeChunksForDefinition(CHUNK_SIZE),
                streamToTableAdapter::chunkTypeForIndex);

        return streamToTableAdapter;
    }
}
