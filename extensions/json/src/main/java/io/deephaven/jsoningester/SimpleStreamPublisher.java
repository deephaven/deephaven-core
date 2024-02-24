package io.deephaven.jsoningester;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.stream.StreamPublisherBase;

/**
 * A basic {@link StreamPublisherBase} implementation that exposes its table definition. Chunks can be retrieved with
 * {@link #getChunks()}. The remaining space in the chunks is available via {@link #getChunkRemainingSpace()}. When data
 * is written to the chunks, {@link #decrementRemainingSpace} must be called.
 */
public class SimpleStreamPublisher extends StreamPublisherBase {
    // TODO SimpleStreamPublisher is a bad name. It's not very simple, as the caller has to worry about
    // chunkRemainingSpace

    private final Runnable shutdownCallback;

    /**
     * Remaining space in the {@link #chunks}. This publisher must be {@link StreamPublisherBase#flush() flush()ed} when
     * the remaining space reaches zero.
     * <p>
     * This must only be accessed/modified under the lock.
     */
    private int chunkRemainingSpace = 0;

    public SimpleStreamPublisher(TableDefinition tableDefinition) {
        this(tableDefinition, () -> {
        });
    }

    public SimpleStreamPublisher(TableDefinition tableDefinition, Runnable shutdownCallback) {
        super(tableDefinition);
        this.shutdownCallback = shutdownCallback;
    }

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public synchronized void flush() {
        super.flush();
        chunkRemainingSpace = 0;
    }

    /**
     * Get writable chunks with at least 1 row of free capacity
     * 
     * @return The non-full, non-null chunks
     */
    synchronized WritableChunk<Values>[] getChunks() {
        if (chunkRemainingSpace > 0) {
            return chunks;
        }

        chunks = super.getChunksToFill(); // Note that this won't do anything unless streamPublisher.flush() was called
                                          // previously
        chunkRemainingSpace = chunks[0].capacity() - chunks[0].size();

        return chunks;
    }

    /**
     * Decrease the tracked amount of space available ({@link #chunkRemainingSpace} in the chunks.
     * 
     * @param decrementAmount How much to decrement by
     * @return The updated remaining free space in the chunks
     */
    synchronized int decrementRemainingSpace(final int decrementAmount) {
        Assert.gtZero(decrementAmount, "decrementAmount");
        chunkRemainingSpace -= decrementAmount;
        Assert.geqZero(chunkRemainingSpace, "chunkRemainingSpace");
        return chunkRemainingSpace;
    }

    synchronized int getChunkRemainingSpace() {
        return chunkRemainingSpace;
    }

    @Override
    public void shutdown() {
        shutdownCallback.run();
    }
}
