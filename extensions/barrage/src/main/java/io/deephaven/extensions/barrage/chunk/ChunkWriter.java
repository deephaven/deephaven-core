//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.Chunk;
import io.deephaven.util.referencecounting.ReferenceCounted;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public interface ChunkWriter<SourceChunkType extends Chunk<Values>> {
    long MS_PER_DAY = 24 * 60 * 60 * 1000L;
    long NS_PER_MS = 1_000_000L;
    long NS_PER_DAY = MS_PER_DAY * NS_PER_MS;

    /**
     * Creator of {@link ChunkWriter} instances.
     * <p>
     * This API may not be stable, while the JS API's usages of it are implemented.
     */
    interface Factory {
        /**
         * Returns a {@link ChunkReader} for the specified arguments.
         *
         * @param typeInfo the type of data to read into a chunk
         * @return a ChunkReader based on the given options, factory, and type to read
         */
        <T extends Chunk<Values>> ChunkWriter<T> newWriter(
                @NotNull ChunkReader.TypeInfo typeInfo);
    }

    /**
     * Create a context for the given chunk.
     *
     * @param chunk the chunk of data to be written
     * @param rowOffset the number of rows that were sent before the first row in this logical message
     * @return a context for the given chunk
     */
    Context<SourceChunkType> makeContext(final SourceChunkType chunk, final long rowOffset);

    /**
     * Get an input stream optionally position-space filtered using the provided RowSet.
     *
     * @param context the chunk writer context holding the data to be drained to the client
     * @param subset if provided, is a position-space filter of source data
     * @param options options for reading the stream
     * @return a single-use DrainableColumn ready to be drained via grpc
     */
    DrainableColumn getInputStream(
            @NotNull Context<SourceChunkType> context,
            @Nullable RowSet subset,
            @NotNull ChunkReader.Options options) throws IOException;

    /**
     * Get an input stream representing the empty wire payload for this writer.
     *
     * @param options options for reading the stream
     * @return a single-use DrainableColumn ready to be drained via grpc
     */
    DrainableColumn getEmptyInputStream(
            @NotNull ChunkReader.Options options) throws IOException;

    class Context<T extends Chunk<Values>> extends ReferenceCounted implements SafeCloseable {
        private final T chunk;
        private final long rowOffset;

        public Context(final T chunk, final long rowOffset) {
            super(1);
            this.chunk = chunk;
            this.rowOffset = rowOffset;
        }

        /**
         * @return the chunk wrapped by this wrapper
         */
        T getChunk() {
            return chunk;
        }

        /**
         * @return the number of rows that were sent before the first row in this writer.
         */
        public long getRowOffset() {
            return rowOffset;
        }

        /**
         * @return the offset of the final row this writer can produce.
         */
        public long getLastRowOffset() {
            return rowOffset + chunk.size() - 1;
        }

        /**
         * @return the number of rows in the wrapped chunk
         */
        public int size() {
            return chunk.size();
        }

        @Override
        public void close() {
            decrementReferenceCount();
        }

        @Override
        protected void onReferenceCountAtZero() {
            if (chunk instanceof PoolableChunk) {
                ((PoolableChunk) chunk).close();
            }
        }
    }

    final class FieldNodeInfo {
        public final int numElements;
        public final int nullCount;

        public FieldNodeInfo(final int numElements, final int nullCount) {
            this.numElements = numElements;
            this.nullCount = nullCount;
        }

        public FieldNodeInfo(final org.apache.arrow.flatbuf.FieldNode node) {
            this(LongSizedDataStructure.intSize("FieldNodeInfo", node.length()),
                    LongSizedDataStructure.intSize("FieldNodeInfo", node.nullCount()));
        }
    }

    @FunctionalInterface
    interface FieldNodeListener {
        void noteLogicalFieldNode(final int numElements, final int nullCount);
    }

    @FunctionalInterface
    interface BufferListener {
        void noteLogicalBuffer(final long length);
    }

    abstract class DrainableColumn extends DefensiveDrainable {
        /**
         * Append the field nde to the flatbuffer payload via the supplied listener.
         * 
         * @param listener the listener to notify for each logical field node in this payload
         */
        public abstract void visitFieldNodes(final FieldNodeListener listener);

        /**
         * Append the buffer boundaries to the flatbuffer payload via the supplied listener.
         * 
         * @param listener the listener to notify for each sub-buffer in this payload
         */
        public abstract void visitBuffers(final BufferListener listener);

        /**
         * Count the number of null elements in the outer-most layer of this column (i.e. does not count nested nulls
         * inside of arrays)
         * 
         * @return the number of null elements -- 'useDeephavenNulls' counts are always 0 so that we may omit the
         *         validity buffer
         */
        public abstract int nullCount();
    }
}
