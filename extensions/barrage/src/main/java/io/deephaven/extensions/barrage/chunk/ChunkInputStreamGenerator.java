//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public interface ChunkInputStreamGenerator extends SafeCloseable {
    long MS_PER_DAY = 24 * 60 * 60 * 1000L;
    long MIN_LOCAL_DATE_VALUE = QueryConstants.MIN_LONG / MS_PER_DAY;
    long MAX_LOCAL_DATE_VALUE = QueryConstants.MAX_LONG / MS_PER_DAY;

    /**
     * Creator of {@link ChunkInputStreamGenerator} instances.
     * <p>
     * This API may not be stable, while the JS API's usages of it are implemented.
     */
    interface Factory {
        /**
         * Returns an instance capable of writing the given chunk
         *
         * @param chunkType the type of the chunk to be written
         * @param type the Java type of the column being written
         * @param componentType the Java type of data in an array/vector, or null if irrelevant
         * @param chunk the chunk that will be written out to an input stream
         * @param rowOffset the offset into the chunk to start writing from
         * @return an instance capable of serializing the given chunk
         * @param <T> the type of data in the column
         */
        <T> ChunkInputStreamGenerator makeInputStreamGenerator(
                final ChunkType chunkType,
                final Class<T> type,
                final Class<?> componentType,
                final Chunk<Values> chunk,
                final long rowOffset);
    }

    /**
     * Returns the number of rows that were sent before the first row in this generator.
     */
    long getRowOffset();

    /**
     * Returns the offset of the final row this generator can produce.
     */
    long getLastRowOffset();

    /**
     * Get an input stream optionally position-space filtered using the provided RowSet.
     *
     * @param options the serializable options for this subscription
     * @param subset if provided, is a position-space filter of source data
     * @return a single-use DrainableColumn ready to be drained via grpc
     */
    DrainableColumn getInputStream(final StreamReaderOptions options, @Nullable final RowSet subset) throws IOException;

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
