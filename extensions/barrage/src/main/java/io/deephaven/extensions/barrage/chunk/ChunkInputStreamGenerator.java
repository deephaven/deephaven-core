//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.annotations.GwtIncompatible;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ChunkType;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public interface ChunkInputStreamGenerator extends SafeCloseable {
    long MS_PER_DAY = 24 * 60 * 60 * 1000L;
    long MIN_LOCAL_DATE_VALUE = QueryConstants.MIN_LONG / MS_PER_DAY;
    long MAX_LOCAL_DATE_VALUE = QueryConstants.MAX_LONG / MS_PER_DAY;

    @GwtIncompatible
    @Deprecated
    static WritableChunk<Values> extractChunkFromInputStream(
            final StreamReaderOptions options,
            final ChunkType chunkType, final Class<?> type, final Class<?> componentType,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk, final int offset, final int totalRows) throws IOException {
        return extractChunkFromInputStream(options, 1, chunkType, type, componentType, fieldNodeIter, bufferInfoIter,
                is, outChunk, offset, totalRows);
    }

    @GwtIncompatible
    @Deprecated
    private static WritableChunk<Values> extractChunkFromInputStream(
            final StreamReaderOptions options,
            final int factor,
            final ChunkType chunkType, final Class<?> type, final Class<?> componentType,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk, final int outOffset, final int totalRows) throws IOException {
        return DefaultChunkReadingFactory.INSTANCE.extractChunkFromInputStream(options, factor,
                new ChunkReadingFactory.ChunkTypeInfo(chunkType, type, componentType, null))
                .read(fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
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
