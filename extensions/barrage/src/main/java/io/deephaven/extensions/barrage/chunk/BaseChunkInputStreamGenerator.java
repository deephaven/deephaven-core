/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.util.pools.PoolableChunk;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class BaseChunkInputStreamGenerator<T extends Chunk<Values>> implements ChunkInputStreamGenerator {
    public static final byte[] PADDING_BUFFER = new byte[8];
    public static final int REMAINDER_MOD_8_MASK = 0x7;

    // Ensure that we clean up chunk only after all copies of the update are released.
    private volatile int refCount = 1;

    // Field updater for refCount, so we can avoid creating an {@link java.util.concurrent.atomic.AtomicInteger} for each instance.
    @SuppressWarnings("rawtypes")
    protected static final AtomicIntegerFieldUpdater<BaseChunkInputStreamGenerator> REFERENCE_COUNT_UPDATER
            = AtomicIntegerFieldUpdater.newUpdater(BaseChunkInputStreamGenerator.class, "refCount");

    protected final T chunk;
    protected final int elementSize;

    private final long rowOffset;

    BaseChunkInputStreamGenerator(final T chunk, final int elementSize, final long rowOffset) {
        this.chunk = chunk;
        this.elementSize = elementSize;
        this.rowOffset = rowOffset;
    }

    @Override
    public long getRowOffset() {
        return rowOffset;
    }

    @Override
    public long getLastRowOffset() {
        return rowOffset + chunk.size() - 1;
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT_UPDATER.decrementAndGet(this) == 0) {
            if (chunk instanceof PoolableChunk) {
                ((PoolableChunk) chunk).close();
            }
        }
    }

    /**
     * Returns expected size of validity map in bytes.
     *
     * @param numElements the number of rows
     * @return number of bytes to represent the validity buffer for numElements
     */
    protected static int getValidityMapSerializationSizeFor(final int numElements) {
        return ((numElements + 63) / 64) * 8;
    }

    abstract class BaseChunkInputStream extends DrainableColumn {
        protected final StreamReaderOptions options;
        protected final RowSequence subset;
        protected boolean read = false;

        BaseChunkInputStream(final T chunk, final StreamReaderOptions options, final RowSet subset) {
            this.options = options;
            this.subset = chunk.size() == 0 ? RowSequenceFactory.EMPTY : subset != null ? subset.copy() : RowSequenceFactory.forRange(0, chunk.size() - 1);
            REFERENCE_COUNT_UPDATER.incrementAndGet(BaseChunkInputStreamGenerator.this);
            // ignore the empty chunk as these are intentionally empty generators that should work for any subset
            if (chunk.size() > 0 && this.subset.lastRowKey() >= chunk.size()) {
                throw new IllegalStateException(
                        "Subset " + this.subset + " is out of bounds for chunk of size " + chunk.size());
            }
        }

        @Override
        public void close() throws IOException {
            BaseChunkInputStreamGenerator.this.close();
            subset.close();
        }

        protected int getRawSize() throws IOException {
            long size = 0;
            if (sendValidityBuffer()) {
                size += getValidityMapSerializationSizeFor(subset.intSize());
            }
            size += elementSize * subset.size();
            return LongSizedDataStructure.intSize("BaseChunkInputStream.getRawSize", size);
        }

        @Override
        public int available() throws IOException {
            final int rawSize = getRawSize();
            final int rawMod8 = rawSize & REMAINDER_MOD_8_MASK;
            return (read ? 0 : rawSize + (rawMod8 > 0 ? 8 - rawMod8 : 0));
        }

        /**
         * There are two cases we don't send a validity buffer:
         * - the simplest case is following the arrow flight spec, which says that if there are no nulls present,
         *   the buffer is optional.
         * - Our implementation of nullCount() for primitive types will return zero if the useDeephavenNulls flag is
         *   set, so the buffer will also be omitted in that case. The client's marshaller does not need to be aware of
         *   deephaven nulls but in this mode we assume the consumer understands which value is the assigned NULL.
         */
        protected boolean sendValidityBuffer() {
            return nullCount() != 0;
        }
    }

    protected static final class SerContext {
        long accumulator = 0;
        long count = 0;
    }
}
