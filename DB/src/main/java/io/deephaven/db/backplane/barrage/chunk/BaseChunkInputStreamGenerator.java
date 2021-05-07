/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.backplane.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class BaseChunkInputStreamGenerator<T extends WritableChunk<Attributes.Values>> implements ChunkInputStreamGenerator {
    public static final byte[] PADDING_BUFFER = new byte[8];
    public static final int REMAINDER_MOD_8_MASK = 0x7;

    // Ensure that we clean up chunk only after all copies of the update are released.
    private volatile int refCount = 1;

    // Field updater for refCount, so we can avoid creating an {@link java.util.concurrent.atomic.AtomicInteger} for each instance.
    @SuppressWarnings("rawtypes")
    protected static final AtomicIntegerFieldUpdater<BaseChunkInputStreamGenerator> REFERENCE_COUNT_UPDATER
            = AtomicIntegerFieldUpdater.newUpdater(BaseChunkInputStreamGenerator.class, "refCount");

    protected final int elementSize;

    protected final T chunk;

    BaseChunkInputStreamGenerator(final T chunk, final int elementSize) {
        this.chunk = chunk;
        this.elementSize = elementSize;
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT_UPDATER.decrementAndGet(this) == 0) {
            chunk.close();
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

    /**
     * Returns expected size of serializing numElements with fixed size elementSize and a validity map.
     * @param elementSize size of one element
     * @param numElements the number of rows
     * @return number of bytes to represent the validity buffer and data for numElements
     */
    protected static int getSerializationSizeFor(final Options options, final int elementSize, final int numElements) {
        return elementSize * numElements + (options.useDeephavenNulls ? 0 : getValidityMapSerializationSizeFor(numElements));
    }

    abstract class BaseChunkInputStream extends DrainableColumn {
        protected final Options options;
        protected final OrderedKeys subset;
        protected boolean read = false;

        BaseChunkInputStream(final T chunk, final Options options, final Index subset) {
            this.options = options;
            this.subset = subset != null ? subset.clone() : chunk.size() == 0 ? OrderedKeys.EMPTY : OrderedKeys.forRange(0, chunk.size() - 1);
            REFERENCE_COUNT_UPDATER.incrementAndGet(BaseChunkInputStreamGenerator.this);
            Assert.leq(this.subset.lastKey(), "this.subset.lastKey()", Integer.MAX_VALUE, "Integer.MAX_VALUE");
        }

        @Override
        public void close() throws IOException {
            BaseChunkInputStreamGenerator.this.close();
            subset.close();
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException(getClass() + " is to be used as Drainable only");
        }

        protected int getRawSize() throws IOException {
            long size = 0;
            if (nullCount() != 0) {
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
    }

    protected static final class SerContext {
        long accumulator = 0;
        long count = 0;
    }
}
