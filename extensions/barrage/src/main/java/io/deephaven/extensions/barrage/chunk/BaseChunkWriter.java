//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

public abstract class BaseChunkWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>>
        implements ChunkWriter<SOURCE_CHUNK_TYPE> {
    @FunctionalInterface
    public interface ChunkTransformer<SOURCE_CHUNK_TYPE extends Chunk<Values>> {
        Chunk<Values> transform(SOURCE_CHUNK_TYPE values);
    }

    public static final byte[] PADDING_BUFFER = new byte[8];
    public static final int REMAINDER_MOD_8_MASK = 0x7;

    private final ChunkTransformer<SOURCE_CHUNK_TYPE> transformer;
    private final Supplier<SOURCE_CHUNK_TYPE> emptyChunkSupplier;
    /** the size of each element in bytes if fixed */
    protected final int elementSize;
    /** whether we can use the wire value as a deephaven null for clients that support dh nulls */
    protected final boolean dhNullable;
    /** whether the field is nullable */
    protected final boolean fieldNullable;

    BaseChunkWriter(
            @Nullable final ChunkTransformer<SOURCE_CHUNK_TYPE> transformer,
            @NotNull final Supplier<SOURCE_CHUNK_TYPE> emptyChunkSupplier,
            final int elementSize,
            final boolean dhNullable,
            final boolean fieldNullable) {
        this.transformer = transformer;
        this.emptyChunkSupplier = emptyChunkSupplier;
        this.elementSize = elementSize;
        this.dhNullable = dhNullable;
        this.fieldNullable = fieldNullable;
    }

    @Override
    public final DrainableColumn getEmptyInputStream(final @NotNull BarrageOptions options) throws IOException {
        try (Context context = makeContext(emptyChunkSupplier.get(), 0)) {
            return getInputStream(context, null, options);
        }
    }

    @Override
    public Context makeContext(@NotNull SOURCE_CHUNK_TYPE chunk, long rowOffset) {
        if (transformer == null) {
            return new Context(chunk, rowOffset);
        }
        Context retContext = null;
        try {
            retContext = new Context(transformer.transform(chunk), rowOffset);
        } finally {
            if (chunk instanceof PoolableChunk && (retContext == null || retContext.getChunk() != chunk)) {
                ((PoolableChunk<?>) chunk).close();
            }
        }
        return retContext;
    }

    @Override
    public boolean isFieldNullable() {
        return fieldNullable;
    }

    /**
     * Compute the number of nulls in the subset.
     *
     * @param context the context for the chunk
     * @param subset the subset of rows to consider
     * @return the number of nulls in the subset
     */
    protected abstract int computeNullCount(
            @NotNull Context context,
            @NotNull RowSequence subset);

    /**
     * Update the validity buffer for the subset.
     *
     * @param context the context for the chunk
     * @param subset the subset of rows to consider
     * @param serContext the serialization context
     */
    protected abstract void writeValidityBufferInternal(
            @NotNull Context context,
            @NotNull RowSequence subset,
            @NotNull SerContext serContext);

    abstract class BaseChunkInputStream<CONTEXT_TYPE extends Context> extends DrainableColumn {
        protected final CONTEXT_TYPE context;
        protected final RowSet subset;
        protected final BarrageOptions options;

        protected boolean hasBeenRead = false;
        private final int nullCount;

        BaseChunkInputStream(
                @NotNull final CONTEXT_TYPE context,
                @Nullable final RowSet subset,
                @NotNull final BarrageOptions options) {
            this.context = context;
            context.incrementReferenceCount();
            this.options = options;

            this.subset = context.size() == 0 ? RowSetFactory.empty()
                    : subset != null
                            ? subset.copy()
                            : RowSetFactory.flat(context.size());

            // ignore the empty context as these are intentionally empty writers that should work for any subset
            if (context.size() > 0 && this.subset.lastRowKey() >= context.size()) {
                throw new IllegalStateException(
                        "Subset " + this.subset + " is out of bounds for context of size " + context.size());
            }

            if (dhNullable && options.useDeephavenNulls()) {
                nullCount = 0;
            } else {
                nullCount = computeNullCount(context, this.subset);
            }
        }

        @Override
        public void close() throws IOException {
            context.decrementReferenceCount();
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
            return (hasBeenRead ? 0 : rawSize + (rawMod8 > 0 ? 8 - rawMod8 : 0));
        }

        /**
         * @formatter:off
         * There are two cases we don't send a validity buffer:
         * - the simplest case is following the arrow flight spec, which says that if there are no nulls present, the
         *   buffer is optional.
         * - Our implementation of nullCount() for primitive types will return zero if the useDeephavenNulls flag is
         *   set, so the buffer will also be omitted in that case. The client's marshaller does not need to be aware of
         *   deephaven nulls but in this mode we assume the consumer understands which value is the assigned NULL.
         * @formatter:on
         */
        protected boolean sendValidityBuffer() {
            return nullCount() != 0;
        }

        @Override
        public int nullCount() {
            return fieldNullable ? nullCount : 0;
        }

        protected long writeValidityBuffer(final DataOutput dos) {
            if (!sendValidityBuffer()) {
                return 0;
            }

            try (final SerContext serContext = new SerContext(dos)) {
                writeValidityBufferInternal(context, subset, serContext);
            }

            return getValidityMapSerializationSizeFor(subset.intSize());
        }

        /**
         * @param bufferSize the size of the buffer to pad
         * @return the total size of the buffer after padding
         */
        protected long padBufferSize(long bufferSize) {
            final long bytesExtended = bufferSize & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                bufferSize += 8 - bytesExtended;
            }
            return bufferSize;
        }

        /**
         * Write padding bytes to the output stream to ensure proper alignment.
         *
         * @param dos the output stream
         * @param bytesWritten the number of bytes written so far that need to be padded
         * @return the number of bytes extended by the padding
         * @throws IOException if an error occurs while writing to the output stream
         */
        protected long writePadBuffer(final DataOutput dos, long bytesWritten) throws IOException {
            final long bytesExtended = bytesWritten & REMAINDER_MOD_8_MASK;
            if (bytesExtended == 0) {
                return 0;
            }
            dos.write(PADDING_BUFFER, 0, (int) (8 - bytesExtended));
            return 8 - bytesExtended;
        }
    }

    /**
     * Returns expected size of validity map in bytes.
     *
     * @param numElements the number of rows
     * @return number of bytes to represent the validity buffer for numElements
     */
    protected static int getValidityMapSerializationSizeFor(final int numElements) {
        return getNumLongsForBitPackOfSize(numElements) * 8;
    }

    /**
     * Returns the number of longs needed to represent a single bit per element.
     *
     * @param numElements the number of rows
     * @return number of longs needed to represent numElements bits rounded up to the nearest long
     */
    protected static int getNumLongsForBitPackOfSize(final int numElements) {
        return ((numElements + 63) / 64);
    }

    protected static final class SerContext implements SafeCloseable {
        private final DataOutput dos;

        private long accumulator = 0;
        private long count = 0;

        public SerContext(@NotNull final DataOutput dos) {
            this.dos = dos;
        }

        public void setNextIsNull(boolean isNull) {
            if (!isNull) {
                accumulator |= 1L << count;
            }
            if (++count == 64) {
                flush();
            }
        }

        private void flush() {
            if (count == 0) {
                return;
            }

            try {
                dos.writeLong(accumulator);
            } catch (final IOException e) {
                throw new UncheckedDeephavenException(
                        "Unexpected exception while draining data to OutputStream: ", e);
            }
            accumulator = 0;
            count = 0;
        }

        @Override
        public void close() {
            flush();
        }
    }
}
