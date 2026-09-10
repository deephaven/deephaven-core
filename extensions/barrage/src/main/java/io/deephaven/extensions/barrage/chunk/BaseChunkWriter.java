//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.configuration.Configuration;
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
import java.io.OutputStream;
import java.util.Arrays;
import java.util.function.Supplier;

public abstract class BaseChunkWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>>
        implements ChunkWriter<SOURCE_CHUNK_TYPE> {
    @FunctionalInterface
    public interface ChunkTransformer<SOURCE_CHUNK_TYPE extends Chunk<Values>> {
        Chunk<Values> transform(SOURCE_CHUNK_TYPE values);
    }

    public static final byte[] PADDING_BUFFER = new byte[8];
    public static final int REMAINDER_MOD_8_MASK = 0x7;

    /**
     * Upper bound (in bytes) on a single bulk payload write shared by the fixed-width primitive writers. Wide columns
     * are encoded into little-endian bytes and flushed in windows of this size rather than writing one value (and its
     * individual bytes) at a time. Configure once here (rather than per type) via
     * {@code BaseChunkWriter.bulkWriteBufferBytes}.
     */
    protected static final int BULK_WRITE_BUFFER_BYTES = Configuration.getInstance()
            .getIntegerForClassWithDefault(BaseChunkWriter.class, "bulkWriteBufferBytes", 4096);

    /** Number of {@code int}s buffered per bulk-write window (see {@link #BULK_WRITE_BUFFER_BYTES}). */
    private static final int BULK_WRITE_INTS = Math.max(1, BULK_WRITE_BUFFER_BYTES / Integer.BYTES);

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
    public DrainableColumn getEmptyInputStream(
            @NotNull final BarrageOptions options,
            @Nullable final DictionaryWriterRegistry dictionaryRegistry) throws IOException {
        try (Context context = makeContext(emptyChunkSupplier.get(), 0)) {
            // Route through the registry-aware overload so a composite writer (e.g. RunEndEncodedChunkWriter) that
            // forwards dictionaryRegistry to a nested dictionary-encoded child does so here too, and a
            // DictionaryChunkWriter itself registers its state even for this empty payload.
            return getInputStream(context, null, options, dictionaryRegistry);
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
     * Report the nullness of each row of the subset, in order, to {@code validity}.
     * <p>
     * This is invoked at most once per {@link BaseChunkInputStream}: the null count carried by the field node and the
     * bytes of the validity buffer are both derived from this single traversal of the row data.
     *
     * @param context the context for the chunk
     * @param subset the subset of rows to consider
     * @param validity the validity buffer to populate
     */
    protected abstract void computeValidity(
            @NotNull Context context,
            @NotNull RowSequence subset,
            @NotNull ValidityBuffer validity);

    abstract class BaseChunkInputStream<CONTEXT_TYPE extends Context> extends DrainableColumn {
        protected final CONTEXT_TYPE context;
        protected final RowSet subset;
        protected final BarrageOptions options;

        protected boolean hasBeenRead = false;
        private final int nullCount;
        /** The bitmap to drain, retained only when we will actually send a validity buffer. */
        private final ValidityBuffer validityBuffer;

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

            // A non-nullable field never reports nulls (see nullCount()) and a dh-nullable field encodes them in the
            // payload itself, so in both cases the traversal below would produce a bitmap nobody reads.
            if (!fieldNullable || (dhNullable && options.useDeephavenNulls())) {
                nullCount = 0;
                validityBuffer = null;
            } else {
                final ValidityBuffer validity = new ValidityBuffer(this.subset.intSize());
                computeValidity(context, this.subset, validity);
                nullCount = validity.nullCount();
                // Retain the bitmap only if we will send it; a batch of null-free columns would otherwise hold one
                // bitmap per column until it drains.
                validityBuffer = nullCount == 0 ? null : validity;
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

            // the bitmap was packed into little-endian bytes when it was computed; emit it with a single bulk write
            final byte[] bytes = validityBuffer.bytes();
            try {
                dos.write(bytes, 0, bytes.length);
            } catch (final IOException e) {
                throw new UncheckedDeephavenException(
                        "Unexpected exception while draining data to OutputStream: ", e);
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

    /**
     * A bit per element, packed LSB-first into little-endian 64-bit words, as Arrow encodes a validity buffer: a set
     * bit marks a valid (non-null) element. Nulls are counted as the bits are appended, so a single traversal of the
     * row data yields both the field node's null count and the bytes of the validity buffer.
     * <p>
     * {@link BooleanChunkWriter} also uses this to pack its payload, which has the same shape, via
     * {@link #packed(int)}.
     */
    protected static final class ValidityBuffer {
        private final int numElements;

        /**
         * Allocated on the first null. A column with no nulls sends no validity buffer at all, and that is the common
         * case, so until a null appears the only state worth maintaining is {@link #count} — this keeps the traversal
         * as cheap as the bare null count it replaced. The bits skipped along the way are all set, so the buffer can
         * still be reconstructed exactly whenever a null does turn up.
         */
        private byte[] bytes;

        /** Number of elements appended so far; the packed bit position when {@link #bytes} is non-null. */
        private int count = 0;
        private long accumulator = 0;
        private int byteOffset = 0;
        private int nullCount = 0;
        private boolean sealed = false;

        /**
         * A validity bitmap, materialized only once a null is appended. A column with no nulls sends no validity buffer
         * at all, so asking such a buffer for its {@link #bytes()} is a caller bug and throws.
         */
        public ValidityBuffer(final int numElements) {
            this.numElements = numElements;
        }

        /**
         * A bit-packed buffer that is materialized up front, for a caller that needs the bits whatever the values turn
         * out to be. {@link BooleanChunkWriter} packs its payload this way, where a set bit means TRUE rather than
         * non-null and an all-TRUE column must still emit a full buffer.
         *
         * @param numElements the number of elements to be appended
         * @return a buffer whose {@link #bytes()} is always available
         */
        public static ValidityBuffer packed(final int numElements) {
            final ValidityBuffer buffer = new ValidityBuffer(numElements);
            buffer.allocate();
            return buffer;
        }

        public void setNextIsNull(final boolean isNull) {
            if (bytes != null) {
                appendPacked(isNull);
            } else if (isNull) {
                allocate();
                appendPacked(true);
            } else {
                ++count;
            }
        }

        /**
         * Append {@code numElements} null entries; equivalent to that many {@code setNextIsNull(true)} calls, but
         * without visiting each element.
         *
         * @param numElements the number of null entries to append
         */
        public void setNextAreNull(final int numElements) {
            if (numElements == 0) {
                return;
            }
            allocate();
            nullCount += numElements;
            for (int remaining = numElements; remaining > 0;) {
                final int inThisWord = Math.min(remaining, 64 - (count & 63));
                count += inThisWord;
                if ((count & 63) == 0) {
                    flushWord();
                }
                remaining -= inThisWord;
            }
        }

        private void appendPacked(final boolean isNull) {
            if (isNull) {
                ++nullCount;
            } else {
                accumulator |= 1L << (count & 63);
            }
            if ((++count & 63) == 0) {
                flushWord();
            }
        }

        /**
         * Materialize the buffer at the first null. Every element visited so far was valid, so the words already passed
         * are all-ones, as are the bits of the word in progress.
         */
        private void allocate() {
            if (bytes != null) {
                return;
            }
            bytes = new byte[getValidityMapSerializationSizeFor(numElements)];
            byteOffset = (count >>> 6) * Long.BYTES;
            Arrays.fill(bytes, 0, byteOffset, (byte) 0xFF);
            accumulator = (1L << (count & 63)) - 1;
        }

        public int nullCount() {
            return nullCount;
        }

        /**
         * Finalize and return the packed bytes; exactly {@code getValidityMapSerializationSizeFor(numElements)} of
         * them. No element may be appended afterwards.
         *
         * @throws IllegalStateException if nothing was ever materialized, i.e. no null was appended and the buffer was
         *         not created by {@link #packed(int)}
         */
        public byte[] bytes() {
            if (bytes == null) {
                throw new IllegalStateException("No bits have been packed: a validity buffer is only written when "
                        + "nullCount() is non-zero; a caller that needs the bytes regardless must use packed()");
            }
            if (!sealed) {
                sealed = true;
                if ((count & 63) != 0) {
                    flushWord();
                }
            }
            return bytes;
        }

        private void flushWord() {
            LittleEndianCodec.putLong(bytes, byteOffset, accumulator);
            byteOffset += Long.BYTES;
            accumulator = 0;
        }
    }

    /**
     * Buffers little-endian {@code int} values — an Arrow offset or lengths buffer — and flushes them in windows of
     * {@link #BULK_WRITE_BUFFER_BYTES}, rather than making one {@link DataOutput} call, i.e. four individual byte
     * writes, per value.
     */
    protected static final class BulkIntWriter implements SafeCloseable {
        private final OutputStream outputStream;
        private final byte[] buffer;
        private int bufferPos = 0;

        public BulkIntWriter(@NotNull final OutputStream outputStream) {
            this.outputStream = outputStream;
            this.buffer = new byte[BULK_WRITE_INTS * Integer.BYTES];
        }

        public void write(final int value) {
            LittleEndianCodec.putInt(buffer, bufferPos, value);
            bufferPos += Integer.BYTES;
            if (bufferPos == buffer.length) {
                flush();
            }
        }

        private void flush() {
            if (bufferPos == 0) {
                return;
            }
            try {
                outputStream.write(buffer, 0, bufferPos);
            } catch (final IOException e) {
                throw new UncheckedDeephavenException(
                        "Unexpected exception while draining data to OutputStream: ", e);
            }
            bufferPos = 0;
        }

        @Override
        public void close() {
            flush();
        }
    }
}
