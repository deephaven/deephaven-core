package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;

import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.db.v2.sources.chunk.util.pools.PoolableChunk;

import java.nio.Buffer;

/**
 * Data structure for a contiguous region of data that may be mutated.
 *
 * @param <ATTR> {@link Attributes} that apply to this chunk
 */
public interface WritableChunk<ATTR extends Any> extends Chunk<ATTR>, PoolableChunk {
    @Override
    WritableChunk<ATTR> slice(int offset, int capacity);

    /**
     * Fill a sub-range of this writable chunk with the appropriate Deephaven null value for the
     * type.
     *
     * @param offset Starting offset
     * @param size Number of values to fill
     */
    default void fillWithNullValue(int offset, int size) {
        throw new UnsupportedOperationException();
    }

    /**
     * Fill a sub-range of this writable chunk with the given value, unboxing it as appropriate.
     *
     * @param offset Starting offset
     * @param size Number of values to fill
     */
    default void fillWithBoxedValue(int offset, int size, Object value) {
        throw new UnsupportedOperationException();
    }



    void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length);

    void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length);

    /**
     * <p>
     * Fill a sub-range of this writable chunk with values from a {@link Buffer}. This is an
     * optional method, as some chunk types do not have a corresponding buffer type.
     *
     * <p>
     * Implementations are free to copy data as efficiently as they may, and will use absolute
     * rather than positional access where possible. To facilitate this pattern, {@code srcOffset}
     * is an absolute offset from position 0, rather than a relative offset from
     * {@code srcBuffer.position()}.
     *
     * <p>
     * <It is required that {@code srcBuffer.limit()} is at least {@code srcOffset + length}.
     *
     * <p>
     * {@code srcBuffer}'s position may be modified, but will always be restored to its initial
     * value upon successful return.
     *
     * @param srcBuffer The source buffer, which will be cast to the appropriate type for this chunk
     * @param srcOffset The offset into {@code srcBuffer} (from position 0, <em>not</em>
     *        {@code srcBuffer.position()}) to start copying from
     * @param destOffset The offset into this chunk to start copying to
     * @param length The number of elements to copy
     */
    default void copyFromBuffer(Buffer srcBuffer, int srcOffset, int destOffset, int length) {
        throw new UnsupportedOperationException();
    }

    default void setSize(int newSize) {
        internalSetSize(newSize, -7025656774858671822L);
    }

    /**
     * DO NOT CALL THIS INTERNAL METHOD. If you want to set a size, call
     * {@link WritableChunk#setSize}. That method is the only legal caller of this method in the
     * entire system.
     */
    void internalSetSize(int newSize, long password);

    default int capacity() {
        return internalCapacity(1837055652467547514L);
    }

    /**
     * DO NOT CALL THIS INTERNAL METHOD. Call {@link WritableChunk#capacity()} That method is the
     * only legal caller of this method in the entire system.
     */
    int internalCapacity(long password);

    /**
     * Sort this chunk in-place using Java's primitive defined ordering.
     *
     * Of note is that nulls or NaNs are not sorted according to Deephaven ordering rules.
     */
    void sort();

    /**
     * Sort this chunk in-place using Java's primitive defined ordering.
     *
     * Of note is that nulls or NaNs are not sorted according to Deephaven ordering rules.
     */
    default void sort(int start, int length) {
        throw new UnsupportedOperationException();
    }

    /**
     * Our ChunkFiller "plugin".
     */
    ChunkFiller getChunkFiller();

    default WritableByteChunk<ATTR> asWritableByteChunk() {
        return (WritableByteChunk<ATTR>) this;
    }

    default WritableBooleanChunk<ATTR> asWritableBooleanChunk() {
        return (WritableBooleanChunk<ATTR>) this;
    }

    default WritableCharChunk<ATTR> asWritableCharChunk() {
        return (WritableCharChunk<ATTR>) this;
    }

    default WritableShortChunk<ATTR> asWritableShortChunk() {
        return (WritableShortChunk<ATTR>) this;
    }

    default WritableIntChunk<ATTR> asWritableIntChunk() {
        return (WritableIntChunk<ATTR>) this;
    }

    default WritableLongChunk<ATTR> asWritableLongChunk() {
        return (WritableLongChunk<ATTR>) this;
    }

    default WritableFloatChunk<ATTR> asWritableFloatChunk() {
        return (WritableFloatChunk<ATTR>) this;
    }

    default WritableDoubleChunk<ATTR> asWritableDoubleChunk() {
        return (WritableDoubleChunk<ATTR>) this;
    }

    default <T> WritableObjectChunk<T, ATTR> asWritableObjectChunk() {
        return (WritableObjectChunk<T, ATTR>) this;
    }

    /**
     * Upcast the attribute.
     *
     * When you know the data you will receive in this chunk from another source is a more specific
     * suptype than the source provides, you can upcast the attribute with this helper method (such
     * as reading KeyIndices from a ColumnSource which thinks they are just Values.)
     *
     * @apiNote Downcast should not be necessary on WritableChunks, as a WritableChunk filling
     *          method should accept an lower bound wildcard.
     */
    static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableChunk<ATTR> upcast(
        WritableChunk<ATTR_DERIV> self) {
        // noinspection unchecked
        return (WritableChunk<ATTR>) self;
    }
}
