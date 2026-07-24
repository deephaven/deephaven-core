//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableIntChunk;

public interface ToIntegerCast<T extends Any> extends ToIntFunctor<T> {
    IntChunk<? extends T> apply(Chunk<? extends T> input);

    /**
     * Create an IntFunctor that casts the values in an input chunk to an int. An optional offset is applied to each
     * value after the cast.
     *
     * @param type the type of chunk, must be an integral primitive type
     * @param size the size of the largest chunk that can be cast by this functor
     * @param offset an offset to add to each casted result
     * @param <T> the chunk's attribute
     *
     * @return a {@link ToIntFunctor} that can be applied to chunks of type in order to produce an IntChunk of values
     */
    static <T extends Any> ToIntFunctor<T> makeToIntegerCast(ChunkType type, int size, int offset) {
        switch (type) {
            case Byte:
                if (offset == 0) {
                    return new ByteToIntegerCast<>(size);
                } else {
                    return new ByteToIntegerCastWithOffset<>(size, offset);
                }
            case Char:
                if (offset == 0) {
                    return new CharToIntegerCast<>(size);
                } else {
                    return new CharToIntegerCastWithOffset<>(size, offset);
                }
            case Short:
                if (offset == 0) {
                    return new ShortToIntegerCast<>(size);
                } else {
                    return new ShortToIntegerCastWithOffset<>(size, offset);
                }
            case Int:
                if (offset == 0) {
                    return ToIntFunctor.makeIdentity();
                } else {
                    return new IntToIntegerCastWithOffset<>(size, offset);
                }
            case Long:
                if (offset == 0) {
                    return new LongToIntegerCast<>(size);
                } else {
                    return new LongToIntegerCastWithOffset<>(size, offset);
                }

            case Boolean:
            case Float:
            case Double:
            case Object:
        }
        throw new UnsupportedOperationException();
    }

    /**
     * Create an IntFunctor that casts the values in an input chunk to an int, mapping the source type's null sentinel
     * to {@link io.deephaven.util.QueryConstants#NULL_INT}.
     *
     * @param type the type of chunk, must be an integral primitive type
     * @param size the size of the largest chunk that can be cast by this functor
     * @param <T> the chunk's attribute
     *
     * @return a {@link ToIntFunctor} that can be applied to chunks of type in order to produce an IntChunk of values
     */
    static <T extends Any> ToIntFunctor<T> makeToIntegerCastNullAware(ChunkType type, int size) {
        switch (type) {
            case Byte:
                return new ByteToIntegerCastNullAware<>(size);
            case Char:
                return new CharToIntegerCastNullAware<>(size);
            case Short:
                return new ShortToIntegerCastNullAware<>(size);
            case Int:
                return ToIntFunctor.makeIdentity();
            case Long:
                return new LongToIntegerCastNullAware<>(size);

            case Boolean:
            case Float:
            case Double:
            case Object:
        }
        throw new UnsupportedOperationException();
    }

    /**
     * Cast {@code src} into {@code dst}, performing a raw (non-null-aware) widening or narrowing conversion.
     *
     * @param src the source chunk, must be an integral primitive type
     * @param dst the destination chunk
     * @param <T> the chunk's attribute
     */
    static <T extends Any> void castInto(Chunk<? extends T> src, WritableIntChunk<T> dst) {
        switch (src.getChunkType()) {
            case Byte:
                ByteToIntegerCast.castInto(src.asByteChunk(), dst);
                return;
            case Char:
                CharToIntegerCast.castInto(src.asCharChunk(), dst);
                return;
            case Short:
                ShortToIntegerCast.castInto(src.asShortChunk(), dst);
                return;
            case Int:
                IntToIntegerCast.castInto(src.asIntChunk(), dst);
                return;
            case Long:
                LongToIntegerCast.castInto(src.asLongChunk(), dst);
                return;

            case Boolean:
            case Float:
            case Double:
            case Object:
        }
        throw new UnsupportedOperationException();
    }

    /**
     * Cast {@code src} into {@code dst}, mapping the source type's null sentinel to
     * {@link io.deephaven.util.QueryConstants#NULL_INT}.
     *
     * @param src the source chunk, must be an integral primitive type
     * @param dst the destination chunk
     * @param <T> the chunk's attribute
     */
    static <T extends Any> void castIntoNullAware(Chunk<? extends T> src, WritableIntChunk<T> dst) {
        switch (src.getChunkType()) {
            case Byte:
                ByteToIntegerCastNullAware.castInto(src.asByteChunk(), dst);
                return;
            case Char:
                CharToIntegerCastNullAware.castInto(src.asCharChunk(), dst);
                return;
            case Short:
                ShortToIntegerCastNullAware.castInto(src.asShortChunk(), dst);
                return;
            case Int:
                IntToIntegerCastNullAware.castInto(src.asIntChunk(), dst);
                return;
            case Long:
                LongToIntegerCastNullAware.castInto(src.asLongChunk(), dst);
                return;

            case Boolean:
            case Float:
            case Double:
            case Object:
        }
        throw new UnsupportedOperationException();
    }

}
