package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;

public interface ToLongCast<T extends Any> extends ToLongFunctor<T> {
    LongChunk<T> apply(Chunk<T> input);

    /**
     * Create an LongFunctor that casts the values in an input chunk to an long.  An optional offset is applied to each
     * value after the cast.
     *
     * @param type the type of chunk, must be an integral primitive type
     * @param size the size of the largest chunk that can be cast by this functor
     * @param offset an offset to add to each casted result
     * @param <T> the chunk's attribute
     *
     * @return a {@link ToLongFunctor} that can be applied to chunks of type in order to produce an LongChunk of values
     */
    static <T extends Any> ToLongFunctor<T> makeToLongCast(ChunkType type, int size, long offset) {
        switch (type) {
            case Byte:
                if (offset == 0) {
                    return new ByteToLongCast<>(size);
                } else {
                    return new ByteToLongCastWithOffset<>(size, offset);
                }
            case Char:
                if (offset == 0) {
                    return new CharToLongCast<>(size);
                } else {
                    return new CharToLongCastWithOffset<>(size, offset);
                }
            case Short:
                if (offset == 0) {
                    return new ShortToLongCast<>(size);
                } else {
                    return new ShortToLongCastWithOffset<>(size, offset);
                }
            case Int:
                if (offset == 0) {
                    return new IntToLongCast<>(size);
                } else {
                    return new IntToLongCastWithOffset<>(size, offset);
                }
            case Long:
                if (offset == 0) {
                    return ToLongFunctor.makeIdentity();
                } else {
                    return new LongToLongCastWithOffset<>(size, offset);
                }

            case Boolean:
            case Float:
            case Double:
            case Object:
        }
        throw new UnsupportedOperationException();
    }

}
