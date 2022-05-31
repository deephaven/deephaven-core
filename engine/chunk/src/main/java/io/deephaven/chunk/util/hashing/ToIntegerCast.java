package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;

public interface ToIntegerCast<T extends Any>  extends ToIntFunctor<T> {
    IntChunk<? extends T> apply(Chunk<? extends T> input);

    /**
     * Create an IntFunctor that casts the values in an input chunk to an int.  An optional offset is applied to each
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

}
