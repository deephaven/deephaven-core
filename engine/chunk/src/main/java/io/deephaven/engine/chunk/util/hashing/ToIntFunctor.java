package io.deephaven.engine.chunk.util.hashing;

import io.deephaven.engine.chunk.Attributes.Any;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.IntChunk;
import io.deephaven.util.SafeCloseable;

/**
 * A function that transforms a Chunk to an IntChunk.
 *
 * @param <T> the chunk's attribute
 */
public interface ToIntFunctor<T extends Any> extends SafeCloseable {
    /**
     * Apply this function to the input chunk, returning an output chunk.
     *
     * The result is owned by this ToIntFunctor.
     *
     * @param input the chunk to transform
     * @return the result IntChunk
     */
    IntChunk<? extends T> apply(Chunk<? extends T> input);

    @Override
    default void close() {}

    static <T extends Any> Identity<T> makeIdentity() {
        //noinspection unchecked
        return Identity.INSTANCE;
    }

    class Identity<T extends Any> implements ToIntFunctor<T> {
        public static final Identity INSTANCE = new Identity();

        @Override
        public IntChunk<? extends T> apply(Chunk<? extends T> input) {
            return input.asIntChunk();
        }
    }
}