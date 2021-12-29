package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.util.SafeCloseable;

/**
 * A function that transforms a Chunk to an LongChunk.
 *
 * @param <T> the chunk's attribute
 */
public interface ToLongFunctor<T extends Any> extends SafeCloseable {
    /**
     * Apply this function to the input chunk, returning an output chunk.
     *
     * The result is owned by this {@link ToLongFunctor}.
     *
     * @param input the chunk to transform
     * @return the result LongChunk
     */
    LongChunk<T> apply(Chunk<T> input);

    @Override
    default void close() {}

    static <T extends Any> Identity<T> makeIdentity() {
        //noinspection unchecked
        return Identity.INSTANCE;
    }

    class Identity<T extends Any> implements ToLongFunctor<T> {
        public static final Identity INSTANCE = new Identity();

        @Override
        public LongChunk<T> apply(Chunk<T> input) {
            return input.asLongChunk();
        }
    }
}