package io.deephaven.engine.v2.hashing;

import io.deephaven.engine.chunk.Attributes.Any;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.rftable.Context;
import io.deephaven.engine.chunk.LongChunk;

/**
 * A function that transforms a Chunk to an LongChunk.
 *
 * @param <T> the chunk's attribute
 */
public interface ToLongFunctor<T extends Any> extends Context {
    /**
     * Apply this function to the input chunk, returning an output chunk.
     *
     * The result is owned by this {@link Context}.
     *
     * @param input the chunk to transform
     * @return the result LongChunk
     */
    LongChunk<T> apply(Chunk<T> input);

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