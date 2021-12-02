package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

/**
 * Cast the values in the input chunk to an int.
 *
 * @param <T> the chunk's attribute
 */
public class CharToIntegerCast<T extends Any> implements ToIntFunctor<T> {
    private final WritableIntChunk<T> result;

    CharToIntegerCast(int size) {
        result = WritableIntChunk.makeWritableChunk(size);
    }

    @Override
    public IntChunk<? extends T> apply(Chunk<? extends T> input) {
        return cast(input.asCharChunk());
    }

    private IntChunk<T> cast(CharChunk<? extends T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(CharChunk<? extends T2> input, WritableIntChunk<T2> result) {
        for (int ii = 0; ii < input.size(); ++ii) {
            result.set(ii, (int)input.get(ii));
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        result.close();
    }
}