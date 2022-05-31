package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableIntChunk;

/**
 * Cast the values in the input chunk to an int and add the specified offset.
 *
 * @param <T> the chunk's attribute
 */
public class CharToIntegerCastWithOffset<T extends Any> implements ToIntFunctor<T> {
    private final WritableIntChunk<T> result;
    private final int offset;

    CharToIntegerCastWithOffset(int size, int offset) {
        result = WritableIntChunk.makeWritableChunk(size);
        this.offset = offset;
    }

    @Override
    public IntChunk<? extends T> apply(Chunk<? extends T> input) {
        return castWithOffset(input.asCharChunk());
    }

    private IntChunk<T> castWithOffset(CharChunk<? extends T> input) {
        for (int ii = 0; ii < input.size(); ++ii) {
            result.set(ii, (int)input.get(ii) + offset);
        }
        result.setSize(input.size());
        return result;
    }

    @Override
    public void close() {
        result.close();
    }
}
