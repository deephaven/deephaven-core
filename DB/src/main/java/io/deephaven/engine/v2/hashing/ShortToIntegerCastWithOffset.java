/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharToIntegerCastWithOffset and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.hashing;

import io.deephaven.engine.v2.sources.chunk.Attributes.Any;
import io.deephaven.engine.v2.sources.chunk.ShortChunk;
import io.deephaven.engine.v2.sources.chunk.Chunk;
import io.deephaven.engine.v2.sources.chunk.IntChunk;
import io.deephaven.engine.v2.sources.chunk.WritableIntChunk;

/**
 * Cast the values in the input chunk to an int and add the specified offset.
 *
 * @param <T> the chunk's attribute
 */
public class ShortToIntegerCastWithOffset<T extends Any> implements ToIntFunctor<T> {
    private final WritableIntChunk<T> result;
    private final int offset;

    ShortToIntegerCastWithOffset(int size, int offset) {
        result = WritableIntChunk.makeWritableChunk(size);
        this.offset = offset;
    }

    @Override
    public IntChunk<? extends T> apply(Chunk<? extends T> input) {
        return castWithOffset(input.asShortChunk());
    }

    private IntChunk<T> castWithOffset(ShortChunk<? extends T> input) {
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
