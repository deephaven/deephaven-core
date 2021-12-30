/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharToLongCast and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

/**
 * Cast the values in the input chunk to a long.
 *
 * @param <T> the chunk's attribute
 */
public class IntToLongCast<T extends Any> implements ToLongFunctor<T> {
    private final WritableLongChunk<T> result;

    IntToLongCast(int size) {
        result = WritableLongChunk.makeWritableChunk(size);
    }

    @Override
    public LongChunk<T> apply(Chunk<T> input) {
        return cast(input.asIntChunk());
    }

    private LongChunk<T> cast(IntChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any>  void castInto(IntChunk<? extends T2> input, WritableLongChunk<? super T2> result) {
        for (int ii = 0; ii < input.size(); ++ii) {
            result.set(ii, (long)input.get(ii));
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        result.close();
    }
}