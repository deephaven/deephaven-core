/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharToLongCast and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.hashing;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;

/**
 * Cast the values in the input chunk to a long.
 *
 * @param <T> the chunk's attribute
 */
public class LongToLongCast<T extends Any> implements ToLongFunctor<T> {
    private final WritableLongChunk<T> result;

    LongToLongCast(int size) {
        result = WritableLongChunk.makeWritableChunk(size);
    }

    @Override
    public LongChunk<T> apply(Chunk<T> input) {
        return cast(input.asLongChunk());
    }

    private LongChunk<T> cast(LongChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any>  void castInto(LongChunk<? extends T2> input, WritableLongChunk<? super T2> result) {
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