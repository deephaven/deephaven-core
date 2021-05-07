package io.deephaven.db.v2.hashing;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;

/**
 * Cast the values in the input chunk to a long.
 *
 * @param <T> the chunk's attribute
 */
public class CharToLongCast<T extends Any> implements ToLongFunctor<T> {
    private final WritableLongChunk<T> result;

    CharToLongCast(int size) {
        result = WritableLongChunk.makeWritableChunk(size);
    }

    @Override
    public LongChunk<T> apply(Chunk<T> input) {
        return cast(input.asCharChunk());
    }

    private LongChunk<T> cast(CharChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any>  void castInto(CharChunk<? extends T2> input, WritableLongChunk<? super T2> result) {
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