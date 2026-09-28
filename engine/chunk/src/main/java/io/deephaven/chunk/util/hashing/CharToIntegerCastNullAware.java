//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;

/**
 * Cast the values in the input chunk to an int, mapping the null sentinel to {@link QueryConstants#NULL_INT}.
 *
 * @param <T> the chunk's attribute
 */
public class CharToIntegerCastNullAware<T extends Any> implements ToIntFunctor<T> {
    private final WritableIntChunk<T> result;

    CharToIntegerCastNullAware(int size) {
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
            final char c = input.get(ii);
            result.set(ii, c == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : (int) c);
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        result.close();
    }
}
