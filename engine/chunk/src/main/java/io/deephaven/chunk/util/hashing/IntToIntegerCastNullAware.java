//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharToIntegerCastNullAware and run "./gradlew replicateHashing" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;

/**
 * Cast the values in the input chunk to an int, mapping the null sentinel to {@link QueryConstants#NULL_INT}.
 *
 * @param <T> the chunk's attribute
 */
public class IntToIntegerCastNullAware<T extends Any> implements ToIntFunctor<T> {
    private final WritableIntChunk<T> result;

    IntToIntegerCastNullAware(int size) {
        result = WritableIntChunk.makeWritableChunk(size);
    }

    @Override
    public IntChunk<? extends T> apply(Chunk<? extends T> input) {
        return cast(input.asIntChunk());
    }

    private IntChunk<T> cast(IntChunk<? extends T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(IntChunk<? extends T2> input, WritableIntChunk<T2> result) {
        for (int ii = 0; ii < input.size(); ++ii) {
            final int c = input.get(ii);
            result.set(ii, c == QueryConstants.NULL_INT ? QueryConstants.NULL_INT : (int) c);
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        result.close();
    }
}
