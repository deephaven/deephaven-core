//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharToIntegerCast and run "./gradlew replicateHashing" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

/**
 * Cast the values in the input chunk to an int.
 *
 * @param <T> the chunk's attribute
 */
public class LongToIntegerCast<T extends Any> implements ToIntFunctor<T> {
    private final WritableIntChunk<T> result;

    LongToIntegerCast(int size) {
        result = WritableIntChunk.makeWritableChunk(size);
    }

    @Override
    public IntChunk<? extends T> apply(Chunk<? extends T> input) {
        return cast(input.asLongChunk());
    }

    private IntChunk<T> cast(LongChunk<? extends T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(LongChunk<? extends T2> input, WritableIntChunk<T2> result) {
        for (int ii = 0; ii < input.size(); ++ii) {
            result.set(ii, (int) input.get(ii));
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        result.close();
    }
}
