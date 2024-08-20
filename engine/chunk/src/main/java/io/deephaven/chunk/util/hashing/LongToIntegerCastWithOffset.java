//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharToIntegerCastWithOffset and run "./gradlew replicateHashing" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableIntChunk;

/**
 * Cast the values in the input chunk to an int and add the specified offset.
 *
 * @param <T> the chunk's attribute
 */
public class LongToIntegerCastWithOffset<T extends Any> implements ToIntFunctor<T> {
    private final WritableIntChunk<T> result;
    private final int offset;

    LongToIntegerCastWithOffset(int size, int offset) {
        result = WritableIntChunk.makeWritableChunk(size);
        this.offset = offset;
    }

    @Override
    public IntChunk<? extends T> apply(Chunk<? extends T> input) {
        return castWithOffset(input.asLongChunk());
    }

    private IntChunk<T> castWithOffset(LongChunk<? extends T> input) {
        for (int ii = 0; ii < input.size(); ++ii) {
            result.set(ii, (int) input.get(ii) + offset);
        }
        result.setSize(input.size());
        return result;
    }

    @Override
    public void close() {
        result.close();
    }
}
