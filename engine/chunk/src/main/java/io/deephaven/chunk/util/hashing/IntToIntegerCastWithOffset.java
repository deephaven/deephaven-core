/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharToIntegerCastWithOffset and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableIntChunk;

/**
 * Cast the values in the input chunk to an int and add the specified offset.
 *
 * @param <T> the chunk's attribute
 */
public class IntToIntegerCastWithOffset<T extends Any> implements ToIntFunctor<T> {
    private final WritableIntChunk<T> result;
    private final int offset;

    IntToIntegerCastWithOffset(int size, int offset) {
        result = WritableIntChunk.makeWritableChunk(size);
        this.offset = offset;
    }

    @Override
    public IntChunk<? extends T> apply(Chunk<? extends T> input) {
        return castWithOffset(input.asIntChunk());
    }

    private IntChunk<T> castWithOffset(IntChunk<? extends T> input) {
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
