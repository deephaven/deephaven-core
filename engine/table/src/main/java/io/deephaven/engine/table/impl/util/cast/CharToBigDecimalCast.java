//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.cast;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;

/**
 * Cast the values in the input chunk to an int.
 */
public class CharToBigDecimalCast implements ToBigDecimalCast {
    private final WritableObjectChunk<BigDecimal, ? extends Any> result;

    CharToBigDecimalCast(int size) {
        result = WritableObjectChunk.makeWritableChunk(size);
    }

    @Override
    public <T> ObjectChunk<BigDecimal, ? extends Any> cast(Chunk<? extends T> input) {
        return cast(input.asCharChunk());
    }

    @SuppressWarnings("unchecked")
    private <T extends Any> ObjectChunk<BigDecimal, ? extends Any> cast(CharChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(CharChunk<? extends T2> input,
            WritableObjectChunk<BigDecimal, ? extends Any> result) {
        final int size = input.size();
        for (int ii = 0; ii < size; ++ii) {
            final char value = input.get(ii);
            if (value == QueryConstants.NULL_CHAR) {
                result.set(ii, null);
            } else {
                result.set(ii, BigDecimal.valueOf(value));
            }
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        // Fill with nulls before closing.
        result.fillWithNullValue(0, result.size());
        result.close();
    }
}
