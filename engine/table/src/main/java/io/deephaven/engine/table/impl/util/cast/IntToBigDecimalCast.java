//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharToBigDecimalCast and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.cast;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;

/**
 * Cast the values in the input chunk to an int.
 */
public class IntToBigDecimalCast implements ToBigDecimalCast {
    private final WritableObjectChunk<BigDecimal, ? extends Any> result;

    IntToBigDecimalCast(int size) {
        result = WritableObjectChunk.makeWritableChunk(size);
    }

    @Override
    public <T> ObjectChunk<BigDecimal, ? extends Any> cast(Chunk<? extends T> input) {
        return cast(input.asIntChunk());
    }

    @SuppressWarnings("unchecked")
    private <T extends Any> ObjectChunk<BigDecimal, ? extends Any> cast(IntChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(IntChunk<? extends T2> input,
            WritableObjectChunk<BigDecimal, ? extends Any> result) {
        final int size = input.size();
        for (int ii = 0; ii < size; ++ii) {
            final int value = input.get(ii);
            if (value == QueryConstants.NULL_INT) {
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
