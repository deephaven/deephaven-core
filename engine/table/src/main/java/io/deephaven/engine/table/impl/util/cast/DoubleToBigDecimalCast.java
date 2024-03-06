//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharToBigDecimalCast and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.cast;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;

/**
 * Cast the values in the input chunk to an int.
 */
public class DoubleToBigDecimalCast implements ToBigDecimalCast {
    private final WritableObjectChunk<BigDecimal, ? extends Any> result;

    DoubleToBigDecimalCast(int size) {
        result = WritableObjectChunk.makeWritableChunk(size);
    }

    @Override
    public <T> ObjectChunk<BigDecimal, ? extends Any> cast(Chunk<? extends T> input) {
        return cast(input.asDoubleChunk());
    }

    @SuppressWarnings("unchecked")
    private <T extends Any> ObjectChunk<BigDecimal, ? extends Any> cast(DoubleChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(DoubleChunk<? extends T2> input,
            WritableObjectChunk<BigDecimal, ? extends Any> result) {
        final int size = input.size();
        for (int ii = 0; ii < size; ++ii) {
            final double value = input.get(ii);
            if (value == QueryConstants.NULL_DOUBLE) {
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
