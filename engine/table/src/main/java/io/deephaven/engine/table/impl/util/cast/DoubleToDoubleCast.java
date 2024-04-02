//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharToDoubleCast and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.cast;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;

/**
 * Cast the values in the input chunk to an int.
 */
public class DoubleToDoubleCast implements ToDoubleCast {
    private final WritableDoubleChunk result;

    DoubleToDoubleCast(int size) {
        result = WritableDoubleChunk.makeWritableChunk(size);
    }

    @Override
    public <T> DoubleChunk<? extends T> cast(Chunk<? extends T> input) {
        return cast(input.asDoubleChunk());
    }

    @SuppressWarnings("unchecked")
    private <T extends Any> DoubleChunk<T> cast(DoubleChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(DoubleChunk<? extends T2> input, WritableDoubleChunk<T2> result) {
        final int size = input.size();
        for (int ii = 0; ii < size; ++ii) {
            final double value = input.get(ii);
            if (value == QueryConstants.NULL_DOUBLE) {
                result.set(ii, QueryConstants.NULL_DOUBLE);
            } else {
                result.set(ii, value);
            }
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        result.close();
    }
}
