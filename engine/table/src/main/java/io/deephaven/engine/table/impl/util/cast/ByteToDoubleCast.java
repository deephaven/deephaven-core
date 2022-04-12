/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharToDoubleCast and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.util.cast;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;

/**
 * Cast the values in the input chunk to an int.
 */
public class ByteToDoubleCast implements ToDoubleCast {
    private final WritableDoubleChunk result;

    ByteToDoubleCast(int size) {
        result = WritableDoubleChunk.makeWritableChunk(size);
    }

    @Override
    public <T> DoubleChunk<? extends T> cast(Chunk<? extends T> input) {
        return cast(input.asByteChunk());
    }

    @SuppressWarnings("unchecked")
    private <T extends Any> DoubleChunk<T> cast(ByteChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(ByteChunk<? extends T2> input, WritableDoubleChunk<T2> result) {
        for (int ii = 0; ii < input.size(); ++ii) {
            final byte value = input.get(ii);
            if (value == QueryConstants.NULL_BYTE) {
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