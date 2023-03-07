package io.deephaven.engine.primitive.function;

import java.util.function.IntToLongFunction;

/**
 * Like {@link IntToLongFunction}, but for primitive chars to primitive ints.
 */
@FunctionalInterface
public interface CharToIntFunction {

    /**
     * See {@link IntToLongFunction#applyAsLong(int)}.
     */
    int applyAsInt(char value);
}
