//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

/**
 * Factory to produce immutable tuples from a long row key.
 */
public interface TupleSource<TUPLE_TYPE> extends TupleExporter<TUPLE_TYPE>, ChunkSource.WithPrev<Values> {

    /**
     * Create a tuple for key column values at the supplied row key.
     *
     * @param rowKey The row key
     * @return The resulting tuple
     */
    TUPLE_TYPE createTuple(long rowKey);

    /**
     * Create a tuple for previous key column values at the supplied row key.
     *
     * @param rowKey The row key
     * @return The resulting tuple
     */
    TUPLE_TYPE createPreviousTuple(long rowKey);

    /**
     * Create a tuple for the supplied (boxed) values.
     *
     * @param values The values
     * @return The resulting tuple
     */
    TUPLE_TYPE createTupleFromValues(@NotNull Object... values);

    /**
     * Create a tuple for the supplied reinterpreted values.
     *
     * @param values The values
     * @return The resulting tuple
     */
    default TUPLE_TYPE createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return createTupleFromValues(values);
    }
}
