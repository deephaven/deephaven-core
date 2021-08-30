package io.deephaven.db.v2.tuples;

import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Factory to produce immutable tuples from a long key in {@link io.deephaven.db.v2.utils.Index}
 * space.
 */
public interface TupleSource<TUPLE_TYPE>
    extends TupleExporter<TUPLE_TYPE>, ChunkSource.WithPrev<Attributes.Values> {

    /**
     * Get the {@link ColumnSource}s backing this tuple source.
     *
     * @return The column sources
     */
    List<ColumnSource> getColumnSources();

    /**
     * Create a tuple for key column values at the supplied index key.
     *
     * @param indexKey The index key
     * @return The resulting tuple
     */
    TUPLE_TYPE createTuple(final long indexKey);

    /**
     * Create a tuple for previous key column values at the supplied index key.
     *
     * @param indexKey The index key
     * @return The resulting tuple
     */
    TUPLE_TYPE createPreviousTuple(final long indexKey);

    /**
     * Create a tuple for the supplied (boxed) values.
     *
     * @param values The values
     * @return The resulting tuple
     */
    TUPLE_TYPE createTupleFromValues(@NotNull final Object... values);

    /**
     * Create a tuple for the supplied reinterpreted values (e.g., those that come from the
     * getColumnSources after a reinterpretation by {@link TupleSourceFactory}).
     *
     * @param values The values
     * @return The resulting tuple
     */
    default TUPLE_TYPE createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return createTupleFromValues(values);
    }
}
