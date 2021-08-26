package io.deephaven.db.v2.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import org.jetbrains.annotations.NotNull;

/**
 * Simplest factory that has no operators or results, used to implement selectDistinct.
 */
public class KeyOnlyAggregationFactory implements AggregationContextFactory {
    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table,
        @NotNull final String... groupByColumns) {
        // noinspection unchecked
        return new AggregationContext(
            IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY,
            CollectionUtil.ZERO_LENGTH_STRING_ARRAY_ARRAY,
            ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY,
            false);
    }
}
