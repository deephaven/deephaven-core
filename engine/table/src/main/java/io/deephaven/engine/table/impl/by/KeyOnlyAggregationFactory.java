package io.deephaven.engine.table.impl.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ChunkSource;
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
