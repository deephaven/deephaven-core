package io.deephaven.engine.table.impl.by;

public interface IterativeChunkedOperatorFactory {
    IterativeChunkedAggregationOperator getChunkedOperator(Class type, String resultName,
            boolean exposeInternalColumns);
}
