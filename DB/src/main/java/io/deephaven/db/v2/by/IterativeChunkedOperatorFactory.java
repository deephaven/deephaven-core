package io.deephaven.db.v2.by;

public interface IterativeChunkedOperatorFactory {
    IterativeChunkedAggregationOperator getChunkedOperator(Class type, String resultName,
        boolean exposeInternalColumns);
}
