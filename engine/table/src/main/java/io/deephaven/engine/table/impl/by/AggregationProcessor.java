package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.*;
import io.deephaven.api.agg.spec.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Conversion tool to generate an {@link AggregationContextFactory} for a collection of
 * {@link Aggregation aggregations}.
 */
public class AggregationProcessor implements Aggregation.Visitor, AggSpec.Visitor {

    /**
     * Convert a collection of {@link Aggregation aggregations} to an {@link AggregationContextFactory}.
     *
     * @param aggregations The {@link Aggregation aggregations}
     * @return The {@link AggregationContextFactory}
     */
    public static AggregationContextFactory of(@NotNull final Collection<? extends Aggregation> aggregations) {
        return
    }

    private final List<IterativeChunkedAggregationOperator> operators = new ArrayList<>();
    private final List<String[]> inputNames = new ArrayList<>();
    private final List<ChunkSource.WithPrev<Values>> inputColumns = new ArrayList<>();
    private final List<AggregationContextTransformer> transformers = new ArrayList<>();

    private AggregationContextFactory build() {
        final IterativeChunkedAggregationOperator[] operatorsArray = operators
                .toArray(IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY);
        final String[][] inputNamesArray = inputNames
                .toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY_ARRAY);
        // noinspection unchecked
        final ChunkSource.WithPrev<Values>[] inputColumnsArray = inputColumns
                .toArray(ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY);
        final AggregationContextTransformer[] transformersArray = transformers
                .toArray(AggregationContextTransformer.ZERO_LENGTH_AGGREGATION_CONTEXT_TRANSFORMER_ARRAY);

        return new AggregationContext(operatorsArray, inputNamesArray, inputColumnsArray, transformersArray, true);
    }

    @Override
    public void visit(Count count) {

    }

    @Override
    public void visit(NormalAggregation normalAgg) {

    }

    @Override
    public void visit(NormalAggregations normalAggs) {

    }

    @Override
    public void visit(AggSpecAbsSum absSum) {

    }

    @Override
    public void visit(AggSpecCountDistinct countDistinct) {

    }

    @Override
    public void visit(AggSpecDistinct distinct) {

    }

    @Override
    public void visit(AggSpecGroup group) {

    }

    @Override
    public void visit(AggSpecAvg avg) {

    }

    @Override
    public void visit(AggSpecFirst first) {

    }

    @Override
    public void visit(AggSpecFormula formula) {

    }

    @Override
    public void visit(AggSpecLast last) {

    }

    @Override
    public void visit(AggSpecMax max) {

    }

    @Override
    public void visit(AggSpecMedian median) {

    }

    @Override
    public void visit(AggSpecMin min) {

    }

    @Override
    public void visit(AggSpecPercentile pct) {

    }

    @Override
    public void visit(AggSpecSortedFirst sortedFirst) {

    }

    @Override
    public void visit(AggSpecSortedLast sortedLast) {

    }

    @Override
    public void visit(AggSpecStd std) {

    }

    @Override
    public void visit(AggSpecSum sum) {

    }

    @Override
    public void visit(AggSpecUnique unique) {

    }

    @Override
    public void visit(AggSpecWAvg wAvg) {

    }

    @Override
    public void visit(AggSpecWSum wSum) {

    }

    @Override
    public void visit(AggSpecVar var) {

    }
}
