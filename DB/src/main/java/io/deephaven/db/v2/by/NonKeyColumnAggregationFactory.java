package io.deephaven.db.v2.by;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class NonKeyColumnAggregationFactory implements AggregationContextFactory {
    private final IterativeChunkedOperatorFactory iterativeChunkedOperatorFactory;

    public NonKeyColumnAggregationFactory(
        IterativeChunkedOperatorFactory iterativeChunkedOperatorFactory) {
        this.iterativeChunkedOperatorFactory = iterativeChunkedOperatorFactory;
    }

    @Override
    public boolean allowKeyOnlySubstitution() {
        return true;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table,
        @NotNull final String... groupByColumns) {
        return getAllColumnOperators(table, groupByColumns, iterativeChunkedOperatorFactory);
    }

    private static AggregationContext getAllColumnOperators(Table withView,
        String[] groupByNameArray, IterativeChunkedOperatorFactory iterativeOperatorStateFactory) {
        final Set<String> groupByNames = new HashSet<>(Arrays.asList(groupByNameArray));
        final int operatorColumnCount = withView.getColumnSourceMap().size() - groupByNames.size();

        final List<IterativeChunkedAggregationOperator> operators =
            new ArrayList<>(operatorColumnCount);
        final List<ChunkSource.WithPrev<Values>> inputColumns =
            new ArrayList<>(operatorColumnCount);
        final List<String> inputNames = new ArrayList<>(operatorColumnCount);

        withView.getColumnSourceMap().forEach((name, columnSource) -> {
            if (groupByNames.contains(name)) {
                return;
            }

            final Class<?> type = columnSource.getType();

            // For DBDateTime columns, the in-memory source uses longs internally, and all supported
            // aggregations (i.e. min and max) work correctly against longs.
            final ColumnSource inputSource = columnSource.getType() == DBDateTime.class
                ? ReinterpretUtilities.dateTimeToLongSource(columnSource)
                : columnSource;

            final IterativeChunkedAggregationOperator chunkedOperator =
                iterativeOperatorStateFactory.getChunkedOperator(type, name, false);
            if (chunkedOperator != null) {
                // noinspection unchecked
                inputColumns.add(inputSource);
                inputNames.add(name);
                operators.add(chunkedOperator);
            }
        });

        final String[][] inputNameArray = new String[inputNames.size()][1];
        for (int ii = 0; ii < inputNames.size(); ++ii) {
            inputNameArray[ii][0] = inputNames.get(ii);
        }

        // noinspection unchecked
        return new AggregationContext(operators.toArray(
            IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY),
            inputNameArray,
            inputColumns.toArray(ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY));
    }

    @Override
    public String toString() {
        return iterativeChunkedOperatorFactory.toString();
    }
}
