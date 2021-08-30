/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.utils.freezeby.FreezeByCountOperator;
import io.deephaven.db.v2.utils.freezeby.FreezeByOperator;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class FreezeByAggregationFactory implements AggregationContextFactory {
    @Override
    public boolean allowKeyOnlySubstitution() {
        return true;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table,
        @NotNull final String... groupByColumns) {
        return getAllColumnOperators(table, groupByColumns);
    }

    private static AggregationContext getAllColumnOperators(Table withView,
        String[] groupByNameArray) {
        final Set<String> groupByNames = new HashSet<>(Arrays.asList(groupByNameArray));
        final int operatorCount = withView.getColumnSourceMap().size() - groupByNames.size() + 1;

        final List<IterativeChunkedAggregationOperator> operators = new ArrayList<>(operatorCount);
        final List<ChunkSource.WithPrev<Values>> inputColumns = new ArrayList<>(operatorCount);
        final List<String> inputNames = new ArrayList<>(operatorCount - 1);

        final FreezeByCountOperator countOperator = new FreezeByCountOperator();
        inputColumns.add(null);
        operators.add(countOperator);

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

            // noinspection unchecked
            inputColumns.add(inputSource);
            inputNames.add(name);
            operators.add(new FreezeByOperator(type, name, countOperator));
        });

        final String[][] inputNameArray = new String[inputNames.size() + 1][1];
        inputNameArray[0] = CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
        for (int ii = 0; ii < inputNames.size(); ++ii) {
            inputNameArray[ii + 1][0] = inputNames.get(ii);
        }

        // noinspection unchecked
        return new AggregationContext(operators.toArray(
            IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY),
            inputNameArray,
            inputColumns.toArray(ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY));
    }

    @Override
    public String toString() {
        return "FreezeBy";
    }
}
