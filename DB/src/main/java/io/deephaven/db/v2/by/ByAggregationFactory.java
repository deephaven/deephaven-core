package io.deephaven.db.v2.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An {@link AggregationContextFactory} used in the implementation of {@link io.deephaven.db.tables.Table#by}.
 */
public class ByAggregationFactory implements AggregationContextFactory {

    private static final ByAggregationFactory INSTANCE = new ByAggregationFactory();

    public static AggregationContextFactory getInstance() {
        return INSTANCE;
    }

    private ByAggregationFactory() {}

    @Override
    public boolean allowKeyOnlySubstitution() {
        return true;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table inputTable,
            @NotNull final String... groupByColumnNames) {
        final Set<String> groupByColumnNameSet = Arrays.stream(groupByColumnNames).collect(Collectors.toSet());
        final String[] resultColumnNames = inputTable.getDefinition().getColumnNames().stream()
                .filter(cn -> !groupByColumnNameSet.contains(cn)).toArray(String[]::new);
        // noinspection unchecked
        return new AggregationContext(
                new IterativeChunkedAggregationOperator[] {new ByChunkedOperator((QueryTable) inputTable, true,
                        MatchPairFactory.getExpressions(resultColumnNames))},
                new String[][] {CollectionUtil.ZERO_LENGTH_STRING_ARRAY},
                new ChunkSource.WithPrev[] {null});
    }

    @Override
    public String toString() {
        return "By";
    }

    public static QueryTable by(@NotNull final QueryTable inputTable,
            @NotNull final String... groupByColumnNames) {
        return by(AggregationControl.DEFAULT_FOR_OPERATOR, inputTable, groupByColumnNames);
    }

    public static QueryTable by(@NotNull final QueryTable inputTable,
            @NotNull final SelectColumn[] groupByColumns) {
        return by(AggregationControl.DEFAULT_FOR_OPERATOR, inputTable, groupByColumns);
    }

    public static QueryTable by(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final String... groupByColumnNames) {
        return by(aggregationControl, inputTable, SelectColumnFactory.getExpressions(groupByColumnNames));
    }

    public static QueryTable by(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final SelectColumn[] groupByColumns) {
        return ChunkedOperatorAggregationHelper.aggregation(aggregationControl, getInstance(), inputTable,
                groupByColumns);
    }
}
