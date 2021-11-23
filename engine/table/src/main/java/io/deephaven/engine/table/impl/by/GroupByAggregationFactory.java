package io.deephaven.engine.table.impl.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An {@link AggregationContextFactory} used in the implementation of {@link Table#groupBy}.
 */
public class GroupByAggregationFactory implements AggregationContextFactory {

    private static final GroupByAggregationFactory INSTANCE = new GroupByAggregationFactory();

    public static AggregationContextFactory getInstance() {
        return INSTANCE;
    }

    private GroupByAggregationFactory() {}

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
                new IterativeChunkedAggregationOperator[] {new GroupByChunkedOperator((QueryTable) inputTable, true,
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
