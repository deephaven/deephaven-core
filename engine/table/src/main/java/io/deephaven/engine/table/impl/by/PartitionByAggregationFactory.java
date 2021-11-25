package io.deephaven.engine.table.impl.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.LocalTableMap;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * An {@link AggregationContextFactory} used in the implementation of {@link Table#partitionBy}.
 */
public class PartitionByAggregationFactory implements AggregationContextFactory {

    private final boolean dropKeys;
    private final PartitionByChunkedOperator.AttributeCopier attributeCopier;
    private final List<Object> keysToPrepopulate;

    private PartitionByChunkedOperator operator;

    private PartitionByAggregationFactory(final boolean dropKeys,
            @NotNull final PartitionByChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate) {
        this.dropKeys = dropKeys;
        this.attributeCopier = attributeCopier;
        this.keysToPrepopulate = keysToPrepopulate;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table inputTable,
            @NotNull final String... groupByColumnNames) {
        final QueryTable adjustedInputTable =
                (QueryTable) (dropKeys ? inputTable.dropColumns(groupByColumnNames) : inputTable);
        // noinspection unchecked
        return new AggregationContext(
                new IterativeChunkedAggregationOperator[] {
                        operator = new PartitionByChunkedOperator(
                                (QueryTable) inputTable,
                                adjustedInputTable,
                                attributeCopier,
                                keysToPrepopulate,
                                groupByColumnNames)
                },
                new String[][] {CollectionUtil.ZERO_LENGTH_STRING_ARRAY},
                new ChunkSource.WithPrev[] {null});
    }

    @Override
    public String toString() {
        return "PartitionBy";
    }

    public static LocalTableMap partitionBy(@NotNull final QueryTable inputTable,
            final boolean dropKeys,
            @NotNull final PartitionByChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate,
            @NotNull final String... groupByColumnNames) {
        return partitionBy(AggregationControl.DEFAULT_FOR_OPERATOR, inputTable, dropKeys, attributeCopier,
                keysToPrepopulate, groupByColumnNames);
    }

    public static LocalTableMap partitionBy(@NotNull final QueryTable inputTable,
            final boolean dropKeys,
            @NotNull final PartitionByChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate,
            @NotNull final SelectColumn[] groupByColumns) {
        return partitionBy(AggregationControl.DEFAULT_FOR_OPERATOR, inputTable, dropKeys, attributeCopier,
                keysToPrepopulate, groupByColumns);
    }

    public static LocalTableMap partitionBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            final boolean dropKeys,
            @NotNull final PartitionByChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate,
            @NotNull final String... groupByColumnNames) {
        return partitionBy(aggregationControl, inputTable, dropKeys, attributeCopier, keysToPrepopulate,
                SelectColumnFactory.getExpressions(groupByColumnNames));
    }

    public static LocalTableMap partitionBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            final boolean dropKeys,
            @NotNull final PartitionByChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate,
            @NotNull final SelectColumn[] groupByColumns) {
        if (groupByColumns.length == 0) {
            return noKeyPartitionBy(inputTable);
        }
        final PartitionByAggregationFactory aggregationFactory =
                new PartitionByAggregationFactory(dropKeys, attributeCopier, keysToPrepopulate);
        ChunkedOperatorAggregationHelper.aggregation(aggregationControl, aggregationFactory, inputTable,
                groupByColumns);
        return aggregationFactory.operator.getTableMap();
    }

    public static LocalTableMap noKeyPartitionBy(@NotNull final QueryTable inputTable) {
        final LocalTableMap result = new LocalTableMap(null, inputTable.getDefinition());
        result.put(CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY, inputTable);
        return result;
    }
}
