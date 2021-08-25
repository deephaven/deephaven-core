package io.deephaven.db.v2.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.v2.LocalTableMap;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * An {@link AggregationContextFactory} used in the implementation of {@link io.deephaven.db.tables.Table#byExternal}.
 */
public class ByExternalAggregationFactory implements AggregationContextFactory {

    private final boolean dropKeys;
    private final ByExternalChunkedOperator.AttributeCopier attributeCopier;
    private final List<Object> keysToPrepopulate;

    private ByExternalChunkedOperator operator;

    private ByExternalAggregationFactory(final boolean dropKeys,
            @NotNull final ByExternalChunkedOperator.AttributeCopier attributeCopier,
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
                        operator = new ByExternalChunkedOperator(
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
        return "ByExternal";
    }

    public static LocalTableMap byExternal(@NotNull final QueryTable inputTable,
            final boolean dropKeys,
            @NotNull final ByExternalChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate,
            @NotNull final String... groupByColumnNames) {
        return byExternal(AggregationControl.DEFAULT_FOR_OPERATOR, inputTable, dropKeys, attributeCopier,
                keysToPrepopulate, groupByColumnNames);
    }

    public static LocalTableMap byExternal(@NotNull final QueryTable inputTable,
            final boolean dropKeys,
            @NotNull final ByExternalChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate,
            @NotNull final SelectColumn[] groupByColumns) {
        return byExternal(AggregationControl.DEFAULT_FOR_OPERATOR, inputTable, dropKeys, attributeCopier,
                keysToPrepopulate, groupByColumns);
    }

    public static LocalTableMap byExternal(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            final boolean dropKeys,
            @NotNull final ByExternalChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate,
            @NotNull final String... groupByColumnNames) {
        return byExternal(aggregationControl, inputTable, dropKeys, attributeCopier, keysToPrepopulate,
                SelectColumnFactory.getExpressions(groupByColumnNames));
    }

    public static LocalTableMap byExternal(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            final boolean dropKeys,
            @NotNull final ByExternalChunkedOperator.AttributeCopier attributeCopier,
            @NotNull final List<Object> keysToPrepopulate,
            @NotNull final SelectColumn[] groupByColumns) {
        if (groupByColumns.length == 0) {
            return noKeyByExternal(inputTable);
        }
        final ByExternalAggregationFactory aggregationFactory =
                new ByExternalAggregationFactory(dropKeys, attributeCopier, keysToPrepopulate);
        ChunkedOperatorAggregationHelper.aggregation(aggregationControl, aggregationFactory, inputTable,
                groupByColumns);
        return aggregationFactory.operator.getTableMap();
    }

    public static LocalTableMap noKeyByExternal(@NotNull final QueryTable inputTable) {
        final LocalTableMap result = new LocalTableMap(null, inputTable.getDefinition());
        result.put(CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY, inputTable);
        return result;
    }
}
