//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.ColumnName;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.by.*;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.by.AggregationProcessor.EXPOSED_GROUP_ROW_SETS;

/**
 * This class creates a possibly-{@link AbstractDataIndex#isRefreshing() refreshing} {@link AbstractDataIndex} for a
 * table. At its core, the index is a {@link Table} containing the key column(s) and the
 * {@link io.deephaven.engine.rowset.RowSet RowSets} that contain these values.
 */
public class TableBackedDataIndex extends AbstractDataIndex {

    @NotNull
    private final Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn;

    @NotNull
    final List<String> keyColumnNames;

    private final boolean isRefreshing;

    private QueryTable sourceTable;

    /**
     * The lookup function for the index table. Note that this is always set before {@link #indexTable}.
     */
    private AggregationRowLookup lookupFunction;

    /**
     * The index table. Note that we use this as a barrier to ensure {@link #lookupFunction} is visible.
     */
    private volatile Table indexTable;

    public TableBackedDataIndex(
            @NotNull final QueryTable sourceTable,
            @NotNull final String... keyColumnNames) {
        this.keyColumnNames = List.of(Require.elementsNeqNull(keyColumnNames, "keyColumnNames"));

        // Create an in-order reverse lookup map for the key column names.
        keyColumnNamesByIndexedColumn = Collections.unmodifiableMap(
                Arrays.stream(keyColumnNames).collect(Collectors.toMap(
                        sourceTable::getColumnSource, Function.identity(), Assert::neverInvoked, LinkedHashMap::new)));

        isRefreshing = sourceTable.isRefreshing();

        // Defer the actual index table and lookup function creation until they are needed.
        this.sourceTable = sourceTable;
        if (isRefreshing) {
            manage(sourceTable);
        }
    }

    @Override
    @NotNull
    public List<String> keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
        return keyColumnNamesByIndexedColumn;
    }

    @Override
    @NotNull
    public Table table() {
        Table localIndexTable;
        if ((localIndexTable = indexTable) != null) {
            return localIndexTable;
        }
        synchronized (this) {
            if ((localIndexTable = indexTable) != null) {
                return localIndexTable;
            }

            return computeTable();
        }
    }

    /**
     * Compute {@link #indexTable} and {@link #lookupFunction}.
     *
     * @return The newly-computed index table
     */
    private Table computeTable() {
        final MutableObject<AggregationRowLookup> resultLookupFunction = new MutableObject<>();
        final Table resultIndexTable = QueryPerformanceRecorder.withNugget(String.format(
                "Build table-backed DataIndex for %s on [%s]",
                sourceTable.getDescription(), String.join(", ", keyColumnNames)), () -> {
                    try (final SafeCloseable ignored1 =
                            ExecutionContext.getContext().withUpdateGraph(sourceTable.getUpdateGraph()).open();
                            final SafeCloseable ignored2 = isRefreshing() ? LivenessScopeStack.open() : null) {
                        final QueryTable groupedTable = ChunkedOperatorAggregationHelper.aggregation(
                                AggregationControl.IGNORE_INDEXING, AggregationProcessor.forExposeGroupRowSets(),
                                sourceTable, false, null, ColumnName.from(keyColumnNames));

                        resultLookupFunction.setValue(AggregationProcessor.getRowLookup(groupedTable));
                        Assert.neqNull(resultLookupFunction.getValue(), "AggregationRowLookup");

                        final Table withWrappedRowSetSource =
                                indexTableWrapper(groupedTable, EXPOSED_GROUP_ROW_SETS.name(), ROW_SET_COLUMN_NAME);
                        if (isRefreshing()) {
                            manage(withWrappedRowSetSource);
                        }
                        return withWrappedRowSetSource;
                    }
                });
        lookupFunction = resultLookupFunction.getValue();
        indexTable = resultIndexTable;
        if (isRefreshing) {
            unmanage(sourceTable);
        }
        sourceTable = null;
        return resultIndexTable;
    }

    @Override
    @NotNull
    public RowKeyLookup rowKeyLookup() {
        table();
        return (final Object key, final boolean usePrev) -> {
            // Pass the object to the aggregation lookup, then return the resulting row key. This index will be
            // correct in prev or current space because of the aggregation's hash-based lookup.
            return lookupFunction.get(key);
        };
    }

    @Override
    public boolean isRefreshing() {
        return isRefreshing;
    }

    @Override
    public boolean isValid() {
        return true;
    }
}
