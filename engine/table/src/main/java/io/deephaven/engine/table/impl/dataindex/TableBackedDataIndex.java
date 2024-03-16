//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.ColumnName;
import io.deephaven.base.verify.Assert;
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

import static io.deephaven.engine.table.impl.by.AggregationProcessor.EXPOSED_GROUP_ROW_SETS;

/**
 * This class creates a possibly-{@link BaseDataIndex#isRefreshing() refreshing} {@link BaseDataIndex} for a table. At
 * its core, the index is a {@link Table} containing the key column(s) and the {@link io.deephaven.engine.rowset.RowSet
 * RowSets} that contain these values.
 */
public class TableBackedDataIndex extends BaseDataIndex {

    /**
     * The table containing the index. Consists of sorted key column(s) and an associated
     * {@link io.deephaven.engine.rowset.RowSet} column.
     */
    private Table indexTable;

    @NotNull
    private final QueryTable sourceTable;

    @NotNull
    private final Map<ColumnSource<?>, String> keyColumnMap;

    @NotNull
    final String[] keyColumnNames;

    private AggregationRowLookup lookupFunction;

    public TableBackedDataIndex(
            @NotNull final QueryTable sourceTable,
            @NotNull final String[] keyColumnNames) {

        this.sourceTable = sourceTable;
        this.keyColumnNames = keyColumnNames;

        // Create an in-order reverse lookup map for the key column names.
        keyColumnMap = new LinkedHashMap<>(keyColumnNames.length);
        for (final String keyColumnName : keyColumnNames) {
            final ColumnSource<?> keySource = sourceTable.getColumnSource(keyColumnName);
            keyColumnMap.put(keySource, keyColumnName);
        }

        // We will defer the actual index creation until it is needed.
    }

    @Override
    public String[] keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnMap() {
        return keyColumnMap;
    }

    @Override
    @NotNull
    public String rowSetColumnName() {
        return ROW_SET_COLUMN_NAME;
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
        return sourceTable.isRefreshing();
    }

    @Override
    public boolean isValid() {
        return true;
    }
}
