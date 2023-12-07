package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.ColumnName;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.by.AggregationControl;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.by.AggregationProcessor.EXPOSED_GROUP_ROW_SETS;

/**
 * This class creates a data index for a table. The index is a table containing the key column(s) and the RowSets that
 * contain these values. DataIndexes may be loaded from storage or created in-memory using aggregations.
 */
public class TableBackedDataIndexImpl extends BaseDataIndex {
    /** The table containing the index. Consists of sorted key column(s) and an associated RowSet column. */
    private Table indexTable;

    @NotNull
    private final QueryTable sourceTable;

    @NotNull
    private final WeakHashMap<ColumnSource<?>, String> keyColumnMap;

    @NotNull
    final String[] keyColumnNames;

    private AggregationRowLookup lookupFunction;

    public TableBackedDataIndexImpl(@NotNull final QueryTable sourceTable,
            @NotNull final String[] keyColumnNames) {

        this.sourceTable = sourceTable;
        this.keyColumnNames = keyColumnNames;
        List<ColumnSource<?>> keySources = Arrays.stream(keyColumnNames).map(sourceTable::getColumnSource)
                .collect(Collectors.toList());

        // Create an in-order reverse lookup map for the key columnn names.
        keyColumnMap = new WeakHashMap<>(keySources.size());
        for (int ii = 0; ii < keySources.size(); ii++) {
            final ColumnSource<?> keySource = keySources.get(ii);
            final String keyColumnName = keyColumnNames[ii];
            keyColumnMap.put(keySource, keyColumnName);
        }

        // We will defer the actual index creation until it is needed.
    }

    @Override
    public String[] keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    public Map<ColumnSource<?>, String> keyColumnMap() {
        return keyColumnMap;
    }

    @Override
    public String rowSetColumnName() {
        return INDEX_COL_NAME;
    }

    @Override
    @NotNull
    public Table table() {
        if (indexTable != null) {
            return indexTable;
        }
        synchronized (this) {
            // Test again under the lock.
            if (indexTable != null) {
                return indexTable;
            }

            // Create the index table, grouped by the key column sources.
            indexTable = QueryPerformanceRecorder
                    .withNugget("Build Table Backed Data Index [" + String.join(", ", keyColumnNames) + "]", () -> {
                        final QueryTable groupedTable = sourceTable
                                .aggNoMemo(
                                        AggregationControl.IGNORE_GROUPING,
                                        AggregationProcessor.forExposeGroupRowSets(),
                                        false,
                                        null,
                                        ColumnName.from(keyColumnNames));

                        lookupFunction = AggregationProcessor.getRowLookup(groupedTable);
                        Assert.neqNull(lookupFunction, "AggregationRowLookup lookupFunction should never be null");

                        return indexTableWrapper(groupedTable, EXPOSED_GROUP_ROW_SETS.name(), INDEX_COL_NAME);
                    });
        }
        return indexTable;
    }

    @Override
    public @Nullable RowSetLookup rowSetLookup() {
        final ColumnSource<RowSet> rowSetColumnSource = rowSetColumn();
        return (Object key, boolean usePrev) -> {
            // Pass the object to the position lookup and get the resulting position.
            final int position = lookupFunction.get(key);
            if (position == AggregationRowLookup.DEFAULT_UNKNOWN_ROW) {
                return null;
            }

            // Aggregations return a dense result, so this position can be used directly as a row key.
            if (usePrev) {
                return rowSetColumnSource.getPrev(position);
            } else {
                return rowSetColumnSource.get(position);
            }
        };
    }

    @Override
    public @NotNull PositionLookup positionLookup() {
        return (Object key, boolean usePrev) -> {
            // Pass the object to the aggregation lookup, then return the resulting position. This index will be
            // correct in prev or current space because of the aggregation's hash-based lookup.
            return lookupFunction.get(key);
        };
    }

    @Override
    public boolean isRefreshing() {
        return sourceTable.isRefreshing();
    }

    @Override
    public boolean validate() {
        return true;
    }
}

