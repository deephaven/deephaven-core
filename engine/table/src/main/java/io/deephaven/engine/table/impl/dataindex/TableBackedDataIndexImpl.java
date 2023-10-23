package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
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

import static io.deephaven.engine.table.impl.by.AggregationProcessor.EXPOSED_GROUP_ROW_SETS;

/**
 * This class creates a data index for a table. The index is a table containing the key column(s) and the RowSets that
 * contain these values. DataIndexes may be loaded from storage or created in-memory using aggregations.
 */
public class TableBackedDataIndexImpl extends AbstractDataIndex {
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
            @NotNull final List<ColumnSource<?>> keySources) {

        this.sourceTable = sourceTable;

        // Create an array to hold the key column names from the source table (and replicated into the index table).
        keyColumnNames = new String[keySources.size()];

        // Create an in-order reverse lookup map for the key columnn names.
        keyColumnMap = new WeakHashMap<>(keySources.size());
        for (int ii = 0; ii < keySources.size(); ii++) {
            ColumnSource<?> keySource = keySources.get(ii);

            // Find the column name in the source table and add to the map.
            for (final Map.Entry<String, ? extends ColumnSource<?>> entry : sourceTable.getColumnSourceMap()
                    .entrySet()) {
                if (keySource == entry.getValue()) {
                    final String columnName = entry.getKey();
                    keyColumnMap.put(keySource, columnName);
                    keyColumnNames[ii] = columnName;
                    break;
                }
            }
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
    public @Nullable Table table() {
        if (indexTable == null) {
            // TODO: break the hard reference from the index table to the source table. Otherwise this index will keep
            // the source table from being garbage collected.

            // Create the index table, grouped by the key column sources.
            indexTable = QueryPerformanceRecorder
                    .withNugget("Build Table Backed Data Index [" + String.join(", ", keyColumnNames) + "]", () -> {
                        final Table groupedTable = sourceTable
                                .aggNoMemo(
                                        AggregationControl.IGNORE_GROUPING,
                                        AggregationProcessor.forExposeGroupRowSets(),
                                        false,
                                        null,
                                        ColumnName.from(keyColumnNames));

                        lookupFunction = AggregationProcessor.getRowLookup(groupedTable);
                        Assert.neqNull(lookupFunction, "AggregationRowLookup lookupFunction should never be null");

                        return groupedTable.renameColumns(
                                Collections.singleton(Pair.of(EXPOSED_GROUP_ROW_SETS, ColumnName.of(INDEX_COL_NAME))));
                    });
        }
        return indexTable;
    }

    @Override
    public @Nullable RowSetLookup rowSetLookup() {
        return (Object key) -> {
            // Pass the object to the aggregation lookup, then return the row set at that position.
            final int position = lookupFunction.get(key);
            return (RowSet) indexTable.getColumnSource(rowSetColumnName()).get(position);
        };
    }

    @Override
    public @NotNull PositionLookup positionLookup() {
        return (Object key) -> {
            // Pass the object to the aggregation lookup, then return the resulting position
            return lookupFunction.get(key);
        };
    }

    @Override
    public @Nullable Table prevTable() {
        if (!isRefreshing()) {
            // This index is static, so prev==current
            return table();
        }
        final Table indexTable = table();

        // Return a table containing the previous values of the index table.
        final TrackingRowSet prevRowSet = indexTable.getRowSet().copyPrev().toTracking();
        final Map<String, ColumnSource<?>> prevColumnSourceMap = new LinkedHashMap<>();
        indexTable.getColumnSourceMap().forEach((columnName, columnSource) -> {
            prevColumnSourceMap.put(columnName, columnSource.getPrevSource());
        });

        final Table prevTable = new QueryTable(prevRowSet, prevColumnSourceMap);
        return prevTable;
    }

    @Override
    public boolean isRefreshing() {
        return indexTable.isRefreshing();
    }
}

