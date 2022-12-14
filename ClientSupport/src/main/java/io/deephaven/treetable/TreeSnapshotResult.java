/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.base.Pair;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.by.TreeConstants;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * The result of the {@link TreeSnapshotQuery}. It contains
 * <ul>
 * <li>The total tree size</li>
 * <li>The start and end indices of the snapshot</li>
 * <li>The data for the snapshot</li>
 * <li>A list up updated {@link TableDetails} containing updated expanded children</li>
 * <li>A boolean indicating whether the tree table was ready to process the request</li>
 * </ul>
 *
 * @implNote If wasReady is false, clients should re-issue the snapshot query.
 */
public class TreeSnapshotResult {
    private static final TableDetails[] ZERO_LENGTH_TD_ARRAY = new TableDetails[0];

    private final long treeSize;
    private final long snapshotStart;
    private final long snapshotEnd;
    private final Table updatedSource;
    private final Object[] data;
    private final TableDetails[] tableData;
    private final Object[] tableKeyColumn;
    private final BitSet childPresenceColumn;

    // Constituent data
    private final Pair<String, Object>[] constituentData;

    /**
     * Create an empty result.
     */
    TreeSnapshotResult() {
        treeSize = snapshotStart = snapshotEnd = -1;
        updatedSource = null;
        data = CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY;
        tableData = ZERO_LENGTH_TD_ARRAY;
        tableKeyColumn = CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY;
        childPresenceColumn = null;
        constituentData = null;
    }

    /**
     * Create a result populated with data, and potentially a new source table.
     *
     * @param updatedSource An updated source table. Should be set to null if it hasn't changed.
     * @param treeSize The total size of tree taking into account currently expanded rows.
     * @param data An array of arrays containing each data column for the snapshot.
     * @param tableData The list of {@link TableDetails} describing the state of the tree as of this TSQ.
     * @param tableKeyColumn A column containing the table key of the parent table of each row.
     * @param childPresenceColumn A bitset where each bit represents if that row has children.
     * @param start The actual start of the snapshot in viewport coordinates.
     * @param end The actual end of the snapshot in viewport coordinates.
     */
    TreeSnapshotResult(@Nullable Table updatedSource,
            long treeSize,
            Object[] data,
            TableDetails[] tableData,
            Object[] tableKeyColumn,
            BitSet childPresenceColumn,
            long start,
            long end,
            Pair<String, Object>[] constituentData) {
        this.tableData = tableData;
        this.data = data;
        this.treeSize = treeSize;
        this.snapshotStart = start;
        this.snapshotEnd = end;
        this.tableKeyColumn = tableKeyColumn;
        this.childPresenceColumn = childPresenceColumn;
        this.updatedSource = updatedSource;
        this.constituentData = constituentData;
    }

    public long getTreeSize() {
        return treeSize;
    }

    public long getSnapshotStart() {
        return snapshotStart;
    }

    public long getSnapshotEnd() {
        return snapshotEnd;
    }

    @NotNull
    public Object[] getData() {
        return data;
    }

    @NotNull
    public List<TableDetails> getTableData() {
        return tableData.length == 0 ? Collections.emptyList() : Arrays.asList(tableData);
    }

    @NotNull
    public Object[] getTableKeyColumn() {
        return tableKeyColumn;
    }

    @Nullable
    public BitSet getChildPresenceColumn() {
        return childPresenceColumn;
    }

    @Nullable
    public Pair<String, Object>[] getConstituentData() {
        return constituentData;
    }

    @TestUseOnly
    @Nullable
    Table asTable(Table originalTree) {
        final List<String> columnNames = originalTree.getDefinition().getColumnNames();
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>();

        if (data == null) {
            return null;
        }

        for (int i = 0; i < data.length; i++) {
            if (data[i] != null) {
                final ColumnDefinition<?> colDef = originalTree.getDefinition().getColumn(columnNames.get(i));
                // noinspection unchecked
                sources.put(columnNames.get(i), InMemoryColumnSource.getImmutableMemoryColumnSource(data[i],
                        colDef.getDataType(), colDef.getComponentType()));
            }
        }

        sources.put(TreeConstants.TABLE_KEY_COLUMN,
                InMemoryColumnSource.getImmutableMemoryColumnSource(tableKeyColumn));
        sources.put(TreeConstants.CHILD_PRESENCE_COLUMN, new BitSetColumnSource(childPresenceColumn));

        return new QueryTable(
                RowSetFactory.flat((snapshotEnd - snapshotStart) + 1).toTracking(),
                sources);
    }

    public Table getUpdatedSource() {
        return updatedSource;
    }

    public static class Descriptor {
        private static final long serialVersionUID = 3L;

        private final long treeSize;
        private final long start;
        private final long end;
        private final Object[] data;
        private final TableDetails[] tableData;
        private final Object[] tableKeyColumn;
        private final BitSet childPresenceColumn;
        private final Table updatedSource;
        private final Pair<String, Object>[] constituentData;

        Descriptor(long treeSize, Object[] data, TableDetails[] tableData, long start, long end,
                Object[] tableKeyColumn,
                BitSet childPresenceColumn, Table updatedSource, Pair<String, Object>[] constituentData) {
            this.treeSize = treeSize;
            this.data = data;
            this.tableData = tableData;
            this.start = start;
            this.end = end;
            this.tableKeyColumn = tableKeyColumn;
            this.childPresenceColumn = childPresenceColumn;
            this.updatedSource = updatedSource;
            this.constituentData = constituentData;
        }

        public long getTreeSize() {
            return treeSize;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public Object[] getData() {
            return data;
        }

        public List<TableDetails> getTableData() {
            return Arrays.asList(tableData);
        }

        public Table getUpdatedSource() {
            return updatedSource;
        }

        public Object[] getTableKeyColumn() {
            return tableKeyColumn;
        }

        public BitSet getChildPresenceColumn() {
            return childPresenceColumn;
        }

        public Pair<String, Object>[] getConstituentData() {
            return constituentData;
        }
    }
}
