/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.engine.table.impl.HierarchicalTable;
import io.deephaven.engine.table.impl.HierarchicalTableInfo;
import io.deephaven.engine.table.impl.RollupInfo;
import io.deephaven.engine.table.impl.TreeTableInfo;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.table.sort.SortDirective;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;

/**
 * A query that fetches a flat viewport-ready snapshot of a tree table, taking into account the set of expanded rows at
 * each level.
 */
public class TreeSnapshotQuery implements Function<Table, TreeSnapshotResult> {

    private final TreeTableClientTableManager.Client client;

    // TODO-RWC:
    //   1. We can assume that viewports are a single range.
    //   2. Columns can be sparse.
    //   3. baseTableId is the Ticket (export) for the hierarchical table
    //   4. How best to communicate rolled up vs. constituent columns (rollup-specific)
    //   5. Replace tablesByKey with a Ticket for a Table of expanded rows.
    private final long firstViewportRow;
    private final long lastViewportRow;
    private final int baseTableId;
    private final BitSet columns;

    private final WhereFilter[] filters;

    private final List<SortDirective> directives;
    private final Map<Object, TableDetails> tablesByKey;
    private final EnumSet<Operation> includedOps;

    public enum Operation {
        Expand, Contract, FilterChanged, SortChanged, Close
    }

    /**
     * Construct a new query that will create a flat snapshot of the tree table using a flat viewport beginning at the
     * specified rows and columns, applying the specified sorts and filters if required to fetch tables
     *
     * @param baseId The id of the base table to be used as a key to manage this client's state.
     * @param tablesByKey The tables within the tree for which viewports are being tracked, separated by table key.
     * @param firstRow The first row of the flat viewport
     * @param lastRow The last row of the flat viewport
     * @param columns The columns to include in the viewport
     * @param filters The filters to apply to new tables.
     * @param sorts The sorts to apply to new tables
     * @param client The CLIENT_TYPE instance
     * @param includedOps The set of operations the client has performed since the last TSQ.
     */
    public TreeSnapshotQuery(int baseId, Map<Object, TableDetails> tablesByKey,
            long firstRow, long lastRow, BitSet columns,
            @NotNull WhereFilter[] filters, @NotNull List<SortDirective> sorts,
            TreeTableClientTableManager.Client client, EnumSet<Operation> includedOps) {
        this.client = client;
        Assert.leq(firstRow, "firstRow", lastRow, "lastRow");
        Assert.leq(lastRow - firstRow, "lastRow - firstRow", Integer.MAX_VALUE, "Integer.MAX_VALUE");
        this.tablesByKey = tablesByKey;

        firstViewportRow = firstRow;
        lastViewportRow = lastRow;
        this.columns = columns;
        this.filters = filters;
        this.directives = sorts;
        this.includedOps = includedOps;
        this.baseTableId = baseId;
    }

    @Override
    @ConcurrentMethod
    public TreeSnapshotResult apply(Table arg) {
        if (!(arg instanceof HierarchicalTable)) {
            throw new IllegalArgumentException("Input table was not a hierarchical table");
        }

        final HierarchicalTableInfo sourceInfoAttr = ((HierarchicalTable) arg).getInfo();
        if (sourceInfoAttr instanceof TreeTableInfo) {
            return new TreeTableSnapshotImpl(baseTableId, (HierarchicalTable) arg, tablesByKey,
                    firstViewportRow, lastViewportRow, columns, filters, directives, client, includedOps).getSnapshot();
        } else if (sourceInfoAttr instanceof RollupInfo) {
            return new RollupSnapshotImpl(baseTableId, (HierarchicalTable) arg, tablesByKey,
                    firstViewportRow, lastViewportRow, columns, filters, directives, client, includedOps).getSnapshot();
        }

        throw new IllegalStateException("Could not determine tree table type");
    }

    @Override
    public String toString() {
        return "TreeSnapshotQuery{" +
                "firstViewportRow=" + firstViewportRow +
                ", lastViewportRow=" + lastViewportRow +
                ", columns=" + (columns == null ? "(null)" : FormatBitSet.formatBitSetAsString(columns)) +
                ", filters=" + Arrays.toString(filters) +
                ", directives=" + directives +
                ", tablesByKey.size()=" + tablesByKey.size() +
                '}';
    }
}
