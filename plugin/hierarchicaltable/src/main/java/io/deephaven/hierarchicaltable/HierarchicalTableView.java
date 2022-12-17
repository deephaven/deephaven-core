package io.deephaven.hierarchicaltable;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.Collection;

/**
 * Server-side "view" object representing a client's snapshot target for hierarchical table data. This associates four
 * different kinds of information to fully describe the view:
 * <ul>
 * <li>The base version of the view, which should be used when applying new transformations without this view's
 * transformations</li>
 * <li>The {@link HierarchicalTable} instance associated with this view</li>
 * <li>Key {@link Table} information to be used when
 * {@link HierarchicalTable#snapshot(HierarchicalTable.SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])
 * snapshotting} the {@link HierarchicalTable}</li>
 * <li>Transformations (e.g. filters or sorts) that were applied to the base view's {@link HierarchicalTable} to produce
 * this view's instance</li>
 * </ul>
 */
public class HierarchicalTableView<TYPE extends HierarchicalTable<TYPE>> extends LivenessArtifact {

    private final HierarchicalTableView<TYPE> base;

    private final TYPE hierarchicalTable;

    private final Table keyTable;
    private final ColumnName keyTableActionColumn;

    private final Collection<? extends Filter> filters;
    private final Collection<SortColumn> sorts;

    HierarchicalTableView(
            @NotNull final HierarchicalTableView<TYPE> base,
            @NotNull final TYPE hierarchicalTable,
            @NotNull final Table keyTable,
            @NotNull final ColumnName keyTableActionColumn,
            @NotNull final Collection<Filter> filters,
            @NotNull final Collection<SortColumn> sorts) {
        this.base = base;
        this.hierarchicalTable = hierarchicalTable;
        this.keyTable = keyTable;
        this.keyTableActionColumn = keyTableActionColumn;
        this.filters = filters;
        this.sorts = sorts;
    }

    boolean isBaseline() {
        return this == base;
    }



}
