//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.hierarchical;

import io.deephaven.api.*;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.DynamicNode;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;

/**
 * Interface for the result of {@link Table#tree(String, String)} tree} operations.
 */
public interface TreeTable extends HierarchicalTable<TreeTable> {

    /**
     * Get the identifier column from the {@link Table#tree(String, String) tree} operation.
     *
     * @return The identifier column
     */
    ColumnName getIdentifierColumn();

    /**
     * Get the parent identifier column from the {{@link Table#tree(String, String) tree} operation.
     *
     * @return The parent identifier column
     */
    ColumnName getParentIdentifierColumn();

    /**
     * Get the {@link TableDefinition} that should be exposed to node table consumers, e.g. UI-driven snapshots. This
     * excludes "internal" columns used to organize the tree or support operations, as well as the
     * {@link #getRowDepthColumn() row-depth column} and {@link #getRowExpandedColumn() row-expanded column}, but
     * includes formatting columns.
     *
     * @return The externally-visible node {@link TableDefinition}
     */
    TableDefinition getNodeDefinition();

    /**
     * Get a new TreeTable with {@code columns} designated for node-level filtering, in addition to any columns already
     * so-designated on {@code this} TreeTable.
     * <p>
     * Filters specified via {@link #withFilter(Filter)}, typically from the UI, that only use the designated node-level
     * filtering columns will be applied to the nodes during snapshots. If no node-filter columns are designated, no
     * filters will be handled at node level.
     * <p>
     * Filters that include other columns are handled by filtering the source table in a ancestor-preserving manner and
     * re-applying the tree operation to the result to produce a new TreeTable. Users of orphan promotion or other
     * strategies to govern the structure of the tree should carefully consider the structure of their data before
     * specifying node-filter columns.
     * <p>
     * Specifying node-filter columns represents a trade-off between performance (which is expected to be much better
     * for node-level filtering) and tree structural integrity (which may be lost since node-level filters are not
     * ancestor-preserving).
     *
     * @param columns The columns to designate
     * @return The new TreeTable
     */
    TreeTable withNodeFilterColumns(@NotNull Collection<? extends ColumnName> columns);

    /**
     * Apply a filter to the columns of this TreeTable in order to produce a new TreeTable.
     *
     * @param filter The filter to apply
     * @return The new TreeTable
     */
    TreeTable withFilter(@NotNull Filter filter);

    /**
     * Recorder for node-level operations to be applied when gathering snapshots.
     */
    interface NodeOperationsRecorder extends
            FormatOperationsRecorder<NodeOperationsRecorder>,
            SortOperationsRecorder<NodeOperationsRecorder>,
            FilterOperationsRecorder<NodeOperationsRecorder> {

        /**
         * Test if this NodeOperationsRecorder has recorded any operations.
         *
         * @return Whether this NodeOperationsRecorder has recorded any operations
         */
        boolean isEmpty();
    }

    /**
     * Get a {@link NodeOperationsRecorder recorder} for per-node operations to apply during snapshots.
     *
     * @return The new recorder
     */
    NodeOperationsRecorder makeNodeOperationsRecorder();

    /**
     * Get a new TreeTable that will apply the {@link NodeOperationsRecorder recorded} operations to nodes when
     * gathering snapshots.
     *
     * @param nodeOperations The node-level operations to apply. Must have been initially supplied by
     *        {@link #makeNodeOperationsRecorder()} from {@code this} TreeTable.
     * @return The new TreeTable
     */
    TreeTable withNodeOperations(@NotNull NodeOperationsRecorder nodeOperations);

    /**
     * Adapt a {@code source} {@link Table} to be used for a {@link Table#tree(String, String) tree} to ensure that the
     * result will have no orphaned nodes. Nodes whose parents do not exist will become children of the root node in the
     * resulting tree. The expected usage pattern is:
     * 
     * <pre>
     * TreeTable result = promoteOrphans(source, idColumn, parentColumn).tree(idColumn, parentColumn)
     * </pre>
     * 
     * @param source The source {@link Table}
     * @param idColumn The name of a column containing a unique identifier for a particular row in the table
     * @param parentColumn The name of a column containing the parent's identifier, {@code null} for rows that are part
     *        of the root table
     * @return A {@link Table} derived from {@code source} that has {@code null} as the parent for any nodes that would
     *         have been orphaned by a call to {@code source.tree(idColumn, parentColumn)}
     */
    static Table promoteOrphans(
            @NotNull final Table source,
            @NotNull final String idColumn,
            @NotNull final String parentColumn) {
        final ColumnName parent = ColumnName.of(parentColumn);
        final ColumnName identifier = ColumnName.of(idColumn);
        final ColumnName sentinel = ColumnName.of("__MATCHED_PARENT_IDENTIFIER__");
        final Selectable[] viewColumns = source.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .map((final String columnName) -> columnName.equals(parent.name())
                        ? Selectable.of(parent,
                                RawString.of("isNull(" + sentinel.name() + ") ? null : " + parent.name()))
                        : ColumnName.of(columnName))
                .toArray(Selectable[]::new);
        return LivenessScopeStack.computeEnclosed(
                () -> source
                        .naturalJoin(source,
                                List.of(JoinMatch.of(parent, identifier)),
                                List.of(JoinAddition.of(sentinel, identifier)))
                        .view(List.of(viewColumns)),
                source::isRefreshing,
                DynamicNode::isRefreshing);
    }
}
