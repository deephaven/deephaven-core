/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.hash.KeyedLongObjectHash;
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Base result class for operations that produce hierarchical tables, for example {@link Table#rollup rollup} and
 * {@link Table#tree(String, String) tree}.
 */
abstract class HierarchicalTableImpl<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>, IMPL_TYPE extends HierarchicalTableImpl<IFACE_TYPE, IMPL_TYPE>>
        extends BaseGridAttributes<IFACE_TYPE, IMPL_TYPE>
        implements HierarchicalTable<IFACE_TYPE> {

    /**
     * The source table that operations were applied to in order to produce this hierarchical table.
     */
    final QueryTable source;

    /**
     * The root node of the hierarchy.
     */
    final QueryTable root;

    protected HierarchicalTableImpl(
            @NotNull final Map<String, Object> initialAttributes,
            @NotNull final QueryTable source,
            @NotNull final QueryTable root) {
        super(initialAttributes);
        this.source = source;
        this.root = root;
    }

    @Override
    public Table getSource() {
        return source;
    }

    @Override
    public Table getRoot() {
        return root;
    }

    @Override
    public SnapshotState makeSnapshotState() {
        return new SnapshotStateImpl();
    }

    IFACE_TYPE noopResult() {
        if (getSource().isRefreshing()) {
            manageWithCurrentScope();
        }
        // noinspection unchecked
        return (IFACE_TYPE) this;
    }

    @Override
    protected void checkAvailableColumns(@NotNull final Collection<String> columns) {
        final Set<String> availableColumns = root.getDefinition().getColumnNameMap().keySet();
        final List<String> missingColumns =
                columns.stream().filter(column -> !availableColumns.contains(column)).collect(Collectors.toList());
        if (!missingColumns.isEmpty()) {
            throw new NoSuchColumnException(availableColumns, missingColumns);
        }
    }

    /**
     * Re-usable {@link SnapshotState} implementation, used for keeping track of clock information and node caching.
     */
    final class SnapshotStateImpl extends LivenessArtifact implements SnapshotState {

        private final KeyedLongObjectHashMap<NodeState> cachedNodes =
                new KeyedLongObjectHashMap<>(new NodeStateIdKey());
        private final KeyedLongObjectHash.ValueFactory<NodeState> nodeStateFactory = new NodeStateIdFactory();

        private int snapshotClock = 0;

        SnapshotStateImpl() {
            if (HierarchicalTableImpl.this.getSource().isRefreshing()) {
                manage(HierarchicalTableImpl.this);
            }
        }

        private void beginSnapshot() {
            snapshotClock++;
        }

        private void endSnapshot() {
            final Iterator<NodeState> cachedNodesIterator = cachedNodes.iterator();
            while (cachedNodesIterator.hasNext()) {
                final NodeState nodeState = cachedNodesIterator.next();
                if (!nodeState.visited(snapshotClock)) {
                    nodeState.release();
                    cachedNodesIterator.remove();
                }
            }
        }

        NodeState getNodeState(final long id) {
            return cachedNodes.putIfAbsent(id, nodeStateFactory);
        }

        @Override
        protected void destroy() {
            super.destroy();
            cachedNodes.forEach(NodeState::release);
            cachedNodes.clear();
        }

        private void verifyOwner(@NotNull final HierarchicalTableImpl owner) {
            if (HierarchicalTableImpl.this != owner) {
                throw new UnsupportedOperationException(
                        "Snapshot state must only be used with the hierarchical table it was created from");
            }
        }
    }

    private final class NodeStateIdKey extends KeyedLongObjectKey.BasicStrict<NodeState> {

        @Override
        public long getLongKey(@NotNull final NodeState nodeState) {
            return nodeState.id;
        }
    }

    private final class NodeStateIdFactory extends KeyedLongObjectHash.ValueFactory.Strict<NodeState> {

        @Override
        public NodeState newValue(final long nodeId) {
            return new NodeState(nodeId);
        }
    }

    /**
     * State tracking for nodes in this HierarchicalTableImpl.
     */
    final class NodeState {

        /**
         * Node identifier, a type-specific identifier that uniquely maps to a single table node in the
         * HierarchicalTable.
         */
        private final long id;

        /**
         * The base table at this node, before any node-level operations are applied.
         */
        private final Table base;

        /**
         * The table at this node, after any formatting or node-level filters have been applied.
         * <p>
         * This table may be used to determine node size or traverse children where order is irrelevant. While
         * formatting is not necessary for this use case, we apply it first in order to allow for memoization; we expect
         * most views to share the same formatting given that it is not configurable from the UI.
         */
        private Table filtered;

        /**
         * The table at this node, after any node-level sorting has been applied.
         */
        private Table sorted;

        /**
         * The sort {@link RowRedirection} that maps the appropriate unsorted row key space to {@link #sorted sorted's}
         * outer row space.
         */
        private RowRedirection sortRedirection;

        /**
         * Column sources to be used for retrieving data from {@link #sorted sorted}.
         */
        private ColumnSource<?>[] reinterpretedSources;

        /**
         * The last {@link SnapshotStateImpl#snapshotClock snapshot clock} value this node was visited on.
         */
        private int visitedSnapshotClock;

        private NodeState(final long id) {
            this.id = id;
            this.base = nodeIdToNodeBaseTable(id);
            // NB: No current implementation requires NodeState to ensure liveness for its base. If that changes,
            // we'll need to add liveness retention for base here.
        }

        private void ensurePreparedForSize() {
            if (filtered == null) {
                filtered = applyNodeFormatsAndFilters(id, base);
                if (filtered != base && filtered.isRefreshing()) {
                    filtered.retainReference();
                }
            }
        }

        private void ensurePreparedForData() {
            if (sorted == null) {
                ensurePreparedForSize();
                sorted = applyNodeSorts(id, filtered);
                if (sorted != filtered) {
                    // NB: We can safely assume that sorted != base if sorted != filtered
                    if (sorted.isRefreshing()) {
                        sorted.retainReference();
                    }
                    // NB: We could drop our reference to filtered here, but there's no real reason to do it now rather
                    // than in release
                    sortRedirection = SortOperation.getRowRedirection(sorted);
                }
                reinterpretedSources = sorted.getColumnSources().stream()
                        .map(ReinterpretUtils::maybeConvertToPrimitive)
                        .toArray(ColumnSource[]::new);
            }
        }

        /**
         * @return The internal identifier for this node
         */
        long getId() {
            return id;
        }

        /**
         * @param snapshotClock The {@link SnapshotStateImpl#snapshotClock snapshot clock} value
         * @return The node Table after any structural modifications (e.g. formatting and filtering) have been applied
         */
        Table getTableForSize(final int snapshotClock) {
            ensurePreparedForSize();
            maybeWaitForSatisfaction(filtered);
            visitedSnapshotClock = snapshotClock;
            return filtered;
        }

        /**
         * @param snapshotClock The {@link SnapshotStateImpl#snapshotClock snapshot clock} value
         * @return The node Table after any structural (e.g. formatting and filtering) or ordering modifications (e.g.
         *         sorting) have been applied
         */
        Table getTableForData(final int snapshotClock) {
            ensurePreparedForData();
            maybeWaitForSatisfaction(sorted);
            visitedSnapshotClock = snapshotClock;
            return sorted;
        }

        /**
         * @return Redirections that need to be applied when determining child order in the result of
         *         {@link #getTableForData(int)}, which must have been called before this method for the current
         *         snapshot
         */
        @Nullable
        RowRedirection getRedirectionForData() {
            return sortRedirection;
        }

        /**
         * @return The sources that should be used when retrieving actual data from the result of
         *         {@link #getTableForData(int)}, which must have been called before this method for the current
         *         snapshot
         */
        ColumnSource<?>[] getReinterpretedSourcesForData() {
            return reinterpretedSources;
        }

        /**
         * @param snapshotClock The {@link SnapshotStateImpl#snapshotClock snapshot clock} value
         * @return Whether this NodeState was last visited in on the supplied snapshot clock value
         */
        private boolean visited(final long snapshotClock) {
            return snapshotClock == visitedSnapshotClock;
        }

        private void release() {
            if (sorted != null && sorted != filtered && sorted.isRefreshing()) {
                sorted.dropReference();
            }
            if (filtered != null && filtered != base && filtered.isRefreshing()) {
                filtered.dropReference();
            }
        }
    }

    enum KeyAction {
        // @formatter:off
        Undefined((byte) 0),
        Expand(EXPAND),
        ExpandAll(EXPAND_ALL),
        Contract(CONTRACT);
        // @formatter:on

        private final byte wireValue;

        KeyAction(byte wireValue) {
            this.wireValue = wireValue;
        }

        static KeyAction lookup(final byte wireValue) {
            switch (wireValue) {
                case EXPAND:
                    return Expand;
                case EXPAND_ALL:
                    return ExpandAll;
                case CONTRACT:
                    return Contract;
                default:
                    throw new IllegalArgumentException("Unrecognized key action " + wireValue);
            }
        }
    }

    @Override
    public long fillSnapshotChunks(
            @NotNull final SnapshotState snapshotState,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn,
            @Nullable final BitSet columns,
            @NotNull final RowSequence rows,
            @NotNull final WritableChunk<? extends Values>[] destinations) {
        if (!rows.isContiguous()) {
            throw new UnsupportedOperationException(
                    "Only contiguous row sequences are supported for hierarchical table snapshots, input=" + rows);
        }
        // noinspection unchecked
        final SnapshotStateImpl typedSnapshotState = ((SnapshotStateImpl) snapshotState);
        typedSnapshotState.verifyOwner(this);

        final MutableLong expandedSize = new MutableLong();
        synchronized (typedSnapshotState) {
            // NB: Our snapshot control must be notification-aware, because if our sources tick we cannot guarantee that
            // we won't observe some newly created components on their instantiation step.
            final ConstructSnapshot.SnapshotControl control =
                    ConstructSnapshot.makeSnapshotControl(true, getSource().isRefreshing(), getSourceDependencies());
            ConstructSnapshot.callDataSnapshotFunction(getClass().getSimpleName(), control,
                    (final boolean usePrev, final long beforeClockValue) -> {
                        maybeWaitForStructuralSatisfaction();
                        expandedSize.setValue(0);
                        typedSnapshotState.beginSnapshot();

                        // TODO-RWC: Actual snapshot work.

                        return true;
                    });
            typedSnapshotState.endSnapshot();
        }
        return expandedSize.longValue();
    }

    /**
     * @param nodeKeyTable The table of node key values to create a node key source from
     * @return A {@link ChunkSource} of opaque node key values from {@code nodeKeyTable}
     */
    abstract ChunkSource.WithPrev<? extends Values> makeNodeKeySource(@NotNull Table nodeKeyTable);

    /**
     * @param childNodeKey The node key to map
     * @return The parent node key for {@code childNodeKey}, or {@code null} if {@code childNodeKey} represents the
     * root node key
     */
    @Nullable
    abstract Object nodeKeyToParentNodeKey(@Nullable Object childNodeKey, boolean usePrev);

    /**
     * @param nodeKey The node key to test
     * @return Whether {@code nodeKey} maps to the root node for this HierarchicalTableImpl
     */
    abstract boolean isRootNodeKey(@Nullable Object nodeKey);

    /**
     * @param nodeKey The node key to map
     * @return The internal identifier for {@code nodeKey}
     */
    abstract long nodeKeyToNodeId(@Nullable Object nodeKey);

    /**
     * @param nodeId The internal identifier to map
     * @return The base table at the node identified by {@code nodeId}, or {@code null} if the node is not expandable
     */
    abstract Table nodeIdToNodeBaseTable(long nodeId);

    /**
     * @param nodeId The internal identifier for this node
     * @param nodeBaseTable The base table at this node
     * @return The formatted and filtered table at this node, which may be {@code nodeBaseTable}
     */
    abstract Table applyNodeFormatsAndFilters(long nodeId, @NotNull Table nodeBaseTable);

    /**
     * @param nodeId The internal identifier for this node
     * @param nodeFilteredTable The formatted and filtered table at this node
     * @return The sorted table at this node, which may be {@code nodeFilteredTable}
     */
    abstract Table applyNodeSorts(long nodeId, @NotNull Table nodeFilteredTable);

    abstract NotificationStepSource[] getSourceDependencies();

    /**
     * Ensure that the internal data structures for this HierarchicalTableImpl (excluding the source dependencies and
     * node-level derived tables) are ready to proceed with the current snapshot.
     */
    abstract void maybeWaitForStructuralSatisfaction();

    /**
     * Wait until it's safe to use {@code table} for the current snapshot attempt.
     *
     * @param table The table to test
     */
    static void maybeWaitForSatisfaction(@Nullable final Table table) {
        maybeWaitForSatisfaction((NotificationQueue.Dependency) table);
    }

    /**
     * Wait until it's safe to use {@code dependency} for the current snapshot attempt.
     *
     * @param dependency The dependency to test
     */
    static void maybeWaitForSatisfaction(@Nullable final NotificationQueue.Dependency dependency) {
        ConstructSnapshot.maybeWaitForSatisfaction(dependency);
    }

    // TODO-RWC: Prune and split formats and sorts applied to both rollup node types from UI.
}
