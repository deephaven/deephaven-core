/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.iterators.ByteColumnIterator;
import io.deephaven.engine.table.iterators.ObjectColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.hash.*;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl.KeyTableNodeAction.*;
import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.callDataSnapshotFunction;
import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.makeSnapshotControl;

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

        private final KeyedLongObjectHashMap<NodeTableState> nodeTableStates =
                new KeyedLongObjectHashMap<>(new NodeTableStateIdKey());
        private final KeyedLongObjectHash.ValueFactory<NodeTableState> nodeTableStateFactory =
                new NodeTableStateIdFactory();

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
            final Iterator<NodeTableState> cachedNodesIterator = nodeTableStates.iterator();
            while (cachedNodesIterator.hasNext()) {
                final NodeTableState nodeTableState = cachedNodesIterator.next();
                if (!nodeTableState.visited(snapshotClock)) {
                    nodeTableState.release();
                    cachedNodesIterator.remove();
                }
            }
        }

        NodeTableState getNodeTableState(final long nodeId) {
            return nodeTableStates.putIfAbsent(nodeId, nodeTableStateFactory);
        }

        @Override
        protected void destroy() {
            super.destroy();
            nodeTableStates.forEach(NodeTableState::release);
            nodeTableStates.clear();
        }

        private void verifyOwner(@NotNull final HierarchicalTableImpl owner) {
            if (HierarchicalTableImpl.this != owner) {
                throw new UnsupportedOperationException(
                        "Snapshot state must only be used with the hierarchical table it was created from");
            }
        }
    }

    private final class NodeTableStateIdKey extends KeyedLongObjectKey.BasicStrict<NodeTableState> {

        @Override
        public long getLongKey(@NotNull final NodeTableState nodeTableState) {
            return nodeTableState.id;
        }
    }

    private final class NodeTableStateIdFactory extends KeyedLongObjectHash.ValueFactory.Strict<NodeTableState> {

        @Override
        public NodeTableState newValue(final long nodeId) {
            return new NodeTableState(nodeId);
        }
    }

    /**
     * State tracking for node tables in this HierarchicalTableImpl.
     */
    final class NodeTableState {

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

        private NodeTableState(final long nodeId) {
            this.id = nodeId;
            this.base = nodeIdToNodeBaseTable(nodeId);
            // NB: No current implementation requires NodeTableState to ensure liveness for its base. If that changes,
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
         * @return Whether this NodeTableState was last visited in on the supplied snapshot clock value
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

    enum KeyTableNodeAction {
        // @formatter:off
        /** A newly-created, undefined node. */
        Undefined,
        /** A node created indirectly, in order to parent another node. If this node is reachable post-extraction it
         * exists under an "expand-all" in order to provide a path to a contraction. */
        Linkage,
        /** A simple expanded node.*/
        Expand,
        /** A node whose descendants should all be expanded unless contracted. */
        ExpandAll,
        /** A node that should be contracted, if reachable as the descendant of an "expand-all". */
        Contract;
        // @formatter:on

        static KeyTableNodeAction lookup(final byte wireValue) {
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

    private static final class NodeDirectiveKey<TYPE extends NodeDirective>
            implements KeyedObjectKey<Object, TYPE> {

        private static final KeyedObjectKey<Object, ? extends NodeDirective> INSTANCE =
                new NodeDirectiveKey<>();

        private static <TYPE extends NodeDirective> KeyedObjectKey<Object, TYPE> getInstance() {
            // noinspection unchecked
            return (KeyedObjectKey<Object, TYPE>) INSTANCE;
        }

        private NodeDirectiveKey() {}

        @Override
        public Object getKey(@NotNull final NodeDirective directive) {
            return directive.getAction();
        }

        @Override
        public int hashKey(@Nullable final Object nodeKey) {
            if (nodeKey instanceof Object[]) {
                return Arrays.hashCode((Object[]) nodeKey);
            }
            return Objects.hashCode(nodeKey);
        }

        @Override
        public boolean equalKey(@Nullable final Object nodeKey, @NotNull final NodeDirective directive) {
            if (nodeKey instanceof Object[] && directive.getNodeKey() instanceof Object[]) {
                return Arrays.equals((Object[]) nodeKey, (Object[]) directive.getNodeKey());
            }
            return Objects.equals(nodeKey, directive.getNodeKey());
        }
    }

    /**
     * Information about a node that's referenced directly from the key table for a snapshot.
     */
    private static class NodeDirective {

        /**
         * The node key for this node.
         */
        private final Object nodeKey;

        /**
         * The action that should be taken for this node. Will be {@link KeyTableNodeAction#Undefined undefined} on
         * construction. Will be {@link KeyTableNodeAction#Linkage} if created in order to provide connectivity to
         * another node; this will be overwritten with the key table's action if found.
         */
        private KeyTableNodeAction action;

        private NodeDirective(@Nullable final Object nodeKey) {
            this.nodeKey = nodeKey;
            action = Undefined;
        }

        public Object getNodeKey() {
            return nodeKey;
        }

        void setAction(@NotNull final KeyTableNodeAction action) {
            Assert.neq(action, "action", Undefined, "Undefined");
            if (actionDefined()) {
                Assert.neq(action, "action", Linkage, "Linkage");
            }
            // Allow repeats, accept last
            this.action = action;
        }

        boolean actionDefined() {
            return action != Undefined;
        }

        KeyTableNodeAction getAction() {
            return action;
        }
    }

    /**
     * Information about a node that's referenced directly or indirectly from the key table for a snapshot, with
     * appropriate linkage to its children..
     */
    private static final class LinkedNodeDirective extends NodeDirective {

        /**
         * This is not a complete list of children, just those that occur in the key table for the current snapshot, or
         * are found in order to provide ancestors for such nodes.
         */
        private List<LinkedNodeDirective> children;

        private LinkedNodeDirective(@Nullable final Object nodeKey) {
            super(nodeKey);
        }

        private LinkedNodeDirective addChild(@NotNull final LinkedNodeDirective childInfo) {
            (children == null ? children = new ArrayList<>() : children).add(childInfo);
            return this;
        }

        private List<LinkedNodeDirective> getChildren() {
            return children;
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

        final Collection<NodeDirective> rawKeyTableNodeDirectives =
                maybeSnapshotKeyTableNodeDirectives(keyTable, keyTableActionColumn);

        final MutableLong expandedSize = new MutableLong();
        synchronized (typedSnapshotState) {
            // NB: Our snapshot control must be notification-aware, because if our sources tick we cannot guarantee that
            // we won't observe some newly created components on their instantiation step.
            final SnapshotControl sourceSnapshotControl =
                    makeSnapshotControl(true, getSource().isRefreshing(), getSourceDependencies());
            callDataSnapshotFunction(getClass().getSimpleName() + "-source", sourceSnapshotControl,
                    (final boolean usePrev, final long beforeClockValue) -> {
                        maybeWaitForStructuralSatisfaction();

                        final LinkedNodeDirective rootNodeDirective =
                                linkKeyTableNodeDirectives(rawKeyTableNodeDirectives, usePrev);

                        expandedSize.setValue(0);
                        typedSnapshotState.beginSnapshot();

                        // TODO-RWC: Actual snapshot work.

                        return true;
                    });
            typedSnapshotState.endSnapshot();
        }
        return expandedSize.longValue();
    }

    private Collection<NodeDirective> maybeSnapshotKeyTableNodeDirectives(
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn) {
        if (!keyTable.isRefreshing()) {
            return extractKeyTableNodeDirectives(keyTable, keyTableActionColumn, false);
        }
        final MutableObject<Collection<NodeDirective>> rootNodeInfoHolder = new MutableObject<>();
        final SnapshotControl keyTableSnapshotControl =
                makeSnapshotControl(false, true, (NotificationStepSource) keyTable);
        callDataSnapshotFunction(getClass().getSimpleName() + "-keys", keyTableSnapshotControl,
                (final boolean usePrev, final long beforeClockValue) -> {
                    rootNodeInfoHolder.setValue(extractKeyTableNodeDirectives(keyTable, keyTableActionColumn, usePrev));
                    return true;
                });
        return rootNodeInfoHolder.getValue();
    }

    /**
     * Extract the key table's node directives in "raw" form.
     *
     * @param keyTable The key table to extract node directives from
     * @param keyTableActionColumn See
     *        {@link #fillSnapshotChunks(SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])}
     * @param usePrev Whether to use the previous state of {@code keyTable}
     * @return A collection of node directives described by the key table
     */
    private Collection<NodeDirective> extractKeyTableNodeDirectives(
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn,
            final boolean usePrev) {
        final ChunkSource<? extends Values> nodeKeySource = maybePrevSource(makeNodeKeySource(keyTable), usePrev);
        final ChunkSource<? extends Values> actionSource = keyTableActionColumn == null ? null
                : maybePrevSource(keyTable.getColumnSource(keyTableActionColumn.name(), byte.class), usePrev);
        final KeyedObjectHashSet<Object, NodeDirective> directives =
                new KeyedObjectHashSet<>(NodeDirectiveKey.getInstance());
        try (final RowSet prevRowSet = usePrev ? keyTable.getRowSet().copyPrev() : null) {
            final RowSequence rowsToExtract = usePrev ? prevRowSet : keyTable.getRowSet();
            final ObjectColumnIterator<Object> nodeKeyIterator =
                    new ObjectColumnIterator<>(nodeKeySource, rowsToExtract);
            final ByteColumnIterator actionIterator =
                    actionSource == null ? null : new ByteColumnIterator(actionSource, rowsToExtract);
            while (nodeKeyIterator.hasNext()) {
                final Object nodeKey = nodeKeyIterator.next();
                final KeyTableNodeAction action = actionIterator == null ? Expand : lookup(actionIterator.nextByte());
                directives.putIfAbsent(nodeKey, NodeDirective::new).setAction(action);
            }
        }
        return directives;
    }

    private static <ATTR extends Any> ChunkSource<ATTR> maybePrevSource(
            @NotNull final ChunkSource.WithPrev<ATTR> chunkSource,
            final boolean usePrev) {
        return usePrev ? chunkSource.getPrevSource() : chunkSource;
    }

    /**
     * Convert the "raw" key table node directives into a tree structure that will allow hierarchical traversal.
     *
     * @param rawNodeDirectives The "raw" key table node directives
     * @param usePrev Whether to use the previous state of {@code this}
     * @return The root node directive of the resulting tree structure, or {@code null} if the root was not found
     */
    private LinkedNodeDirective linkKeyTableNodeDirectives(
            @NotNull final Collection<NodeDirective> rawNodeDirectives,
            final boolean usePrev) {
        LinkedNodeDirective rootNodeDirective = null;
        final KeyedObjectHashMap<Object, LinkedNodeDirective> processedNodeInfos =
                new KeyedObjectHashMap<>(NodeDirectiveKey.getInstance());
        final MutableObject<Object> parentNodeKeyHolder = new MutableObject<>();
        for (final NodeDirective raw : rawNodeDirectives) {
            Object nodeKey = raw.getNodeKey();
            LinkedNodeDirective linked = processedNodeInfos.putIfAbsent(nodeKey, LinkedNodeDirective::new);
            boolean needToFindAncestors = !linked.actionDefined();
            linked.setAction(raw.getAction()); // Overwrite "Undefined" or "Linkage" with the key table's action
            // Find ancestors iteratively until we hit the root or a node directive that already exists
            while (needToFindAncestors) {
                final Boolean parentNodeKeyFound = nodeKeyToParentNodeKey(nodeKey, usePrev, parentNodeKeyHolder);
                if (parentNodeKeyFound == null) {
                    rootNodeDirective = linked;
                    // We're at the root, nothing else to look for.
                    needToFindAncestors = false;
                } else if (parentNodeKeyFound) {
                    nodeKey = parentNodeKeyHolder.getValue();
                    linked = processedNodeInfos.putIfAbsent(nodeKey, LinkedNodeDirective::new).addChild(linked);
                    if (linked.actionDefined()) {
                        // We already knew about this node, so we've already found its ancestors.
                        needToFindAncestors = false;
                    } else {
                        // Newly-created parent node directive, assume it's only here to provide linkage unless it's
                        // found in the raw node directives later. We need to find its ancestors to connect to the root.
                        linked.setAction(Linkage);
                    }
                } else {
                    // This node is orphaned, there are no ancestors to be found.
                    needToFindAncestors = false;
                }
            }
        }
        return rootNodeDirective;
    }

    /**
     * @param nodeKeyTable The table of node key values to create a node key source from
     * @return A {@link ChunkSource} of opaque node key values from {@code nodeKeyTable}
     */
    abstract ChunkSource.WithPrev<? extends Values> makeNodeKeySource(@NotNull Table nodeKeyTable);

    /**
     * @param childNodeKey The child node key to map to its parent node key; should not be the
     *        {@link #isRootNodeKey(Object) root node key}.
     * @param usePrev Whether to use previous row sets and values for determining the parent node key
     * @param parentNodeKeyHolder Holder for the result, which is the parent node key for {@code childNodeKey}; only
     *        valid if this method returns {@code true}.
     * @return {@code true} if the parent node key was found, {@code false} if it was not, {@code null} if this "child"
     *         was the root
     */
    @Nullable
    abstract Boolean nodeKeyToParentNodeKey(
            @Nullable Object childNodeKey,
            boolean usePrev,
            @NotNull MutableObject<Object> parentNodeKeyHolder);

    /**
     * @param nodeKey The node key to test
     * @return Whether {@code nodeKey} maps to the root node for this HierarchicalTableImpl
     */
    abstract boolean isRootNodeKey(@Nullable Object nodeKey);

    /**
     * @return The root node key to use for this type
     */
    abstract Object getRootNodeKey();

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
