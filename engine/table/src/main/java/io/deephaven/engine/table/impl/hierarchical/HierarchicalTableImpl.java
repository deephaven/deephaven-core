/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableConstantIntSource;
import io.deephaven.engine.table.iterators.ByteColumnIterator;
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.hash.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl.KeyTableNodeAction.*;
import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.callDataSnapshotFunction;
import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.makeSnapshotControl;
import static io.deephaven.util.QueryConstants.*;

/**
 * Base result class for operations that produce hierarchical tables, for example {@link Table#rollup rollup} and
 * {@link Table#tree(String, String) tree}.
 */
abstract class HierarchicalTableImpl<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>, IMPL_TYPE extends HierarchicalTableImpl<IFACE_TYPE, IMPL_TYPE>>
        extends BaseGridAttributes<IFACE_TYPE, IMPL_TYPE>
        implements HierarchicalTable<IFACE_TYPE> {

    private static final int CHUNK_SIZE = 128;

    @SuppressWarnings("unchecked")
    private static volatile ChunkSource.WithPrev<? extends Values>[] cachedDepthSources =
            ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY;

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
    public HierarchicalTable.SnapshotState makeSnapshotState() {
        return new SnapshotState();
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
     * Re-usable {@link HierarchicalTable.SnapshotState} implementation, used for keeping track of clock information and
     * node caching.
     */
    final class SnapshotState extends LivenessArtifact implements HierarchicalTable.SnapshotState {

        private final KeyedLongObjectHashMap<NodeTableState> nodeTableStates =
                new KeyedLongObjectHashMap<>(new NodeTableStateIdKey());
        private final KeyedLongObjectHash.ValueFactory<NodeTableState> nodeTableStateFactory =
                new NodeTableStateIdFactory();

        private final List<ChunkSource.FillContext[]> perLevelFillContextArrays = new ArrayList<>();
        private final List<SharedContext> perLevelSharedContexts = new ArrayList<>();

        // region Per-snapshot parameters and state
        private BitSet columns;
        private RowSequence rows;
        private int targetIncludedSize = NULL_INT;
        private WritableChunk<? extends Values>[] destinations;
        private ResettableWritableChunk<? extends Values>[] destinationSlices;
        // endregion Per-snapshot parameters and state

        // region Per-attempt parameters and state
        private boolean usePrev;
        private int currentDepth = NULL_INT;
        private boolean expandingAll;
        private long expandedSize = NULL_LONG;
        private long nextRowPositionToInclude = NULL_LONG;
        private int includedSize = NULL_INT;
        // endregion Per-attempt parameters and state

        /**
         * Logical clock, incremented per-attempt, used to decide which node table states can be released upon success.
         */
        private int snapshotClock = 0;

        private SnapshotState() {
            if (HierarchicalTableImpl.this.getSource().isRefreshing()) {
                manage(HierarchicalTableImpl.this);
            }
        }

        NodeTableState getNodeTableState(final long nodeId) {
            return nodeTableStates.putIfAbsent(nodeId, nodeTableStateFactory);
        }

        @NotNull
        private ChunkSource.FillContext[] getFillContextArrayForLevel() {
            ChunkSource.FillContext[] result = perLevelFillContextArrays.get(currentDepth);
            if (result == null) {
                perLevelFillContextArrays.set(currentDepth, result = new ChunkSource.FillContext[destinations.length]);
            }
            return result;
        }

        @NotNull
        private SharedContext getSharedContextForLevel() {
            SharedContext result = perLevelSharedContexts.get(currentDepth);
            if (result == null) {
                perLevelSharedContexts.set(currentDepth, result = SharedContext.makeSharedContext());
            }
            return result;
        }

        /**
         * Initialize structures that will be used across multiple attempts with the same inputs.
         *
         * @param columns The included columns, or {@code null} to include all
         * @param rows The included row positions from the total set of expanded rows
         * @param destinations The destination writable chunks (which should be in reinterpreted-to-primitive types)
         * @return A {@link SafeCloseable} that will clean up pooled state that was allocated for this snapshot
         */
        private SafeCloseable initializeSnapshot(
                @Nullable final BitSet columns,
                @NotNull final RowSequence rows,
                @NotNull final WritableChunk<? extends Values>[] destinations) {
            this.columns = Objects.requireNonNullElseGet(columns, () -> {
                // This is a bit of a hack; we're assuming that destinations map to all possible columns. This would
                // also work correctly for a prefix of the columns, but that doesn't seem like a terrible side effect.
                final BitSet result = new BitSet(destinations.length);
                result.set(0, destinations.length);
                return result;
            });
            this.rows = rows;
            targetIncludedSize = rows.intSize("hierarchical table snapshot");
            this.destinations = destinations;
            // noinspection unchecked
            destinationSlices = Arrays.stream(destinations)
                    .map(Chunk::getChunkType)
                    .map(ChunkType::makeResettableWritableChunk)
                    .toArray(ResettableWritableChunk[]::new);
            return this::releaseSnapshotResources;
        }

        BitSet getColumns() {
            return columns;
        }

        /**
         * Initialize state fields for the next snapshot attempt.
         */
        private void beginSnapshotAttempt(final boolean usePrev) {
            this.usePrev = usePrev;
            currentDepth = -1;
            expandingAll = false;
            expandedSize = 0;
            nextRowPositionToInclude = rows.firstRowKey();
            includedSize = 0;
            sharedContext = SharedContext.makeSharedContext();
            snapshotClock++;
        }

        boolean usePrev() {
            return usePrev;
        }

        int getCurrentDepth() {
            return currentDepth;
        }

        /**
         * Do post-snapshot maintenance upon successful snapshot, and set destinations sizes.
         *
         * @return The total number of expanded rows traversed
         */
        private long finalizeSuccessfulSnapshot() {
            final Iterator<NodeTableState> cachedNodesIterator = nodeTableStates.iterator();
            while (cachedNodesIterator.hasNext()) {
                final NodeTableState nodeTableState = cachedNodesIterator.next();
                if (!nodeTableState.visited(snapshotClock)) {
                    nodeTableState.release();
                    cachedNodesIterator.remove();
                }
            }
            Arrays.stream(destinations).forEach(dc -> dc.setSize(includedSize));
            return expandedSize;
        }

        private void releaseSnapshotResources() {
            usePrev = false;
            currentDepth = NULL_INT;
            expandingAll = false;
            expandedSize = NULL_LONG;
            nextRowPositionToInclude = NULL_LONG;
            includedSize = NULL_INT;
            columns = null;
            rows = null;
            targetIncludedSize = NULL_INT;
            destinations = null;
            Arrays.stream(destinationSlices).forEach(SafeCloseable::close);
            destinationSlices = null;
            perLevelFillContextArrays.clear();
            perLevelSharedContexts.clear();
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
             * formatting is not necessary for this use case, we apply it first in order to allow for memoization; we
             * expect most views to share the same formatting given that it is not configurable from the UI.
             */
            private Table filtered;

            /**
             * The table at this node, after any node-level sorting has been applied.
             */
            private Table sorted;

            /**
             * The sort {@link LongUnaryOperator} that maps the appropriate unsorted row key space to {@link #sorted
             * sorted's} outer row space.
             */
            private LongUnaryOperator sortReverseLookup;

            /**
             * Chunk sources to be used for retrieving data from {@link #sorted sorted}.
             */
            private ChunkSource.WithPrev<? extends Values>[] dataSources;

            /**
             * The last {@link SnapshotState#snapshotClock snapshot clock} value this node was visited on.
             */
            private int visitedSnapshotClock;

            private NodeTableState(final long nodeId) {
                this.id = nodeId;
                this.base = nodeIdToNodeBaseTable(nodeId);
                // NB: No current implementation requires NodeTableState to ensure liveness for its base. If that
                // changes, we'll need to add liveness retention for base here.
            }

            private void ensureFilteredCreated() {
                if (filtered != null) {
                    return;
                }
                filtered = applyNodeFormatsAndFilters(id, base);
                if (filtered != base && filtered.isRefreshing()) {
                    filtered.retainReference();
                }
            }

            private void ensureSortedCreated() {
                ensureFilteredCreated();
                if (sorted != null) {
                    return;
                }
                sorted = applyNodeSorts(id, filtered);
                if (sorted != filtered) {
                    // NB: We can safely assume that sorted != base if sorted != filtered
                    if (sorted.isRefreshing()) {
                        sorted.retainReference();
                    }
                    // NB: We could drop our reference to filtered here, but there's no real reason to do it now rather
                    // than in release
                    sortReverseLookup = SortOperation.getReverseLookup(sorted);
                }
            }

            /**
             * Ensure that we've prepared the node table with any structural modifications (e.g. formatting and
             * filtering), and that the result is safe to access during this snapshot attempt. Initializes the data
             * structures necessary for {@link #getTraversalTable()}.
             */
            void ensurePreparedForTraversal() {
                if (hasNodeFiltersToApply(id)) {
                    ensureFilteredCreated();
                    if (filtered != base) {
                        maybeWaitForSatisfaction(filtered);
                    }
                }
                visitedSnapshotClock = snapshotClock;
            }

            /**
             * Ensure that we've prepared the node table with any structural modifications (e.g. formatting and
             * filtering), or ordering modifications (e.g. sorting), and that the result is safe to access during this
             * snapshot attempt. Initializes the data structures necessary for {@link #getTraversalTable()},
             * {@link #getDataRetrievalTable()}, {@link #getDataSortReverseLookup()}, and {@link #getDataSources()}.
             */
            private void ensurePreparedForDataRetrieval() {
                ensureSortedCreated();
                if (sorted != base) {
                    maybeWaitForSatisfaction(sorted);
                }
                visitedSnapshotClock = snapshotClock;
            }

            /**
             * @return The node Table after any structural modifications (e.g. formatting and filtering) have been
             *         applied
             * @apiNote {@link #ensurePreparedForTraversal()} or {@link #ensurePreparedForDataRetrieval()} must have
             *          been called previously during this snapshot attempt
             */
            Table getTraversalTable() {
                return filtered != null ? filtered : base;
            }

            /**
             * @return The node Table after any structural (e.g. formatting and filtering) or ordering modifications
             *         (e.g. sorting) have been applied
             * @apiNote {@link #ensurePreparedForDataRetrieval()} must have been called previously during this snapshot
             *          attempt
             */
            private Table getDataRetrievalTable() {
                return sorted;
            }

            /**
             * @return Redirections that need to be applied when determining child order in the result of
             *         {@link #getDataRetrievalTable()}
             * @apiNote {@link #ensurePreparedForDataRetrieval()} must have been called previously during this snapshot
             *          attempt
             */
            @Nullable
            private LongUnaryOperator getDataSortReverseLookup() {
                return sortReverseLookup;
            }

            /**
             * @return The chunk sources that should be used when retrieving actual data from the result of
             *         {@link #getDataRetrievalTable()}, at their respective column indices; other column indices may
             *         contain {@code null}, or values cached from previous snapshots
             * @apiNote The {@link SnapshotState#currentDepth current depth} must be up to date and
             *          {@link #ensurePreparedForDataRetrieval()} must have been called previously during this snapshot
             *          attempt. This call should only be made once per node per snapshot attempt.
             */
            private ChunkSource.WithPrev<? extends Values>[] getDataSources() {
                return dataSources = makeOrFillChunkSourceArray(SnapshotState.this, id, sorted, dataSources);
            }

            /**
             * @param snapshotClock The {@link SnapshotState#snapshotClock snapshot clock} value
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
    }

    enum KeyTableNodeAction {
        // @formatter:off
        /** A simple expanded node.*/
        Expand,
        /** A node whose descendants should all be expanded unless contracted or a simple expand is encountered. */
        ExpandAll,
        /** A node created indirectly, in order to parent another node. If this node is reachable post-extraction it
         * exists under an "expand-all" in order to provide a path to a contraction. */
        Linkage,
        /** A node that should be contracted, if reachable as the descendant of an "expand-all". */
        Contract,
        /** A newly-created, undefined node. Used where {@code null} might suffice. */
        Undefined;
        // @formatter:on

        static KeyTableNodeAction lookup(final byte wireValue) {
            switch (wireValue) {
                case ACTION_EXPAND:
                    return Expand;
                case ACTION_EXPAND_ALL:
                    return ExpandAll;
                case ACTION_CONTRACT:
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
         * construction; this is used to determine whether a node was newly-created by a "compute if absent". Will be
         * set to {@link KeyTableNodeAction#Linkage} if created in order to provide connectivity to another node; this
         * will be overwritten with the key table's action if found.
         */
        private KeyTableNodeAction action;

        private NodeDirective(@Nullable final Object nodeKey) {
            this.nodeKey = nodeKey;
            action = Undefined;
        }

        Object getNodeKey() {
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
     * appropriate linkage to its children.
     */
    private static final class LinkedNodeDirective extends NodeDirective {

        /**
         * This is not a complete list of children, just those that occur in the key table for the current snapshot, or
         * are found in order to provide ancestors for such nodes.
         */
        private List<LinkedNodeDirective> children;

        /**
         * Row key in the parent node. This may be unsorted or sorted, depending on how we're handling the parent.
         */
        private long rowKeyInParent;

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

        private void setRowKeyInParent(final long rowKeyInParent) {
            this.rowKeyInParent = rowKeyInParent;
        }

        private long getRowKeyInParent() {
            return rowKeyInParent;
        }
    }

    @Override
    public long snapshot(
            @NotNull final HierarchicalTable.SnapshotState snapshotState,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn,
            @Nullable final BitSet columns,
            @NotNull final RowSequence rows,
            @NotNull final WritableChunk<? extends Values>[] destinations) {

        if (columns != null && columns.cardinality() != destinations.length) {
            throw new IllegalArgumentException("Requested " + columns.cardinality() + ", but only supplied "
                    + destinations.length + " destination chunks");
        }

        if (!rows.isContiguous()) {
            throw new UnsupportedOperationException(
                    "Only contiguous row sequences are supported for hierarchical table snapshots, input=" + rows);
        }

        // noinspection unchecked
        final SnapshotState typedSnapshotState = ((SnapshotState) snapshotState);
        typedSnapshotState.verifyOwner(this);

        final Collection<NodeDirective> rawKeyTableNodeDirectives =
                snapshotKeyTableNodeDirectives(keyTable, keyTableActionColumn);

        return snapshotData(typedSnapshotState, rawKeyTableNodeDirectives, columns, rows, destinations);
    }

    private Collection<NodeDirective> snapshotKeyTableNodeDirectives(
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn) {
        if (keyTable.isRefreshing()) {
            final MutableObject<Collection<NodeDirective>> rootNodeInfoHolder = new MutableObject<>();
            // NB: This snapshot need not be notification-aware. If the key table ticks so be it, as long as we
            // extracted a consistent view of its contents.
            final SnapshotControl keyTableSnapshotControl =
                    makeSnapshotControl(false, true, (NotificationStepSource) keyTable);
            callDataSnapshotFunction(getClass().getSimpleName() + "-keys", keyTableSnapshotControl,
                    (final boolean usePrev, final long beforeClockValue) -> {
                        rootNodeInfoHolder.setValue(extractKeyTableNodeDirectives(
                                keyTable, keyTableActionColumn, usePrev));
                        return true;
                    });
            return rootNodeInfoHolder.getValue();
        } else {
            return extractKeyTableNodeDirectives(keyTable, keyTableActionColumn, false);
        }
    }

    /**
     * Extract the key table's node directives in "raw" form.
     *
     * @param keyTable The key table to extract node directives from
     * @param keyTableActionColumn See
     *        {@link #snapshot(HierarchicalTable.SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])}
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
            final ColumnIterator<?, ?> nodeKeyIterator = ColumnIterator.make(nodeKeySource, rowsToExtract);
            final ByteColumnIterator actionIterator =
                    actionSource == null ? null : new ByteColumnIterator(actionSource, rowsToExtract);
            nodeKeyIterator.forEachRemaining((final Object nodeKey) -> {
                final KeyTableNodeAction action = actionIterator == null ? Expand : lookup(actionIterator.nextByte());
                directives.putIfAbsent(nodeKey, NodeDirective::new).setAction(action);
            });
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

    private long snapshotData(
            @NotNull final SnapshotState snapshotState,
            @NotNull final Collection<NodeDirective> rawKeyTableNodeDirectives,
            @Nullable final BitSet columns,
            @NotNull final RowSequence rows,
            @NotNull final WritableChunk<? extends Values>[] destinations) {
        synchronized (snapshotState) {
            try (final SafeCloseable ignored = snapshotState.initializeSnapshot(columns, rows, destinations)) {
                if (source.isRefreshing()) {
                    // NB: This snapshot control must be notification-aware, because if our sources tick we cannot
                    // guarantee that we won't observe some newly-created components on their instantiation step.
                    final SnapshotControl sourceSnapshotControl =
                            makeSnapshotControl(true, true, getSourceDependencies());
                    callDataSnapshotFunction(getClass().getSimpleName() + "-source", sourceSnapshotControl,
                            (final boolean usePrev, final long beforeClockValue) -> {
                                maybeWaitForStructuralSatisfaction();
                                traverseExpansionsAndFillSnapshotChunks(
                                        snapshotState, rawKeyTableNodeDirectives, usePrev);
                                return true;
                            });
                } else {
                    traverseExpansionsAndFillSnapshotChunks(snapshotState, rawKeyTableNodeDirectives, false);
                }
                return snapshotState.finalizeSuccessfulSnapshot();
            }
        }
    }

    private void traverseExpansionsAndFillSnapshotChunks(
            @NotNull final SnapshotState snapshotState,
            @NotNull final Collection<NodeDirective> rawKeyTableNodeDirectives,
            final boolean usePrev) {
        snapshotState.beginSnapshotAttempt(usePrev);

        // Link the node directives together into a tree
        final LinkedNodeDirective rootNodeDirective = linkKeyTableNodeDirectives(rawKeyTableNodeDirectives, usePrev);
        final KeyTableNodeAction rootNodeAction;
        if (rootNodeDirective == null ||
                ((rootNodeAction = rootNodeDirective.getAction()) != Expand && rootNodeAction != ExpandAll)) {
            // We *could* auto-expand the root, instead, but for now let's treat this as empty for consistency
            return;
        }

        // Depth-first traversal of expanded nodes
        visitExpandedNode(snapshotState, getRootNodeKey(), rootNodeAction, rootNodeDirective.getChildren());
    }

    private void visitExpandedNode(
            @NotNull final SnapshotState snapshotState,
            @Nullable final Object nodeKey,
            @NotNull final KeyTableNodeAction action,
            @Nullable final List<LinkedNodeDirective> childDirectives) {

        // Find our node, and avoid updating any state if we won't do any work.
        final long nodeId = nodeKeyToNodeId(nodeKey);
        if (nodeId == nullNodeId()) {
            // This can happen if a directive specified a non-existent node (e.g. from a saved key table) or
            // expand-all reached a tree leaf.
            return;
        }

        // If we're still trying to fill our destination chunks, we need to consider this node and possibly its
        // descendants after applying any node sorts. If we're just traversing for our expanded size calculation,
        // we proceed in arbitrary order.
        final boolean filling = snapshotState.includedSize < snapshotState.targetIncludedSize;
        final SnapshotState.NodeTableState nodeTableState = snapshotState.getNodeTableState(nodeId);
        final Table forExpansion;
        if (filling) {
            nodeTableState.ensurePreparedForDataRetrieval();
            forExpansion = nodeTableState.getDataRetrievalTable();
        } else {
            nodeTableState.ensurePreparedForTraversal();
            forExpansion = nodeTableState.getTraversalTable();
        }
        if (forExpansion.isEmpty()) {
            // We arrived at an empty node. This is not really an error.
            return;
        }

        // Establish whether we are expanding or expanding-all, and update our depth.
        final boolean oldExpandingAll = snapshotState.expandingAll;
        updateExpansionTypeAndValidateAction(snapshotState, action);
        ++snapshotState.currentDepth;
        try {
            // If we have child directives, we need to know where they fit in this node's row set, and we want to be
            // able to examine them in traversal order.
            if (childDirectives != null) {
                final LongUnaryOperator dataSortReverseLookup =
                        filling ? nodeTableState.getDataSortReverseLookup() : null;
                childDirectives.forEach((final LinkedNodeDirective lnd) -> {
                    long rowKeyInParent = nodeKeyToRowKeyInParentUnsorted(lnd.getNodeKey(), snapshotState.usePrev);
                    if (rowKeyInParent != NULL_ROW_KEY && dataSortReverseLookup != null) {
                        rowKeyInParent = dataSortReverseLookup.applyAsLong(rowKeyInParent);
                    }
                    lnd.setRowKeyInParent(rowKeyInParent);
                });
                childDirectives.sort(Comparator.comparingLong(LinkedNodeDirective::getRowKeyInParent));
            }

            // Prepare our support for filling, but don't do per-node work to get our chunk sources or fill contexts
            // until we have to.
            ChunkSource.WithPrev<? extends Values>[] columnDataSources = null;
            final ChunkSource.FillContext[] fillContexts; // Indexed by destination, not column
            final SharedContext sharedContext;
            if (filling) {
                fillContexts = snapshotState.getFillContextArrayForLevel();
                sharedContext = snapshotState.getSharedContextForLevel();
            } else {
                fillContexts = null;
                sharedContext = null;
            }

            // @formatter:off
            // Traversal rules:
            //  - We should always ignore directives for children that don't occur in this node (the parent); they
            //    will have rowKeyInParent == NULL_ROW_KEY.
            //  - We never expect to encounter any directives with action Undefined, that would imply a bug in
            //    linkKeyTableNodeDirectives(...).
            //  - If we're not expanding all, we can ignore directives with action Linkage and Contract;
            //    we just want to visit the nodes for directives with action Expand or ExpandAll.
            //  - If we're expanding all, we want to visit the nodes for all rows that don't have a directive
            //    with action Contract. If we're visiting a node with no directive, we visit with action Linkage.
            // @formatter:on
            try (final RowSet prevRows = snapshotState.usePrev ? forExpansion.getRowSet().copyPrev() : null;
                    final RowSequence.Iterator rowsIterator =
                            (prevRows != null ? prevRows : forExpansion.getRowSet()).getRowSequenceIterator();
                    final SafeCloseable ignored1 = fillContexts == null ? null : new SafeCloseableArray<>(fillContexts);
                    final SafeCloseable ignored2 = sharedContext) {
                final Iterator<LinkedNodeDirective> childDirectivesIterator =
                        childDirectives == null ? Collections.emptyIterator() : childDirectives.iterator();
                LinkedNodeDirective nextChildDirective =
                        childDirectivesIterator.hasNext() ? childDirectivesIterator.next() : null;
                while (rowsIterator.hasMore()) {
                    if (childDirectiveIndex < numChildDirectives) {

                    }
                    // TODO-RWC: We need to iterate expansion rows and child directives together, looking for places to
                    // expand (or not expand).
                    if (snapshotState.expandingAll) {

                    } else {

                    }
                    // Prepare (or not) for filling data in case we hit the beginning of the rows we're including.
                    if (filling && columnDataSources == null) {
                        columnDataSources = nodeTableState.getDataSources();
                        final BitSet columns = snapshotState.getColumns();
                        for (int dsi = 0, ci = columns.nextSetBit(0); ci >= 0; ++dsi, ci = columns.nextSetBit(ci)) {
                            fillContexts[dsi] = columnDataSources[ci].makeFillContext(CHUNK_SIZE, sharedContext);
                        }
                    }
                }
            }
        } finally {
            --snapshotState.currentDepth;
            snapshotState.expandingAll = oldExpandingAll;
        }
    }

    private static void updateExpansionTypeAndValidateAction(
            @NotNull final HierarchicalTableImpl<?, ?>.SnapshotState snapshotState,
            @NotNull final KeyTableNodeAction action) {
        switch (action) {
            case Expand:
                // There are two reasonable ways to handle "expand" directives when currently expanding-all:
                // (1) treat them like "linkage" directives, or
                // (2) have them turn off expand-all for the subtree rooted at their node.
                // We've opted for option 2, as it creates a mechanism for coarse-grained control of expansions that is
                // richer than only supporting fine-grained contraction when under expand-all.
                snapshotState.expandingAll = false;
                break;
            case ExpandAll:
                snapshotState.expandingAll = true;
                break;
            case Linkage:
                Assert.assertion(snapshotState.expandingAll, "expanding all",
                        "visited a linkage node when not expanding all");
                break;
            case Contract:
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted("visit contraction node");
                break;
            case Undefined:
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted("visit undefined node");
                break;
        }
    }

    // TODO-RWC? private void consumeUnwantedChildren(@Nullable

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
     * @return The internal node identifier for {@code nodeKey}, or the {@link #nullNodeId() null node id} if not found
     */
    abstract long nodeKeyToNodeId(@Nullable Object nodeKey);

    /**
     * @return The "null" node id value that signifies a non-existent node
     */
    abstract long nullNodeId();

    /**
     * @param childNodeKey The child node key to map
     * @param usePrev Whether to use previous row sets and values for determining the row key
     * @return The corresponding row key in the node's parent's address space (before sorting), or
     *         {@link RowSequence#NULL_ROW_KEY null} if not found
     */
    abstract long nodeKeyToRowKeyInParentUnsorted(@Nullable Object childNodeKey, final boolean usePrev);

    /**
     * @param nodeId The internal identifier to map
     * @return The base table at the node identified by {@code nodeId}, or {@code null} if the node is not expandable
     */
    abstract Table nodeIdToNodeBaseTable(long nodeId);

    /**
     * @param nodeId The internal identifier for this node
     * @return Whether there are filters that should be applied at this node before traversal
     */
    abstract boolean hasNodeFiltersToApply(long nodeId);

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

    /**
     * Get or fill an array with (possibly-reinterpreted) chunk sources for use with a node.
     *
     * @param snapshotState The snapshot state for this attempt
     * @param nodeId The internal identifier for this node
     * @param nodeSortedTable The formatted, filtered, and sorted table at this node
     * @param existingChunkSources Existing chunk sources for this node, or {@code null} if a new array is needed
     * @return {@code existingChunkSources} or a new array of chunk sources, with elements filled in for all columns
     *         specified by {@code columns}
     */
    @NotNull
    abstract ChunkSource.WithPrev<? extends Values>[] makeOrFillChunkSourceArray(
            @NotNull SnapshotState snapshotState,
            long nodeId,
            @NotNull Table nodeSortedTable,
            @Nullable ChunkSource.WithPrev<? extends Values>[] existingChunkSources);

    /**
     * @param depth The current snapshot traversal depth
     * @return An immutable source that maps all rows to {@code depth}
     */
    static ChunkSource.WithPrev<? extends Values> getDepthSource(final int depth) {
        ChunkSource.WithPrev<? extends Values>[] localCachedDepthSources;
        if ((localCachedDepthSources = cachedDepthSources).length <= depth) {
            synchronized (TreeTableImpl.class) {
                if ((localCachedDepthSources = cachedDepthSources).length <= depth) {
                    final int oldLength = localCachedDepthSources.length;
                    localCachedDepthSources = Arrays.copyOf(localCachedDepthSources, ((depth + 9) / 10) * 10);
                    final int newLength = localCachedDepthSources.length;
                    for (int di = oldLength; di < newLength; ++di) {
                        localCachedDepthSources[di] = new ImmutableConstantIntSource(di);
                    }
                    cachedDepthSources = localCachedDepthSources;
                }
            }
        }
        return localCachedDepthSources[depth];
    }

    /**
     * @return The dependencies that should be used to ensure consistency for a snapshot of this HierarchicalTable
     */
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
