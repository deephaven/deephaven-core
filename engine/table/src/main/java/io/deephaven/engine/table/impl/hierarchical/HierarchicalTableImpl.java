/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableConstantIntSource;
import io.deephaven.engine.table.iterators.ByteColumnIterator;
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.hash.*;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl.ChildLevelExpandable.None;
import static io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl.VisitAction.*;
import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.*;
import static io.deephaven.util.BooleanUtils.*;
import static io.deephaven.util.QueryConstants.*;

/**
 * Base result class for operations that produce hierarchical tables, for example {@link Table#rollup rollup} and
 * {@link Table#tree(String, String) tree}.
 */
abstract class HierarchicalTableImpl<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>, IMPL_TYPE extends HierarchicalTableImpl<IFACE_TYPE, IMPL_TYPE>>
        extends BaseGridAttributes<IFACE_TYPE, IMPL_TYPE>
        implements HierarchicalTable<IFACE_TYPE> {

    private static final Logger log = LoggerFactory.getLogger(HierarchicalTableImpl.class);

    static final int EXTRA_COLUMN_COUNT = 2;
    static final ColumnName ROW_DEPTH_COLUMN = ColumnName.of("__DEPTH__");
    static final int ROW_DEPTH_COLUMN_INDEX = 0;
    private static final ColumnDefinition<Integer> ROW_DEPTH_COLUMN_DEFINITION =
            ColumnDefinition.ofInt(ROW_DEPTH_COLUMN.name());
    static final ColumnName ROW_EXPANDED_COLUMN = ColumnName.of("__EXPANDED__");
    static final int ROW_EXPANDED_COLUMN_INDEX = 1;
    private static final ColumnDefinition<Boolean> ROW_EXPANDED_COLUMN_DEFINITION =
            ColumnDefinition.ofBoolean(ROW_EXPANDED_COLUMN.name());
    private static final TableDefinition snapshotDefinition = TableDefinition.of(
            ROW_DEPTH_COLUMN_DEFINITION, ROW_EXPANDED_COLUMN_DEFINITION);

    private static final int CHUNK_SIZE = 512;

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
    public ColumnName getRowExpandedColumn() {
        return ROW_EXPANDED_COLUMN;
    }

    @Override
    public ColumnName getRowDepthColumn() {
        return ROW_DEPTH_COLUMN;
    }

    @Override
    public TableDefinition getSnapshotDefinition() {
        return snapshotDefinition;
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
        private long firstRowPositionToInclude = NULL_LONG;
        private int targetIncludedSize = NULL_INT;
        private WritableChunk<? super Values>[] destinations;
        private ResettableWritableChunk<Any>[] destinationSlices;
        private WritableByteChunk<? super Values> expandedDestination;
        ResettableWritableByteChunk<Any> expandedDestinationSlice;
        // endregion Per-snapshot parameters and state

        // region Per-attempt and intra-attempt parameters and state
        private boolean usePrev;
        private int currentDepth = NULL_INT;
        private boolean expandingAll;
        private long visitedSize = NULL_LONG;
        private int includedSize = NULL_INT;
        // endregion Per-attempt and intra-attempt parameters and state

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
                @NotNull final WritableChunk<? super Values>[] destinations) {
            this.columns = Objects.requireNonNullElseGet(columns, () -> {
                // This is a bit of a hack; we're assuming that destinations map to all possible columns. This would
                // also work correctly for a prefix of the columns, but that doesn't seem like a terrible side effect.
                final BitSet result = new BitSet(destinations.length);
                result.set(0, destinations.length);
                return result;
            });
            firstRowPositionToInclude = rows.firstRowKey();
            targetIncludedSize = rows.intSize("hierarchical table snapshot");
            this.destinations = destinations;
            // noinspection unchecked
            destinationSlices = Arrays.stream(destinations)
                    .peek(dc -> dc.setSize(targetIncludedSize))
                    .map(Chunk::getChunkType)
                    .map(ChunkType::makeResettableWritableChunk)
                    .toArray(ResettableWritableChunk[]::new);
            expandedDestination = destinations[ROW_EXPANDED_COLUMN_INDEX].asWritableByteChunk();
            expandedDestinationSlice = destinationSlices[ROW_EXPANDED_COLUMN_INDEX].asResettableWritableByteChunk();
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
            visitedSize = 0;
            includedSize = 0;
            snapshotClock++;
        }

        boolean usePrev() {
            return usePrev;
        }

        int getCurrentDepth() {
            return currentDepth;
        }

        private boolean filling() {
            return includedSize < targetIncludedSize;
        }

        private long remainingToSkip() {
            return Math.max(0, firstRowPositionToInclude - visitedSize);
        }

        private int remainingToFill() {
            return targetIncludedSize - includedSize;
        }

        private void updateExpansionTypeAndValidateAction(
                @NotNull final VisitAction action,
                final boolean childLevelExpandable) {
            switch (action) {
                case Expand:
                    // There are two reasonable ways to handle "expand" directives when currently expanding-all:
                    // (1) treat them like "linkage" directives, or
                    // (2) have them turn off expand-all for the subtree rooted at their node.
                    // We've opted for option 2, as it creates a mechanism for coarse-grained control of expansions
                    // that is richer than only supporting fine-grained contraction when under expand-all.
                    expandingAll = false;
                    break;
                case ExpandAll:
                    // Ignore moot directives to expand-all when we've reached a leaf
                    expandingAll = childLevelExpandable;
                    break;
                case Linkage:
                    Assert.assertion(expandingAll, "expanding all",
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

        /**
         * Child-directive rules for traversal:
         * <ul>
         * <li>We should always ignore directives for children that don't occur in this node (the parent); they will
         * have rowKeyInParentUnsorted == NULL_ROW_KEY. We should never see such a directive here.</li>
         * <li>We never expect to encounter any directives with action Undefined, that would imply a bug in
         * {@link #linkKeyTableNodeDirectives(Collection, boolean)}.</li>
         * <li>If we're expanding all, we need to process all present and defined directives. We want to visit the nodes
         * for all rows that don't have a directive with action Contract. If we're visiting a node with no directive, we
         * visit with action Linkage.</li>
         * <li>If we're not expanding all, we can ignore directives with action Linkage and Contract; we just want to
         * visit the nodes for directives with action Expand or ExpandAll.</li>
         * </ul>
         *
         * @param childDirective The {@link LinkedDirective} to evaluate
         * @return Whether to skip {@code childDirective}
         */
        private boolean shouldSkipChildDirective(@NotNull final LinkedDirective childDirective) {
            final VisitAction action = childDirective.getAction();
            if (action == Undefined) {
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted("encounter undefined child directive during visit");
                return true;
            }
            if (expandingAll) {
                return false;
            }
            return action == Linkage || action == Contract;
        }

        private WritableByteChunk<? super Values> getExpandedDestinationToFill() {
            return expandedDestinationSlice.resetFromChunk(expandedDestination, includedSize, remainingToFill());
        }

        /**
         * Do post-snapshot maintenance upon successful snapshot, and set destinations sizes.
         *
         * @return The total number of expanded rows traversed
         */
        private long finalizeSuccessfulSnapshot() {
            final Iterator<NodeTableState> cachedNodesIter = nodeTableStates.iterator();
            while (cachedNodesIter.hasNext()) {
                final NodeTableState nodeTableState = cachedNodesIter.next();
                if (!nodeTableState.visited(snapshotClock)) {
                    nodeTableState.release();
                    cachedNodesIter.remove();
                }
            }
            Arrays.stream(destinations).forEach(dc -> dc.setSize(includedSize));
            return visitedSize;
        }

        private void releaseSnapshotResources() {
            usePrev = false;
            currentDepth = NULL_INT;
            expandingAll = false;
            visitedSize = NULL_LONG;
            includedSize = NULL_INT;
            columns = null;
            firstRowPositionToInclude = NULL_LONG;
            targetIncludedSize = NULL_INT;
            destinations = null;
            Arrays.stream(destinationSlices).forEach(SafeCloseable::close);
            destinationSlices = null;
            expandedDestination = null;
            expandedDestinationSlice = null;
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
             * If we're still trying to fill our destination chunks, we need to consider this node and possibly its
             * descendants after applying any node sorts. If we're just traversing for our expanded size calculation, we
             * proceed in arbitrary order.
             *
             * @param filling Whether the enclosing SnapshotState is still {@link #filling() filling} the snapshot
             *        chunks. Passed in order to ensure consistency for the duration of a "visit".
             * @return The table to expand, which will be the (prepared) traversal table or data retrieval table
             */
            private Table prepareAndGetTableForExpansion(final boolean filling) {
                if (filling) {
                    ensurePreparedForDataRetrieval();
                    return getDataRetrievalTable();
                } else {
                    ensurePreparedForTraversal();
                    return getTraversalTable();
                }
            }

            /**
             * Filter irrelevant {@code childDirectives}, then stamp the remaining with their
             * {@link LinkedDirective#rowKeyInParentSorted sorted row key}, and sort them into expansion order by the
             * appropriate row key in parent. <em>THIS MUTATES {@code childDirectives}!</em>
             *
             * @param childDirectives The {@link LinkedDirective child directives} to filter, key, and sort
             * @param filling Whether the enclosing SnapshotState is still {@link #filling() filling} the snapshot
             *        chunks. Passed in order to ensure consistency for the duration of a "visit".
             */
            private void filterKeyAndSortChildDirectives(
                    @NotNull final List<LinkedDirective> childDirectives,
                    final boolean filling) {
                final LongUnaryOperator dataSortReverseLookup = filling
                        ? getDataSortReverseLookup()
                        : LongUnaryOperator.identity();
                int numChildDirectives = childDirectives.size();
                for (int cdi = 0; cdi < numChildDirectives;) {
                    final LinkedDirective childDirective = childDirectives.get(cdi);
                    if (shouldSkipChildDirective(childDirective)) {
                        childDirectives.set(cdi, childDirectives.get(numChildDirectives - 1));
                        childDirectives.remove(--numChildDirectives);
                    } else {
                        ++cdi;
                    }
                }
                for (final LinkedDirective childDirective : childDirectives) {
                    childDirective.setRowKeyInParentSorted(dataSortReverseLookup);
                }
                childDirectives.sort(LinkedDirective.SORTED_ROW_KEY_COMPARATOR);
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
             * @apiNote The {@link SnapshotState#currentDepth current depth} must be up-to-date and
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

    enum VisitAction {
        /**
         * Expand a node. Only expand descendants if they have their own directives with action {@code Expand} or
         * {@code ExpandAll}.
         */
        Expand,
        /**
         * Expand a node. Expand its descendants unless they have a directive with action {@code Contract}, or their
         * direct parent has a directive with action {@code Expand} .
         */
        ExpandAll,
        /**
         * Take no action. Allows descendant {@code Expand} or {@code Contract} directives to be linked to an ancestor
         * with action {@code ExpandAll}.
         */
        Linkage,
        /**
         * Don't expand a node or its descendants. Only meaningful for descendants of a node whose directive has action
         * {@code ExpandAll}.
         */
        Contract,
        /**
         * Placeholder action for a newly-created, as-yet-undefined directive. Used where {@code null} might suffice.
         */
        Undefined;

        static VisitAction lookup(final byte wireValue) {
            switch (wireValue) {
                case KEY_TABLE_ACTION_EXPAND:
                    return Expand;
                case KEY_TABLE_ACTION_EXPAND_ALL:
                    return ExpandAll;
                case KEY_TABLE_ACTION_CONTRACT:
                    return Contract;
                default:
                    throw new IllegalArgumentException("Unrecognized key table action " + wireValue);
            }
        }
    }

    private static final LogOutput.ObjFormatter<Object> NODE_KEY_FORMATTER =
            (@NotNull final LogOutput logOutput, @Nullable final Object nodeKey) -> {
                if (nodeKey instanceof Object[]) {
                    final Object[] asArray = (Object[]) nodeKey;
                    final int asArrayLength = asArray.length;
                    if (asArrayLength == 0) {
                        logOutput.append("[]");
                        return;
                    }
                    logOutput.append('[').append(Objects.toString(asArray[0]));
                    for (int ii = 1; ii < asArrayLength; ++ii) {
                        logOutput.append(',').append(Objects.toString(asArray[ii]));
                    }
                    logOutput.append(']');
                    return;
                }
                logOutput.append(Objects.toString(nodeKey));
            };

    /**
     * Key definition for {@link KeyTableDirective} or {@link LinkedDirective} keyed by node key.
     */
    private static final class NodeKeyHashAdapter<DIRECTIVE_TYPE> implements KeyedObjectKey<Object, DIRECTIVE_TYPE> {

        @NotNull
        private final Function<DIRECTIVE_TYPE, Object> nodeKeyLookup;

        private NodeKeyHashAdapter(@NotNull final Function<DIRECTIVE_TYPE, Object> nodeKeyLookup) {
            this.nodeKeyLookup = nodeKeyLookup;
        }

        @Override
        public Object getKey(@NotNull final DIRECTIVE_TYPE directive) {
            return nodeKeyLookup.apply(directive);
        }

        @Override
        public int hashKey(@Nullable final Object nodeKey) {
            if (nodeKey instanceof Object[]) {
                return Arrays.hashCode((Object[]) nodeKey);
            }
            return Objects.hashCode(nodeKey);
        }

        @Override
        public boolean equalKey(@Nullable final Object nodeKey, @NotNull final DIRECTIVE_TYPE directive) {
            final Object directiveNodeKey = nodeKeyLookup.apply(directive);
            if (nodeKey instanceof Object[] && directiveNodeKey instanceof Object[]) {
                return Arrays.equals((Object[]) nodeKey, (Object[]) directiveNodeKey);
            }
            return Objects.equals(nodeKey, directiveNodeKey);
        }
    }

    /**
     * Information about a node that's referenced directly from the key table for a snapshot.
     */
    private static class KeyTableDirective {

        private static final NodeKeyHashAdapter<KeyTableDirective> HASH_ADAPTER =
                new NodeKeyHashAdapter<>(KeyTableDirective::getNodeKey);

        /**
         * The node key for the target of this directive.
         */
        private final Object nodeKey;

        /**
         * The action to take for this directive.
         */
        private final VisitAction action;

        private KeyTableDirective(@Nullable final Object nodeKey, @NotNull final VisitAction action) {
            this.nodeKey = nodeKey;
            this.action = action;
        }

        Object getNodeKey() {
            return nodeKey;
        }

        VisitAction getAction() {
            return action;
        }
    }

    /**
     * Information about a node that's referenced directly or indirectly from the key table for a snapshot, with
     * appropriate linkage to its children.
     */
    private static final class LinkedDirective {

        private static final NodeKeyHashAdapter<LinkedDirective> HASH_ADAPTER =
                new NodeKeyHashAdapter<>(LinkedDirective::getNodeKey);
        private static final Comparator<? super LinkedDirective> SORTED_ROW_KEY_COMPARATOR =
                Comparator.comparingLong(LinkedDirective::getRowKeyInParentSorted);

        /**
         * The node key for the target of this directive.
         */
        private final Object nodeKey;

        /**
         * The node id for the target of this directive.
         */
        private final long nodeId;

        /**
         * The row key where the target can be found in its parent (before any node-level sorting is applied).
         */
        private final long rowKeyInParentUnsorted;

        /**
         * The action to take for this directive. Will be {@link VisitAction#Undefined undefined} on construction; this
         * is used to determine whether a directive was newly-created by a "compute if absent". Set to
         * {@link VisitAction#Linkage} if this directive is created in order to provide connectivity to another, and
         * overwritten with the key table's action if found.
         */
        private VisitAction action;

        /**
         * This is not a complete list of children, just those that occur in the key table for the current snapshot, or
         * are found in order to provide ancestors for such directives.
         */
        private List<LinkedDirective> children;

        /**
         * The row key where the target can be found in its parent (after any node-level sorting relevant to visiting
         * the parent during the current attempt is applied). This may not actually be "sorted," but it creates the
         * order of traversal.
         */
        private long rowKeyInParentSorted = NULL_ROW_KEY;

        private LinkedDirective(@Nullable final Object nodeKey, final long nodeId, final long rowKeyInParentUnsorted) {
            this.nodeId = nodeId;
            this.nodeKey = nodeKey;
            this.rowKeyInParentUnsorted = rowKeyInParentUnsorted;
        }

        private Object getNodeKey() {
            return nodeKey;
        }

        private long getNodeId() {
            return nodeId;
        }

        private long getRowKeyInParentUnsorted() {
            return rowKeyInParentUnsorted;
        }

        private void setAction(@NotNull final VisitAction action) {
            Assert.neq(action, "action", Undefined, "Undefined");
            if (actionDefined()) {
                Assert.neq(action, "action", Linkage, "Linkage");
            }
            // Allow repeats, accept last
            this.action = action;
        }

        private boolean actionDefined() {
            return action != Undefined;
        }

        private VisitAction getAction() {
            return action;
        }

        private LinkedDirective addChild(@NotNull final LinkedDirective childDirective) {
            (children == null ? children = new ArrayList<>() : children).add(childDirective);
            return this;
        }

        private List<LinkedDirective> getChildren() {
            return children;
        }

        private void setRowKeyInParentSorted(@NotNull final LongUnaryOperator dataSortReverseLookup) {
            Assert.neq(rowKeyInParentUnsorted, "rowKeyInParentUnsorted", NULL_ROW_KEY, "null");
            rowKeyInParentSorted = dataSortReverseLookup.applyAsLong(rowKeyInParentUnsorted);
            if (rowKeyInParentSorted == NULL_ROW_KEY) {
                failIfConcurrentAttemptInconsistent();
                log.error().append("Could not find sorted row key in parent for node key=")
                        .append(NODE_KEY_FORMATTER, nodeKey)
                        .append(", unsorted=").append(rowKeyInParentUnsorted).endl();
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted("Failed to perform sort reverse lookup");
            }
        }

        private long getRowKeyInParentSorted() {
            return rowKeyInParentSorted;
        }
    }

    private class LinkedDirectiveFactory implements KeyedObjectHash.ValueFactory<Object, LinkedDirective> {

        private final boolean usePrev;

        private LinkedDirectiveFactory(final boolean usePrev) {
            this.usePrev = usePrev;
        }

        @Override
        public LinkedDirective newValue(@Nullable final Object nodeKey) {
            final long nodeId = nodeKeyToNodeId(nodeKey);
            final long rowKeyInParentUnsorted = nodeId == rootNodeId() || nodeId == nullNodeId()
                    ? NULL_ROW_KEY
                    : findRowKeyInParentUnsorted(nodeId, nodeKey, usePrev);
            // NB: Even if nodeId and nodeKey are valid and in the HierarchicalTable, we don't know for sure their
            // corresponding node is non-empty. That means the "row key in parent unsorted" might be null, and that's
            // OK; we'll ignore it when we visit the parent node.
            return new LinkedDirective(nodeKey, nodeId, rowKeyInParentUnsorted);
        }
    }

    private boolean linkedDirectiveInvalid(@NotNull final LinkedDirective directive) {
        return directive.getNodeId() == nullNodeId()
                || (directive.getRowKeyInParentUnsorted() == NULL_ROW_KEY && directive.getNodeId() != rootNodeId());
    }

    @Override
    public long snapshot(
            @NotNull final HierarchicalTable.SnapshotState snapshotState,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn,
            @Nullable final BitSet columns,
            @NotNull final RowSequence rows,
            @NotNull final WritableChunk<? super Values>[] destinations) {

        if (columns != null) {
            if (columns.cardinality() != destinations.length) {
                throw new IllegalArgumentException("Requested " + columns.cardinality() + ", but only supplied "
                        + destinations.length + " destination chunks");
            }
            if (!columns.get(ROW_EXPANDED_COLUMN_INDEX) || !columns.get(ROW_DEPTH_COLUMN_INDEX)) {
                throw new IllegalArgumentException(
                        "Request must include the \"row expanded\" and \"row depth\" columns");
            }
        }

        if (!rows.isContiguous()) {
            throw new UnsupportedOperationException(
                    "Only contiguous row sequences are supported for hierarchical table snapshots, input=" + rows);
        }

        // noinspection unchecked
        final SnapshotState typedSnapshotState = ((SnapshotState) snapshotState);
        typedSnapshotState.verifyOwner(this);

        final Collection<KeyTableDirective> keyTableDirectives =
                snapshotKeyTableNodeDirectives(keyTable, keyTableActionColumn);

        return snapshotData(typedSnapshotState, keyTableDirectives, columns, rows, destinations);
    }

    private Collection<KeyTableDirective> snapshotKeyTableNodeDirectives(
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn) {
        if (keyTable.isRefreshing()) {
            final MutableObject<Collection<KeyTableDirective>> rootNodeInfoHolder = new MutableObject<>();
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
    private Collection<KeyTableDirective> extractKeyTableNodeDirectives(
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn,
            final boolean usePrev) {
        final ChunkSource<? extends Values> nodeKeySource = maybePrevSource(makeNodeKeySource(keyTable), usePrev);
        final ChunkSource<? extends Values> actionSource = keyTableActionColumn == null ? null
                : maybePrevSource(keyTable.getColumnSource(keyTableActionColumn.name(), byte.class), usePrev);
        final KeyedObjectHashSet<Object, KeyTableDirective> directives =
                new KeyedObjectHashSet<>(KeyTableDirective.HASH_ADAPTER);
        try (final RowSet prevRowSet = usePrev ? keyTable.getRowSet().copyPrev() : null) {
            final RowSequence rowsToExtract = usePrev ? prevRowSet : keyTable.getRowSet();
            final ColumnIterator<?, ?> nodeKeyIter = ColumnIterator.make(nodeKeySource, rowsToExtract);
            final ByteColumnIterator actionIter =
                    actionSource == null ? null : new ByteColumnIterator(actionSource, rowsToExtract);
            nodeKeyIter.forEachRemaining((final Object nodeKey) -> {
                final VisitAction action = actionIter == null ? Expand : lookup(actionIter.nextByte());
                // We don't really expect duplicate node keys, but let the last one win if there are any.
                // Note that KeyedObjectHashSet.add overwrites, instead of following the documentation for Set.add.
                directives.add(new KeyTableDirective(nodeKey, action));
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
     * @param keyTableDirectives The "raw" key table node directives
     * @param usePrev Whether to use the previous state of {@code this}
     * @return The root node directive of the resulting tree structure, or {@code null} if the root was not found
     */
    private LinkedDirective linkKeyTableNodeDirectives(
            @NotNull final Collection<KeyTableDirective> keyTableDirectives,
            final boolean usePrev) {
        LinkedDirective rootNodeDirective = null;
        final KeyedObjectHashSet<Object, LinkedDirective> linkedDirectives =
                new KeyedObjectHashSet<>(LinkedDirective.HASH_ADAPTER);
        final LinkedDirectiveFactory linkedDirectiveFactory = new LinkedDirectiveFactory(usePrev);
        final MutableObject<Object> parentNodeKeyHolder = new MutableObject<>();
        for (final KeyTableDirective fromKeyTable : keyTableDirectives) {
            Object nodeKey = fromKeyTable.getNodeKey();
            LinkedDirective linked = linkedDirectives.putIfAbsent(nodeKey, linkedDirectiveFactory);
            if (linkedDirectiveInvalid(linked)) {
                // This node isn't in our hierarchy. May be left over in the key table from a previous instance
                // with different filtering or different input data (e.g. in the case of a saved key table).
                continue;
            }
            boolean needToFindAncestors = !linked.actionDefined();
            linked.setAction(fromKeyTable.getAction()); // Overwrite Undefined or Linkage with the key table's action
            // Find ancestors iteratively until we hit the root or a node directive that already exists
            while (needToFindAncestors) {
                final Boolean parentNodeKeyFound =
                        findParentNodeKey(nodeKey, linked.getRowKeyInParentUnsorted(), usePrev, parentNodeKeyHolder);
                if (parentNodeKeyFound == null) {
                    rootNodeDirective = linked;
                    // We're at the root, nothing else to look for.
                    needToFindAncestors = false;
                } else if (parentNodeKeyFound) {
                    nodeKey = parentNodeKeyHolder.getValue();
                    linked = linkedDirectives.putIfAbsent(nodeKey, linkedDirectiveFactory).addChild(linked);
                    if (linkedDirectiveInvalid(linked)) {
                        failIfConcurrentAttemptInconsistent();
                        log.error().append("Could not find node for parent key=").append(NODE_KEY_FORMATTER, nodeKey)
                                .append(", usePrev=").append(usePrev).endl();
                        // noinspection ThrowableNotThrown
                        Assert.statementNeverExecuted("Failed to find ancestor for existing key table directive");
                        needToFindAncestors = false; // We won't really get here.
                    } else if (linked.actionDefined()) {
                        // We already knew about this node, so we've already found its ancestors.
                        needToFindAncestors = false;
                    } else {
                        // Newly-created parent node directive, assume it's only here to provide linkage unless it's
                        // found in the fromKeyTable node directives later. We need to find its ancestors to connect to
                        // the root.
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
            @NotNull final Collection<KeyTableDirective> keyTableDirectives,
            @Nullable final BitSet columns,
            @NotNull final RowSequence rows,
            @NotNull final WritableChunk<? super Values>[] destinations) {
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
                                        snapshotState, keyTableDirectives, usePrev);
                                return true;
                            });
                } else {
                    traverseExpansionsAndFillSnapshotChunks(snapshotState, keyTableDirectives, false);
                }
                return snapshotState.finalizeSuccessfulSnapshot();
            }
        }
    }

    private void traverseExpansionsAndFillSnapshotChunks(
            @NotNull final SnapshotState snapshotState,
            @NotNull final Collection<KeyTableDirective> keyTableDirectives,
            final boolean usePrev) {
        snapshotState.beginSnapshotAttempt(usePrev);

        // Link the node directives together into a tree
        final LinkedDirective rootNodeDirective = linkKeyTableNodeDirectives(keyTableDirectives, usePrev);
        final VisitAction rootNodeAction;
        if (rootNodeDirective == null ||
                ((rootNodeAction = rootNodeDirective.getAction()) != Expand && rootNodeAction != ExpandAll)) {
            // We *could* auto-expand the root, instead, but for now let's treat this as empty for consistency
            return;
        }

        // Depth-first traversal of expanded nodes
        visitExpandedNode(snapshotState, rootNodeId(), rootNodeAction, rootNodeDirective.getChildren());
    }

    private void visitExpandedNode(
            @NotNull final SnapshotState snapshotState,
            @NotNull final LinkedDirective directive) {
        visitExpandedNode(snapshotState, directive.getNodeId(), directive.getAction(), directive.getChildren());
    }

    private void visitExpandedNode(
            @NotNull final SnapshotState snapshotState,
            final long nodeId,
            @NotNull final VisitAction action,
            @Nullable final List<LinkedDirective> childDirectives) {

        // Get our node-table state, and the correct table instance to expand.
        final SnapshotState.NodeTableState nodeTableState = snapshotState.getNodeTableState(nodeId);
        final boolean filling = snapshotState.filling();
        final Table forExpansion = nodeTableState.prepareAndGetTableForExpansion(filling);
        if (forExpansion.isEmpty()) {
            // We arrived at an empty node. This is not really an error.
            return;
        }

        // Establish whether we are expanding or expanding-all, and update our depth.
        final boolean oldExpandingAll = snapshotState.expandingAll;
        final ChildLevelExpandable childLevelExpandable = childLevelExpandable(snapshotState);
        snapshotState.updateExpansionTypeAndValidateAction(action, childLevelExpandable != None);
        ++snapshotState.currentDepth;
        try {
            final int numChildDirectives;
            if (childLevelExpandable != None && childDirectives != null) {
                // If we have child directives that we might need to expand for, we need to know where they fit in
                // this node's row set, and we want to be able to examine them in the order they may be expanded.
                nodeTableState.filterKeyAndSortChildDirectives(childDirectives, filling);
                numChildDirectives = childDirectives.size();
            } else {
                numChildDirectives = 0;
            }

            final RowSet prevRows = snapshotState.usePrev ? forExpansion.getRowSet().copyPrev() : null;
            final RowSet rowsToVisit = prevRows != null ? prevRows : forExpansion.getRowSet();
            // @formatter:off
            try (final SafeCloseable ignored = prevRows;
                 final RowSequence.Iterator rowsToVisitIter = rowsToVisit.getRowSequenceIterator();
                 final NodeFillContext filler = filling
                         ? new NodeFillContext(snapshotState, nodeTableState, rowsToVisit.size())
                         : null;
                 final RowSet contractedRowKeys = snapshotState.expandingAll && numChildDirectives > 0
                         ? buildContractedRowKeys(childDirectives)
                         : null;
                 final RowSet contractedRowPositions = contractedRowKeys != null && contractedRowKeys.isNonempty()
                         ? rowsToVisit.invert(contractedRowKeys)
                         : null;
                 final RowSet.RangeIterator contractedRowPositionsIter = contractedRowPositions != null
                         ? contractedRowPositions.rangeIterator()
                         : null) {
                // @formatter:on
                final LongUnaryOperator rowKeyToNodeId = makeChildNodeIdLookup(
                        snapshotState, forExpansion, filling && nodeTableState.getDataSortReverseLookup() != null);
                final long firstRelativePosition = rowsToVisitIter.getRelativePosition();
                int cdi = 0;
                LinkedDirective nextChildDirective = cdi < numChildDirectives ? childDirectives.get(cdi) : null;
                long nextContractedPosition =
                        contractedRowPositionsIter != null && contractedRowPositionsIter.hasNext()
                                ? contractedRowPositionsIter.next()
                                : NULL_ROW_KEY;
                while (rowsToVisitIter.hasMore()) {
                    final boolean expandLastRowToConsume;
                    final long lastRowKeyToConsume;
                    final LinkedDirective childDirectiveToExpand;
                    if (snapshotState.expandingAll) {
                        if (nextContractedPosition == rowsToVisitIter.getRelativePosition() - firstRelativePosition) {
                            assert contractedRowPositionsIter != null;
                            // We're at the start of a contracted range; we need to consume the contracted range
                            // and expand the first row after if there is one.
                            final long lastContractedPositionInRange = contractedRowPositionsIter.currentRangeEnd();
                            final long rangeSize = lastContractedPositionInRange - nextContractedPosition + 1;
                            cdi += rangeSize;
                            nextChildDirective = cdi < numChildDirectives ? childDirectives.get(cdi) : null;
                            if (rowsToVisit.size() > lastContractedPositionInRange + 1) {
                                // Consume the contracted range and one extra row that we will expand.
                                expandLastRowToConsume = true;
                                lastRowKeyToConsume = rowsToVisit.get(lastContractedPositionInRange + 1);
                            } else {
                                // We're out of rows, consume the rest.
                                expandLastRowToConsume = false;
                                lastRowKeyToConsume = clampedRowKeyAfterLast(rowsToVisit.lastRowKey());
                            }
                            nextContractedPosition = contractedRowPositionsIter.hasNext()
                                    ? contractedRowPositionsIter.next()
                                    : NULL_ROW_KEY;
                        } else {
                            // We're just expanding the next row.
                            expandLastRowToConsume = true;
                            lastRowKeyToConsume = rowsToVisitIter.peekNextKey();
                        }
                        if (nextChildDirective != null
                                && nextChildDirective.getRowKeyInParentSorted() == lastRowKeyToConsume) {
                            // We can use a directive when we visit.
                            Assert.assertion(expandLastRowToConsume, "expandingLastRowToConsume");
                            childDirectiveToExpand = nextChildDirective;
                            nextChildDirective = ++cdi < numChildDirectives ? childDirectives.get(cdi) : null;
                        } else {
                            // We're expanding-all; just visit with no child information.
                            childDirectiveToExpand = null;
                        }
                    } else {
                        if (nextChildDirective != null) {
                            // We have an expansion directive. Consume the unexpanded prefix and the row to expand.
                            expandLastRowToConsume = true;
                            childDirectiveToExpand = nextChildDirective;
                            lastRowKeyToConsume = childDirectiveToExpand.getRowKeyInParentSorted();
                            nextChildDirective = ++cdi < numChildDirectives ? childDirectives.get(cdi) : null;
                        } else {
                            // We're out of expansions. Consume the rest.
                            expandLastRowToConsume = false;
                            childDirectiveToExpand = null;
                            lastRowKeyToConsume = clampedRowKeyAfterLast(rowsToVisit.lastRowKey());
                        }
                    }
                    consumeRowsUntilNextExpansion(snapshotState, childLevelExpandable, rowsToVisitIter, filler,
                            rowKeyToNodeId, lastRowKeyToConsume, expandLastRowToConsume);
                    if (!expandLastRowToConsume) {
                        consumeRemainder(snapshotState, rowsToVisitIter);
                        break;
                    }
                    if (childDirectiveToExpand != null) {
                        visitExpandedNode(snapshotState, childDirectiveToExpand);
                    } else {
                        visitExpandedNode(snapshotState,
                                rowKeyToNodeId.applyAsLong(lastRowKeyToConsume), Linkage, null);
                    }
                }
            }
        } finally {
            --snapshotState.currentDepth;
            snapshotState.expandingAll = oldExpandingAll;
        }
    }

    private RowSet buildContractedRowKeys(@NotNull final List<LinkedDirective> childDirectives) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        childDirectives.stream()
                .filter(cd -> cd.getAction() == Contract)
                .mapToLong(LinkedDirective::getRowKeyInParentSorted)
                .forEach(builder::appendKey);
        return builder.build();
    }

    private static long clampedRowKeyAfterLast(final long lastRowKey) {
        return lastRowKey == MAX_LONG ? lastRowKey : lastRowKey + 1;
    }

    private void consumeRowsUntilNextExpansion(
            @NotNull final SnapshotState snapshotState,
            @NotNull final ChildLevelExpandable childLevelExpandable,
            @NotNull final RowSequence.Iterator nodeRowsIter,
            @Nullable final NodeFillContext filler,
            @NotNull final LongUnaryOperator rowKeyToNodeId,
            final long lastRowKeyToConsume,
            final boolean lastRowIsExpanded) {
        if (filler == null || !snapshotState.filling()) {
            // Done filling
            snapshotState.visitedSize += nodeRowsIter.advanceAndGetPositionDistance(
                    clampedRowKeyAfterLast(lastRowKeyToConsume));
            return;
        }
        final RowSequence candidateRows = nodeRowsIter.getNextRowSequenceThrough(lastRowKeyToConsume);
        final long remainingToSkip = snapshotState.remainingToSkip();
        final long candidateSize = candidateRows.size();
        if (candidateSize <= remainingToSkip) {
            // Skipping all the candidate rows
            snapshotState.visitedSize += candidateSize;
            return;
        }
        final int remainingToFill = snapshotState.remainingToFill();
        final RowSequence fillRows = remainingToSkip == 0 && remainingToFill >= candidateSize
                ? candidateRows
                : candidateRows.getRowSequenceByPosition(remainingToSkip, remainingToFill);
        final int fillSize = fillRows.intSize();
        try (final SafeCloseable ignored = candidateRows == fillRows ? null : fillRows) {
            // Fill all the "normal" destinations that use a chunk source
            filler.fillNormalDestinations(fillRows);

            // Fill the "row expanded" destination
            final WritableByteChunk<? super Values> expandedDestination = snapshotState.getExpandedDestinationToFill();
            switch (childLevelExpandable) {
                case All:
                    expandedDestination.fillWithValue(0, fillSize - 1, FALSE_BOOLEAN_AS_BYTE);
                    expandedDestination.set(fillSize - 1,
                            booleanAsByte(lastRowIsExpanded && fillRows.lastRowKey() == lastRowKeyToConsume));
                    break;
                case None:
                    expandedDestination.fillWithValue(0, fillSize, NULL_BOOLEAN_AS_BYTE);
                    break;
                case Undetermined:
                    fillRows.forAllRowKeys((final long rowKey) -> {
                        final long childNodeId = rowKeyToNodeId.applyAsLong(rowKey);
                        if (nodeIdExpandable(snapshotState, childNodeId)) {
                            expandedDestination.add(
                                    booleanAsByte(lastRowIsExpanded && rowKey == lastRowKeyToConsume));
                        } else {
                            expandedDestination.add(NULL_BOOLEAN_AS_BYTE);
                        }
                    });
                    break;
            }
        }
        snapshotState.visitedSize += candidateSize;
        snapshotState.includedSize += fillSize;
    }

    private void consumeRemainder(
            @NotNull final SnapshotState snapshotState,
            final RowSequence.Iterator rowsToVisitIter) {
        // This is pure paranoia about Long.MAX_VALUE as a row key and the API for
        // RowSequence.Iterator#advanceAndGetPositionDistance(long), but as Joseph Heller and Kurt Cobain famously
        // said...
        if (rowsToVisitIter.hasMore()) {
            Assert.eqFalse(snapshotState.filling(), "snapshotState.filling()");
            final RowSequence remainder = rowsToVisitIter.getNextRowSequenceWithLength(Long.MAX_VALUE);
            Assert.eq(remainder.size(), "remainder.size()", 1);
            ++snapshotState.visitedSize;
            Assert.eqFalse(rowsToVisitIter.hasMore(), "rowsToVisitIter.hasMore()");
        }
    }

    final class NodeFillContext implements Context {

        private final SnapshotState snapshotState;
        private final SnapshotState.NodeTableState nodeTableState;
        private final int chunkSize;

        ChunkSource.WithPrev<? extends Values>[] dataSources; // Indexed by column, not destination
        SharedContext sharedContext;
        ChunkSource.FillContext[] fillContexts; // Indexed by destination, not column

        private NodeFillContext(
                @NotNull final SnapshotState snapshotState,
                @NotNull final SnapshotState.NodeTableState nodeTableState,
                final long rowsToVisitSize) {
            this.snapshotState = snapshotState;
            this.nodeTableState = nodeTableState;
            this.chunkSize = rowsToVisitSize >= CHUNK_SIZE ? CHUNK_SIZE : (int) rowsToVisitSize;
        }

        /**
         * Fill all destinations except the one for the "row expanded" column for the supplied {@code rows}.
         *
         * @param rows The row keys to fill from
         */
        private void fillNormalDestinations(@NotNull final RowSequence rows) {
            if (rows.isEmpty()) {
                return;
            }

            prepareToFill();

            final BitSet columns = snapshotState.getColumns();
            final WritableChunk<? super Values>[] destinations = snapshotState.destinations;
            final ResettableWritableChunk<Any>[] destinationSlices = snapshotState.destinationSlices;

            int offset = snapshotState.includedSize;
            int capacity = snapshotState.remainingToFill();

            try (final RowSequence.Iterator rowsIter =
                    rows.size() >= chunkSize ? rows.getRowSequenceIterator() : null) {
                final RowSequence chunkRows =
                        rowsIter == null ? rows : rowsIter.getNextRowSequenceWithLength(chunkSize);
                final int chunkRowsSize = chunkRows.intSize();
                do {
                    for (int di = 0, ci = columns.nextSetBit(0); ci >= 0; ++di, ci = columns.nextSetBit(ci)) {
                        if (ci == ROW_EXPANDED_COLUMN_INDEX) {
                            continue;
                        }
                        final ChunkSource.WithPrev<? extends Values> chunkSource = dataSources[ci];
                        final ChunkSource.FillContext fillContext = fillContexts[di];
                        final WritableChunk<? super Values> destination = destinations[di];
                        final WritableChunk<? super Values> destinationSlice = destinationSlices[di].resetFromChunk(
                                destination, offset, capacity);
                        chunkSource.fillChunk(fillContext, destinationSlice, chunkRows);
                    }
                    sharedContext.reset();
                    offset += chunkRowsSize;
                    capacity -= chunkRowsSize;
                } while (rowsIter != null && rowsIter.hasMore());
            }
        }

        private void prepareToFill() {
            if (dataSources == null) {
                dataSources = nodeTableState.getDataSources();
                sharedContext = snapshotState.getSharedContextForLevel();
                fillContexts = snapshotState.getFillContextArrayForLevel();
                final BitSet columns = snapshotState.getColumns();
                for (int di = 0, ci = columns.nextSetBit(0); ci >= 0; ++di, ci = columns.nextSetBit(ci)) {
                    if (ci == ROW_EXPANDED_COLUMN_INDEX) {
                        continue;
                    }
                    fillContexts[di] = dataSources[ci].makeFillContext(chunkSize, sharedContext);
                }
            }
        }

        @Override
        public void close() {
            if (fillContexts != null) {
                SafeCloseableArray.close(fillContexts);
            }
            if (sharedContext != null) {
                sharedContext.close();
            }
        }
    }

    /**
     * @param nodeKeyTable The table of node key values to create a node key source from
     * @return A {@link ChunkSource} of opaque node key values from {@code nodeKeyTable}
     */
    abstract ChunkSource.WithPrev<? extends Values> makeNodeKeySource(@NotNull Table nodeKeyTable);

    /**
     * @param nodeKey The node key to test
     * @return Whether {@code nodeKey} maps to the root node for this HierarchicalTableImpl
     */
    abstract boolean isRootNodeKey(@Nullable Object nodeKey);

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
     * @return The "root" node id that maps to the root node
     */
    abstract long rootNodeId();

    /**
     * @param childNodeId Node ID of the child
     * @param childNodeKey Node key of the child
     * @param usePrev Whether to use previous row sets and values for determining the row key
     * @return The corresponding row key in the node's parent's address space (before sorting), or
     *         {@link RowSequence#NULL_ROW_KEY null} if not found
     */
    abstract long findRowKeyInParentUnsorted(
            final long childNodeId,
            @Nullable Object childNodeKey,
            final boolean usePrev);

    /**
     * @param childNodeKey Node key of the child
     * @param childRowKeyInParentUnsorted The child node's row key in its parent node, in unsorted row key space
     * @param usePrev Whether to use previous row sets and values for determining the parent node key
     * @param parentNodeKeyHolder Holder for the result, which is the parent node key for {@code childNodeKey}; only
     *        valid if this method returns {@code true}.
     * @return {@code true} if the parent node key was found, {@code false} if it was not, {@code null} if this "child"
     *         was the root
     */
    @Nullable
    abstract Boolean findParentNodeKey(
            @Nullable Object childNodeKey,
            long childRowKeyInParentUnsorted,
            boolean usePrev,
            @NotNull MutableObject<Object> parentNodeKeyHolder);

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

    @NotNull
    static ChunkSource.WithPrev<? extends Values>[] maybeAllocateResultChunkSourceArray(
            @Nullable ChunkSource.WithPrev<? extends Values> @Nullable [] existingChunkSources,
            final int numColumns) {
        final ChunkSource.WithPrev<? extends Values>[] result;
        if (existingChunkSources != null) {
            Assert.eq(existingChunkSources.length, "existingChunkSources.length", numColumns, "numColumns");
            result = existingChunkSources;
        } else {
            // noinspection unchecked
            result = new ChunkSource.WithPrev[numColumns];
        }
        return result;
    }

    /**
     * @param depth The current snapshot traversal depth
     * @return An immutable source that maps all rows to {@code depth}
     */
    @NotNull
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

    enum ChildLevelExpandable {
        /**
         * All Nodes are expandable at the child level.
         */
        All,
        /**
         * No nodes are expandable at the child level.
         */
        None,
        /**
         * Each node at the child level must be checked for expandability.
         */
        Undetermined
    }

    /**
     * @param snapshotState The snapshot state for this attempt
     * @return A {@link ChildLevelExpandable} specifying whether all nodes in the child level are expandable, not
     *         expandable, or must be individually checked.
     */
    abstract ChildLevelExpandable childLevelExpandable(@NotNull SnapshotState snapshotState);

    /**
     * @param snapshotState The snapshot state for this attempt
     * @param nodeTableToExpand The (traversal or data retrieval) table being expanded
     * @param sorted Whether the function needs to consider sort redirection
     * @return A mapping function between row keys in {@code nodeTableToExpand} and child node IDs
     */
    @NotNull
    abstract LongUnaryOperator makeChildNodeIdLookup(
            @NotNull SnapshotState snapshotState,
            @NotNull Table nodeTableToExpand,
            final boolean sorted);

    /**
     * @param snapshotState The snapshot state for this attempt
     * @param nodeId The node ID
     * @return Whether {@code nodeId} maps to an expandable node
     */
    abstract boolean nodeIdExpandable(@NotNull SnapshotState snapshotState, long nodeId);

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
