/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.Strings;
import io.deephaven.api.agg.*;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecColumnReferences;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.util.ColumnFormattingValues;
import io.deephaven.engine.util.systemicmarking.SystemicObject;
import io.deephaven.qst.table.AggregateAllByTable;
import io.deephaven.vector.Vector;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.engine.util.IterableUtils;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.impl.MemoizedOperationKey.SelectUpdateViewOrUpdateView.Flavor;
import io.deephaven.engine.table.impl.by.*;
import io.deephaven.engine.table.impl.locations.GroupingProvider;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.table.impl.snapshot.SnapshotIncrementalListener;
import io.deephaven.engine.table.impl.snapshot.SnapshotInternalListener;
import io.deephaven.engine.table.impl.snapshot.SnapshotUtils;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.engine.table.MatchPair.matchString;
import static io.deephaven.engine.table.impl.partitioned.PartitionedTableCreatorImpl.CONSTITUENT;

/**
 * Primary coalesced table implementation.
 */
public class QueryTable extends BaseTable {

    public interface Operation<T extends DynamicNode & NotificationStepReceiver> {

        default boolean snapshotNeeded() {
            return true;
        }

        /**
         * The resulting table and listener of the operation.
         */
        class Result<T extends DynamicNode & NotificationStepReceiver> {
            public final T resultNode;
            public final TableUpdateListener resultListener; // may be null if parent is non-ticking

            public Result(final @NotNull T resultNode) {
                this(resultNode, null);
            }

            /**
             * Construct the result of an operation. The listener may be null if the parent is non-ticking and the table
             * does not need to respond to ticks from other sources.
             *
             * @param resultNode the result of the operation
             * @param resultListener the listener that should be attached to the parent (or null)
             */
            public Result(final @NotNull T resultNode,
                    final @Nullable TableUpdateListener resultListener) {
                this.resultNode = resultNode;
                this.resultListener = resultListener;
            }
        }

        /**
         * @return the description of this operation
         */
        String getDescription();

        /**
         * @return the log prefix of this operation
         */
        String getLogPrefix();

        default SwapListener newSwapListener(final QueryTable queryTable) {
            return new SwapListener(queryTable);
        }

        /**
         * Initialize this operation.
         *
         * @param usePrev data from the previous cycle should be used (otherwise use this cycle)
         * @param beforeClock the clock value that we captured before the function began; the function can use this
         *        value to bail out early if it notices something has gone wrong.
         * @return the result table / listener if successful, null if it should be retried.
         */
        Result<T> initialize(boolean usePrev, long beforeClock);
    }

    public interface MemoizableOperation<T extends DynamicNode & NotificationStepReceiver> extends Operation<T> {
        /**
         * @return the key that should be used to memoize off of
         */
        MemoizedOperationKey getMemoizedOperationKey();
    }

    private static final long serialVersionUID = 1L;

    static final Logger log = LoggerFactory.getLogger(QueryTable.class);

    // Should we save results of potentially expensive operations (can be disabled for unit tests)
    private static boolean memoizeResults =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.memoizeResults", true);

    /**
     * If set to true, then use a RedirectedColumnSource wrapping an ArrayBackedColumnSource for update() calls.
     * Otherwise, the default of a SparseArraySource is used.
     */
    static boolean USE_REDIRECTED_COLUMNS_FOR_UPDATE =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.redirectUpdate", false);
    /**
     * If set to true, then use a RedirectedColumnSource wrapping an ArrayBackedColumnSource for select() calls.
     * Otherwise, the default of a SparseArraySource is used.
     */
    static boolean USE_REDIRECTED_COLUMNS_FOR_SELECT =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.redirectSelect", false);
    /**
     * For a static select(), we would prefer to flatten the table to avoid using memory unnecessarily (because the data
     * may be spread out across many blocks depending on the input RowSet). However, the select() can become slower
     * because it must look things up in a row redirection.
     *
     * Values less than zero disable overhead checking, and result in never flattening the input.
     *
     * A value of zero results in always flattening the input.
     */
    private static final double MAXIMUM_STATIC_SELECT_MEMORY_OVERHEAD =
            Configuration.getInstance().getDoubleWithDefault("QueryTable.maximumStaticSelectMemoryOverhead", 1.1);

    /**
     * You can chose to enable or disable the column parallel select and update.
     */
    static boolean ENABLE_PARALLEL_SELECT_AND_UPDATE =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.enableParallelSelectAndUpdate", true);

    /**
     * Minimum select "chunk" size, defaults to 4 million.
     */
    public static long MINIMUM_PARALLEL_SELECT_ROWS =
            Configuration.getInstance().getLongWithDefault("QueryTable.minimumParallelSelectRows", 1L << 22);

    /**
     * For unit tests, we do want to force the column parallel select and update at times.
     */
    static boolean FORCE_PARALLEL_SELECT_AND_UPDATE =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.forceParallelSelectAndUpdate", false);

    // Whether we should track the entire RowSet of firstBy and lastBy operations
    @VisibleForTesting
    public static boolean TRACKED_LAST_BY =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.trackLastBy", false);
    @VisibleForTesting
    public static boolean TRACKED_FIRST_BY =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.trackFirstBy", false);

    @VisibleForTesting
    public static boolean USE_OLDER_CHUNKED_BY = false;
    @VisibleForTesting
    public static boolean USE_CHUNKED_CROSS_JOIN =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.chunkedJoin", true);

    private static final AtomicReferenceFieldUpdater<QueryTable, ModifiedColumnSet> MODIFIED_COLUMN_SET_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(QueryTable.class, ModifiedColumnSet.class, "modifiedColumnSet");

    private static final AtomicReferenceFieldUpdater<QueryTable, Map> INDEXED_DATA_COLUMNS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(QueryTable.class, Map.class, "indexedDataColumns");
    private static final Map<String, IndexedDataColumn<?>> EMPTY_INDEXED_DATA_COLUMNS = Collections.emptyMap();

    private static final AtomicReferenceFieldUpdater<QueryTable, Map> CACHED_OPERATIONS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(QueryTable.class, Map.class, "cachedOperations");
    private static final Map<MemoizedOperationKey, MemoizedResult<?>> EMPTY_CACHED_OPERATIONS = Collections.emptyMap();

    private final TrackingRowSet rowSet;
    private final LinkedHashMap<String, ColumnSource<?>> columns;
    @SuppressWarnings("FieldMayBeFinal") // Set via MODIFIED_COLUMN_SET_UPDATER if not initialized
    private volatile ModifiedColumnSet modifiedColumnSet;

    // Cached data columns
    @SuppressWarnings("FieldMayBeFinal") // Set via INDEXED_DATA_COLUMNS_UPDATER
    private volatile Map<String, IndexedDataColumn<?>> indexedDataColumns = EMPTY_INDEXED_DATA_COLUMNS;

    // Flattened table support
    private boolean flat;

    // Cached results
    @SuppressWarnings("FieldMayBeFinal") // Set via CACHED_OPERATIONS_UPDATER
    private volatile Map<MemoizedOperationKey, MemoizedResult<?>> cachedOperations = EMPTY_CACHED_OPERATIONS;

    /**
     * Creates a new abstract table, inferring a definition but creating a new column source map.
     *
     * @param rowSet The RowSet of the new table. Callers may need to {@link WritableRowSet#toTracking() convert}.
     * @param columns The column source map for the table, which will be copied into a new column source map
     */
    public QueryTable(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columns) {
        this(TableDefinition.inferFrom(columns).intern(),
                Require.neqNull(rowSet, "rowSet"), new LinkedHashMap<>(columns), null, null);
    }

    /**
     * Creates a new abstract table, reusing a definition but creating a new column source map.
     *
     * @param definition The definition to use for this table, which will be re-ordered to match the same order as
     *        {@code columns} if it does not match
     * @param rowSet The RowSet of the new table. Callers may need to {@link WritableRowSet#toTracking() convert}.
     * @param columns The column source map for the table, which will be copied into a new column source map
     */
    public QueryTable(
            @NotNull final TableDefinition definition,
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columns) {
        this(definition.checkMutualCompatibility(TableDefinition.inferFrom(columns)).intern(),
                Require.neqNull(rowSet, "rowSet"), new LinkedHashMap<>(columns), null, null);
    }

    /**
     * Creates a new abstract table, reusing a definition and column source map.
     *
     * @param definition The definition to use for this table
     * @param rowSet The RowSet of the new table. Callers may need to {@link WritableRowSet#toTracking() convert}.
     * @param columns The column source map for the table, which is not copied.
     * @param modifiedColumnSet Optional {@link ModifiedColumnSet} that should be re-used if supplied
     * @param attributes Optional value to use for {@link #attributes}
     */
    private QueryTable(
            @NotNull final TableDefinition definition,
            @NotNull final TrackingRowSet rowSet,
            @NotNull final LinkedHashMap<String, ColumnSource<?>> columns,
            @Nullable final ModifiedColumnSet modifiedColumnSet,
            @Nullable final Map<String, Object> attributes) {
        super(definition, "QueryTable", attributes); // TODO: Better descriptions composed from query chain
        this.rowSet = rowSet;
        this.columns = columns;
        this.modifiedColumnSet = modifiedColumnSet;
    }

    /**
     * Create a new query table with the {@link ColumnDefinition ColumnDefinitions} of {@code template}, but in the
     * order of {@code this}. The tables must be mutually compatible, as defined via
     * {@link TableDefinition#checkMutualCompatibility(TableDefinition)}.
     *
     * @param template the new definition template to use
     * @return the new query table
     * @deprecated this is being used a workaround for testing purposes where previously mutations were being used at
     *             the {@link ColumnDefinition} level. Do not use this method without good reason.
     */
    @Deprecated
    public QueryTable withDefinitionUnsafe(TableDefinition template) {
        final TableDefinition inOrder = template.checkMutualCompatibility(definition);
        return (QueryTable) copy(inOrder, StandardOptions.COPY_ALL);
    }

    @Override
    public TrackingRowSet getRowSet() {
        return rowSet;
    }

    @Override
    public long size() {
        return rowSet.size();
    }

    @Override
    public <T> ColumnSource<T> getColumnSource(String sourceName) {
        final ColumnSource<?> columnSource = columns.get(sourceName);
        if (columnSource == null) {
            throw new NoSuchColumnException(columns.keySet(), Collections.singletonList(sourceName));
        }
        // noinspection unchecked
        return (ColumnSource<T>) columnSource;
    }

    @Override
    public Map<String, ColumnSource<?>> getColumnSourceMap() {
        return Collections.unmodifiableMap(columns);
    }

    @Override
    public Collection<? extends ColumnSource<?>> getColumnSources() {
        return Collections.unmodifiableCollection(columns.values());
    }

    @Override
    public DataColumn getColumn(@NotNull final String columnName) {
        return ensureIndexedDataColumns().computeIfAbsent(columnName, cn -> new IndexedDataColumn<>(cn, this));
    }

    private Map<String, IndexedDataColumn<?>> ensureIndexedDataColumns() {
        // noinspection unchecked
        return ensureField(INDEXED_DATA_COLUMNS_UPDATER, EMPTY_INDEXED_DATA_COLUMNS, ConcurrentHashMap::new);
    }

    /**
     * Producers of tables should use the modified column set embedded within the table for their result.
     *
     * You must not mutate the result of this method if you are not generating the updates for this table. Callers
     * should not rely on the dirty state of this modified column set.
     *
     * @return the modified column set for this table
     */
    public ModifiedColumnSet getModifiedColumnSetForUpdates() {
        return ensureField(MODIFIED_COLUMN_SET_UPDATER, null, () -> new ModifiedColumnSet(columns));
    }

    /**
     * Create a {@link ModifiedColumnSet} to use when propagating updates from this table.
     *
     * @param columnNames The columns that should belong to the resulting set
     * @return The resulting ModifiedColumnSet for the given columnNames
     */
    public ModifiedColumnSet newModifiedColumnSet(final String... columnNames) {
        if (columnNames.length == 0) {
            return ModifiedColumnSet.EMPTY;
        }
        final ModifiedColumnSet newSet = new ModifiedColumnSet(getModifiedColumnSetForUpdates());
        newSet.setAll(columnNames);
        return newSet;
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the provided resultTable.
     *
     * @param resultTable the destination table
     * @param columnNames the columns that map one-to-one with the result table
     * @return a transformer that passes dirty details via an identity mapping
     */
    public ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(QueryTable resultTable,
            String... columnNames) {
        final ModifiedColumnSet[] columnSets = new ModifiedColumnSet[columnNames.length];
        for (int i = 0; i < columnNames.length; ++i) {
            columnSets[i] = resultTable.newModifiedColumnSet(columnNames[i]);
        }
        return newModifiedColumnSetTransformer(columnNames, columnSets);
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the provided resultTable.
     *
     * @param resultTable the destination table
     * @param matchPairs the columns that map one-to-one with the result table
     * @return a transformer that passes dirty details via an identity mapping
     */
    public ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(QueryTable resultTable,
            MatchPair... matchPairs) {
        final ModifiedColumnSet[] columnSets = new ModifiedColumnSet[matchPairs.length];
        for (int ii = 0; ii < matchPairs.length; ++ii) {
            columnSets[ii] = resultTable.newModifiedColumnSet(matchPairs[ii].leftColumn());
        }
        return newModifiedColumnSetTransformer(MatchPair.getRightColumns(matchPairs), columnSets);
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the table used to construct columnSets. It is an error if {@code columnNames} and {@code columnSets}
     * are not the same length. The transformer will mark {@code columnSets[i]} as dirty if the column represented by
     * {@code columnNames[i]} is dirty.
     *
     * @param columnNames the source columns
     * @param columnSets the destination columns in the convenient ModifiedColumnSet form
     * @return a transformer that knows the dirty details
     */
    public ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(final String[] columnNames,
            final ModifiedColumnSet[] columnSets) {
        return getModifiedColumnSetForUpdates().newTransformer(columnNames, columnSets);
    }

    /**
     * Create a transformer that uses an identity mapping from one ColumnSourceMap to another. The two CSMs must have
     * equivalent column names and column ordering.
     *
     * @param newColumns the column source map for result table
     * @return a simple Transformer that makes a cheap, but CSM compatible copy
     */
    public ModifiedColumnSet.Transformer newModifiedColumnSetIdentityTransformer(
            final Map<String, ColumnSource<?>> newColumns) {
        return getModifiedColumnSetForUpdates().newIdentityTransformer(newColumns);
    }

    /**
     * Create a transformer that uses an identity mapping from one Table another. The two tables must have equivalent
     * column names and column ordering.
     *
     * @param other the result table
     * @return a simple Transformer that makes a cheap, but CSM compatible copy
     */
    public ModifiedColumnSet.Transformer newModifiedColumnSetIdentityTransformer(final Table other) {
        if (other instanceof QueryTable) {
            return getModifiedColumnSetForUpdates().newIdentityTransformer(((QueryTable) other).columns);
        }
        return getModifiedColumnSetForUpdates().newIdentityTransformer(other.getColumnSourceMap());
    }

    @Override
    public Object[] getRecord(long rowNo, String... columnNames) {
        final long key = rowSet.get(rowNo);
        return (columnNames.length > 0 ? Arrays.stream(columnNames).map(this::getColumnSource)
                : columns.values().stream()).map(cs -> cs.get(key)).toArray(Object[]::new);
    }

    @Override
    public PartitionedTable partitionBy(final boolean dropKeys, final String... keyColumnNames) {
        if (isStream()) {
            throw streamUnsupported("partitionBy");
        }
        final List<ColumnName> columns = ColumnName.from(keyColumnNames);
        return memoizeResult(MemoizedOperationKey.partitionBy(dropKeys, columns), () -> {
            final Table partitioned = aggBy(Partition.of(CONSTITUENT, !dropKeys), columns);
            final Set<String> keyColumnNamesSet =
                    Arrays.stream(keyColumnNames).collect(Collectors.toCollection(LinkedHashSet::new));
            final TableDefinition constituentDefinition;
            if (dropKeys) {
                constituentDefinition = TableDefinition.of(definition.getColumnStream()
                        .filter(cd -> !keyColumnNamesSet.contains(cd.getName())).toArray(ColumnDefinition[]::new));
            } else {
                constituentDefinition = definition;
            }
            return new PartitionedTableImpl(partitioned, keyColumnNamesSet, true, CONSTITUENT.name(),
                    constituentDefinition, isRefreshing(), false);
        });
    }

    @Override
    public PartitionedTable partitionedAggBy(final Collection<? extends Aggregation> aggregations,
            final boolean preserveEmpty, @Nullable final Table initialGroups, final String... keyColumnNames) {
        if (isStream()) {
            throw streamUnsupported("partitionedAggBy");
        }
        final Optional<Partition> includedPartition = aggregations.stream()
                .filter(agg -> agg instanceof Partition)
                .map(agg -> (Partition) agg)
                .findFirst();
        final Partition partition = includedPartition.orElseGet(() -> Partition.of(CONSTITUENT));
        final Collection<? extends Aggregation> aggregationsToUse = includedPartition.isPresent()
                ? aggregations
                : Stream.concat(aggregations.stream(), Stream.of(partition)).collect(Collectors.toList());
        final Table aggregated =
                aggBy(aggregationsToUse, preserveEmpty, initialGroups, ColumnName.from(keyColumnNames));
        final Set<String> keyColumnNamesSet =
                Arrays.stream(keyColumnNames).collect(Collectors.toCollection(LinkedHashSet::new));
        final TableDefinition constituentDefinition;
        if (partition.includeGroupByColumns()) {
            constituentDefinition = definition;
        } else {
            constituentDefinition = TableDefinition.of(definition.getColumnStream()
                    .filter(cd -> !keyColumnNamesSet.contains(cd.getName())).toArray(ColumnDefinition[]::new));
        }
        return new PartitionedTableImpl(aggregated, keyColumnNamesSet, true, partition.column().name(),
                constituentDefinition, isRefreshing(), false);
    }

    @Override
    public Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            ColumnName... groupByColumns) {
        throw new UnsupportedOperationException("rollup is not yet implemented in community");
        // TODO https://github.com/deephaven/deephaven-core/issues/65): Implement rollups based on PartitionedTable
        /*
         * if (isStream() && includeConstituents) { throw streamUnsupported("rollup with included constituents"); }
         * final SelectColumn[] gbsColumns = SelectColumn.from(groupByColumns); final MemoizedOperationKey rollupKey =
         * MemoizedOperationKey.rollup(aggregations, gbsColumns, includeConstituents); return memoizeResult(rollupKey,
         * () -> { final QueryTable baseLevel = aggNoMemo( AggregationProcessor.forRollupBase(aggregations,
         * includeConstituents), gbsColumns);
         *
         * final Deque<SelectColumn> gbsColumnsToReaggregate = new ArrayDeque<>(Arrays.asList(gbsColumns)); final
         * Deque<String> nullColumnNames = new ArrayDeque<>(groupByColumns.length); QueryTable lastLevel = baseLevel;
         * while (!gbsColumnsToReaggregate.isEmpty()) {
         * nullColumnNames.addFirst(gbsColumnsToReaggregate.removeLast().getName()); final TableDefinition
         * lastLevelDefinition = lastLevel.getDefinition(); final Map<String, Class<?>> nullColumns =
         * nullColumnNames.stream().collect(Collectors.toMap( Function.identity(), ncn ->
         * lastLevelDefinition.getColumn(ncn).getDataType(), Assert::neverInvoked, LinkedHashMap::new)); lastLevel =
         * lastLevel.aggNoMemo(AggregationProcessor.forRollupReaggregated(aggregations, nullColumns),
         * gbsColumnsToReaggregate.toArray(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY)); }
         *
         * final String[] internalColumnsToDrop = lastLevel.getDefinition().getColumnStream()
         * .map(ColumnDefinition::getName) .filter(cn -> cn.endsWith(ROLLUP_COLUMN_SUFFIX)).toArray(String[]::new);
         * final QueryTable finalTable = (QueryTable) lastLevel.dropColumns(internalColumnsToDrop); final Object
         * reverseLookup = Require.neqNull(lastLevel.getAttribute(REVERSE_LOOKUP_ATTRIBUTE),
         * "REVERSE_LOOKUP_ATTRIBUTE"); finalTable.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);
         *
         * final Table result = HierarchicalTable.createFrom(finalTable, new RollupInfo(aggregations, gbsColumns,
         * includeConstituents ? RollupInfo.LeafType.Constituent : RollupInfo.LeafType.Normal));
         * result.setAttribute(Table.HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE, QueryTable.this); copyAttributes(result,
         * CopyAttributeOperation.Rollup); maybeUpdateSortableColumns(result);
         *
         * return result; });
         */
    }

    @Override
    public Table treeTable(String idColumn, String parentColumn) {
        throw new UnsupportedOperationException("treeTable is not yet implemented in community");
        // TODO https://github.com/deephaven/deephaven-core/issues/64): Implement treeTable based on PartitionedTable
        /*
         * if (isStream()) { throw streamUnsupported("treeTable"); } return
         * memoizeResult(MemoizedOperationKey.treeTable(idColumn, parentColumn), () -> { // TODO
         * (https://github.com/deephaven/deephaven-core/issues/64): // Need key initialization and preserve-empty
         * support to be able to get the root reliably for // initially-empty parents. final PartitionedTable
         * partitionedTable = partitionBy(false, parentColumn); final QueryTable rootTable = (QueryTable)
         * partitionedTable.table().where(new MatchFilter(parentColumn, (Object) null)); final Table result =
         * HierarchicalTable.createFrom((QueryTable) rootTable.copy(), new TreeTableInfo(idColumn, parentColumn));
         *
         * // If the parent table has an RLL attached to it, we can re-use it. final ReverseLookup reverseLookup; if
         * (hasAttribute(PREPARED_RLL_ATTRIBUTE)) { reverseLookup = (ReverseLookup)
         * getAttribute(PREPARED_RLL_ATTRIBUTE); final String[] listenerCols = reverseLookup.getKeyColumns();
         *
         * if (listenerCols.length != 1 || !listenerCols[0].equals(idColumn)) { final String listenerColError =
         * StringUtils.joinStrings(Arrays.stream(listenerCols).map(col -> "'" + col + "'"), ", "); throw new
         * IllegalStateException( "Table was prepared for Tree table with a different Id column. Expected `" + idColumn
         * + "`, Actual " + listenerColError); } } else { reverseLookup =
         * ReverseLookupListener.makeReverseLookupListenerWithSnapshot(QueryTable.this, idColumn); }
         *
         * result.setAttribute(HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, partitionedTable);
         * result.setAttribute(HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE, QueryTable.this);
         * result.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, reverseLookup); copyAttributes(result,
         * CopyAttributeOperation.Treetable); maybeUpdateSortableColumns(result);
         *
         * return result; });
         */
    }

    @Override
    public Table slice(final long firstPositionInclusive, final long lastPositionExclusive) {
        if (firstPositionInclusive == lastPositionExclusive) {
            return getSubTable(RowSetFactory.empty().toTracking());
        }
        return getResult(SliceLikeOperation.slice(this, firstPositionInclusive, lastPositionExclusive, "slice"));
    }

    @Override
    public Table head(final long size) {
        return slice(0, Require.geqZero(size, "size"));
    }

    @Override
    public Table tail(final long size) {
        return slice(-Require.geqZero(size, "size"), 0);
    }

    @Override
    public Table headPct(final double percent) {
        return getResult(SliceLikeOperation.headPct(this, percent));
    }

    @Override
    public Table tailPct(final double percent) {
        return getResult(SliceLikeOperation.tailPct(this, percent));
    }

    @Override
    public Table exactJoin(Table table, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return QueryPerformanceRecorder.withNugget(
                "exactJoin(" + table + "," + Arrays.toString(columnsToMatch) + "," + Arrays.toString(columnsToMatch)
                        + ")",
                sizeForInstrumentation(),
                () -> naturalJoinInternal(table, columnsToMatch, columnsToAdd, true));
    }

    private static String toString(Collection<? extends Selectable> groupByList) {
        return groupByList.stream().map(Strings::of).collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public Table aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        for (ColumnName name : AggSpecColumnReferences.of(spec)) {
            if (!hasColumns(name.name())) {
                throw new IllegalArgumentException(
                        "aggAllBy spec references column that does not exist: spec=" + spec + ", groupByColumns="
                                + toString(Arrays.asList(groupByColumns)));
            }
        }
        final List<ColumnName> groupByList = Arrays.asList(groupByColumns);
        final List<ColumnName> tableColumns = definition.getTypedColumnNames();
        final Optional<Aggregation> agg = AggregateAllByTable.singleAggregation(spec, groupByList, tableColumns);
        if (agg.isEmpty()) {
            throw new IllegalArgumentException(
                    "aggAllBy has no columns to aggregate: spec=" + spec + ", groupByColumns=" + toString(groupByList));
        }
        final QueryTable tableToUse = (QueryTable) AggAllByUseTable.of(this, spec);
        final List<? extends Aggregation> aggs = List.of(agg.get());
        final MemoizedOperationKey aggKey = MemoizedOperationKey.aggBy(aggs, false, null, groupByList);
        return tableToUse.memoizeResult(aggKey, () -> {
            final QueryTable result =
                    tableToUse.aggNoMemo(AggregationProcessor.forAggregation(aggs), false, null, groupByList);
            spec.walk(new AggAllByCopyAttributes(this, result));
            return result;
        });
    }

    @Override
    public Table aggBy(
            final Collection<? extends Aggregation> aggregations,
            final boolean preserveEmpty,
            final Table initialGroups,
            final Collection<? extends ColumnName> groupByColumns) {
        if (aggregations.isEmpty()) {
            throw new IllegalArgumentException(
                    "aggBy must have at least one aggregation, none specified. groupByColumns="
                            + toString(groupByColumns));
        }
        final List<? extends Aggregation> optimized = AggregationOptimizer.of(aggregations);
        final MemoizedOperationKey aggKey =
                MemoizedOperationKey.aggBy(optimized, preserveEmpty, initialGroups, groupByColumns);
        final Table aggregationTable = memoizeResult(aggKey, () -> aggNoMemo(
                AggregationProcessor.forAggregation(optimized), preserveEmpty, initialGroups, groupByColumns));

        final List<ColumnName> optimizedOrder = AggregationPairs.outputsOf(optimized).collect(Collectors.toList());
        final List<ColumnName> userOrder = AggregationPairs.outputsOf(aggregations).collect(Collectors.toList());
        if (userOrder.equals(optimizedOrder)) {
            return aggregationTable;
        }

        // We need to re-order the result columns to match the user-provided order
        final List<ColumnName> resultOrder =
                Stream.concat(groupByColumns.stream(), userOrder.stream()).collect(Collectors.toList());
        return aggregationTable.view(resultOrder);
    }

    private QueryTable aggNoMemo(
            @NotNull final AggregationContextFactory aggregationContextFactory,
            final boolean preserveEmpty,
            @Nullable final Table initialGroups,
            @NotNull final Collection<? extends ColumnName> groupByColumns) {
        final String description = "aggregation(" + aggregationContextFactory
                + ", " + groupByColumns + ")";
        return QueryPerformanceRecorder.withNugget(description, sizeForInstrumentation(),
                () -> ChunkedOperatorAggregationHelper.aggregation(
                        aggregationContextFactory, this, preserveEmpty, initialGroups, groupByColumns));
    }

    private static UnsupportedOperationException streamUnsupported(@NotNull final String operationName) {
        return new UnsupportedOperationException("Stream tables do not support " + operationName
                + "; use StreamTableTools.streamToAppendOnlyTable to accumulate full history");
    }

    @Override
    public Table headBy(long nRows, String... groupByColumns) {
        return QueryPerformanceRecorder.withNugget("headBy(" + nRows + ", " + Arrays.toString(groupByColumns) + ")",
                sizeForInstrumentation(), () -> headOrTailBy(nRows, true, groupByColumns));
    }

    @Override
    public Table tailBy(long nRows, String... groupByColumns) {
        return QueryPerformanceRecorder.withNugget("tailBy(" + nRows + ", " + Arrays.toString(groupByColumns) + ")",
                sizeForInstrumentation(), () -> headOrTailBy(nRows, false, groupByColumns));
    }

    private Table headOrTailBy(long nRows, boolean head, String... groupByColumns) {
        Require.gtZero(nRows, "nRows");

        Set<String> groupByColsSet = new HashSet<>(Arrays.asList(groupByColumns)); // TODO: WTF?
        List<String> colNames = getDefinition().getColumnNames();

        // Iterate through the columns and build updateView() arguments that will trim the columns to nRows rows
        String[] updates = new String[colNames.size() - groupByColumns.length];
        String[] casting = new String[colNames.size() - groupByColumns.length];
        for (int i = 0, j = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            if (!groupByColsSet.contains(colName)) {
                final Class<?> dataType = getDefinition().getColumn(colName).getDataType();
                casting[j] = colName + " = " + getCastFormula(dataType) + colName;
                if (head)
                    updates[j++] =
                            // Get the first nRows rows:
                            // colName = isNull(colName) ? null
                            // : colName.size() > nRows ? colName.subVector(0, nRows)
                            // : colName
                            colName + '=' + "isNull(" + colName + ") ? null" +
                                    ':' + colName + ".size() > " + nRows + " ? " + colName + ".subVector(0, " + nRows
                                    + ')' +
                                    ':' + colName;
                else
                    updates[j++] =
                            // Get the last nRows rows:
                            // colName = isNull(colName) ? null
                            // : colName.size() > nRows ? colName.subVector(colName.size() - nRows, colName.size())
                            // : colName
                            colName + '=' + "isNull(" + colName + ") ? null" +
                                    ':' + colName + ".size() > " + nRows + " ? " + colName + ".subVector(" + colName
                                    + ".size() - " + nRows + ", " + colName + ".size())" +
                                    ':' + colName;
            }
        }

        return groupBy(groupByColumns).updateView(updates).ungroup().updateView(casting);
    }

    @NotNull
    private String getCastFormula(Class<?> dataType) {
        return "(" + getCastFormulaInternal(dataType) + ")";
    }

    @NotNull
    private String getCastFormulaInternal(Class<?> dataType) {
        if (dataType.isPrimitive()) {
            if (dataType == int.class) {
                return "int";
            } else if (dataType == short.class) {
                return "short";
            } else if (dataType == long.class) {
                return "long";
            } else if (dataType == char.class) {
                return "char";
            } else if (dataType == byte.class) {
                return "byte";
            } else if (dataType == float.class) {
                return "float";
            } else if (dataType == double.class) {
                return "double";
            } else if (dataType == boolean.class) {
                return "Boolean";
            }
            throw Assert.statementNeverExecuted("Unknown primitive: " + dataType);
        } else if (dataType.isArray()) {
            return getCastFormulaInternal(dataType.getComponentType()) + "[]";
        } else {
            return dataType.getName();
        }
    }

    @Override
    public Table formatColumns(String... columnFormats) {
        final SelectColumn[] selectColumns = SelectColumnFactory.getFormatExpressions(columnFormats);

        final Set<String> existingColumns = getDefinition().getColumnNames()
                .stream()
                .filter(column -> !ColumnFormattingValues.isFormattingColumn(column))
                .collect(Collectors.toSet());

        final String[] unknownColumns = Arrays.stream(selectColumns)
                .map(SelectColumnFactory::getFormatBaseColumn)
                .filter(column -> (column != null && !column.equals("*") && !existingColumns.contains(column)))
                .toArray(String[]::new);

        if (unknownColumns.length > 0) {
            throw new RuntimeException(
                    "Unknown columns: " + Arrays.toString(unknownColumns) + ", available columns = " + existingColumns);
        }

        return viewOrUpdateView(Flavor.UpdateView, selectColumns);
    }

    @Override
    public Table moveColumns(int index, boolean moveToEnd, String... columnsToMove) {
        // Get the current columns
        final List<String> currentColumns = getDefinition().getColumnNames();

        // Create a Set from columnsToMove. This way, we can rename and rearrange columns at once.
        final Set<String> leftColsToMove = new HashSet<>();
        final Set<String> rightColsToMove = new HashSet<>();
        int extraCols = 0;

        for (final String columnToMove : columnsToMove) {
            final String left = MatchPairFactory.getExpression(columnToMove).leftColumn;
            final String right = MatchPairFactory.getExpression(columnToMove).rightColumn;

            if (!leftColsToMove.add(left) || !currentColumns.contains(left) || (rightColsToMove.contains(left)
                    && !left.equals(right) && leftColsToMove.stream().anyMatch(col -> col.equals(right)))) {
                extraCols++;
            }
            if (currentColumns.stream().anyMatch(currentColumn -> currentColumn.equals(right)) && !left.equals(right)
                    && rightColsToMove.add(right) && !rightColsToMove.contains(left)) {
                extraCols--;
            }
        }
        index += moveToEnd ? extraCols : 0;

        // vci for write, cci for currentColumns, ctmi for columnsToMove
        final SelectColumn[] viewColumns = new SelectColumn[currentColumns.size() + extraCols];
        for (int vci = 0, cci = 0, ctmi = 0; vci < viewColumns.length;) {
            if (vci >= index && ctmi < columnsToMove.length) {
                viewColumns[vci++] = SelectColumnFactory.getExpression(columnsToMove[ctmi++]);
            } else {
                // Don't add the column if it's one of the columns we're moving or if it has been renamed.
                final String currentColumn = currentColumns.get(cci++);
                if (!leftColsToMove.contains(currentColumn)
                        && Arrays.stream(viewColumns).noneMatch(
                                viewCol -> viewCol != null && viewCol.getMatchPair().leftColumn.equals(currentColumn))
                        && Arrays.stream(columnsToMove)
                                .noneMatch(colToMove -> MatchPairFactory.getExpression(colToMove).rightColumn
                                        .equals(currentColumn))) {

                    viewColumns[vci++] = SelectColumnFactory.getExpression(currentColumn);
                }
            }
        }
        return viewOrUpdateView(Flavor.View, viewColumns);
    }

    @Override
    public Table dateTimeColumnAsNanos(String dateTimeColumnName, String nanosColumnName) {
        return viewOrUpdateView(Flavor.UpdateView,
                new ReinterpretedColumn<>(dateTimeColumnName, DateTime.class, nanosColumnName, long.class));
    }

    public static class FilteredTable extends QueryTable implements WhereFilter.RecomputeListener {
        private final QueryTable source;
        private final WhereFilter[] filters;
        private boolean refilterMatchedRequested = false;
        private boolean refilterUnmatchedRequested = false;
        private MergedListener whereListener;

        public FilteredTable(final TrackingRowSet currentMapping, final QueryTable source,
                final WhereFilter[] filters) {
            super(source.getDefinition(), currentMapping, source.columns, null, null);
            this.source = source;
            this.filters = filters;
            for (final WhereFilter f : filters) {
                if (f instanceof LivenessReferent) {
                    manage((LivenessReferent) f);
                }
            }
        }

        @Override
        public void requestRecompute() {
            refilterMatchedRequested = refilterUnmatchedRequested = true;
            Require.neqNull(whereListener, "whereListener").notifyChanges();
        }

        @Override
        public void requestRecomputeUnmatched() {
            refilterUnmatchedRequested = true;
            Require.neqNull(whereListener, "whereListener").notifyChanges();
        }

        /**
         * Called if something about the filters has changed such that all matched rows of the source table should be
         * re-evaluated.
         */
        @Override
        public void requestRecomputeMatched() {
            refilterMatchedRequested = true;
            Require.neqNull(whereListener, "whereListener").notifyChanges();
        }

        private boolean refilterRequested() {
            return refilterUnmatchedRequested || refilterMatchedRequested;
        }

        @NotNull
        @Override
        public FilteredTable getTable() {
            return this;
        }

        @Override
        public void setIsRefreshing(boolean refreshing) {
            setRefreshing(refreshing);
        }

        /**
         * Refilter relevant rows.
         *
         * @param upstreamAdded RowSet of keys that were added upstream
         * @param upstreamRemoved RowSet of keys that were removed
         * @param upstreamModified RowSet of keys that were modified upstream
         * @param shiftData Sequence of shifts that apply to keyspace
         * @param modifiedColumnSet The set of columns that have any changes to indices in {@code modified}
         */
        private void doRefilter(final RowSet upstreamAdded, final RowSet upstreamRemoved, final RowSet upstreamModified,
                final RowSetShiftData shiftData, final ModifiedColumnSet modifiedColumnSet) {

            // Remove upstream keys first, so that keys at rows that were removed and then added are propagated as such.
            // Note that it is a failure to propagate these as modifies, since modifiedColumnSet may not mark that all
            // columns have changed.
            final WritableRowSet removed = upstreamRemoved == null ? RowSetFactory.empty()
                    : upstreamRemoved.intersect(getRowSet());
            getRowSet().writableCast().remove(removed);

            // Update our rowSet and compute removals due to splatting.
            if (shiftData != null && shiftData.nonempty()) {
                shiftData.apply(getRowSet().writableCast());
            }

            final WritableRowSet newMapping;
            if (refilterMatchedRequested && refilterUnmatchedRequested) {
                newMapping = filterRows(source.getRowSet(), source.getRowSet(), false, filters);
                refilterMatchedRequested = refilterUnmatchedRequested = false;
            } else if (refilterUnmatchedRequested) {
                // things that are added or removed are already reflected in source.build
                try (final WritableRowSet unmatchedRows = source.getRowSet().minus(getRowSet())) {
                    // we must check rows that have been modified instead of just preserving them
                    if (upstreamModified != null) {
                        unmatchedRows.insert(upstreamModified);
                    }
                    newMapping = filterRows(unmatchedRows, unmatchedRows, false, filters);
                }
                // add back what we previously matched, but for modifications and removals
                try (final WritableRowSet previouslyMatched = getRowSet().copy()) {
                    if (upstreamAdded != null) {
                        previouslyMatched.remove(upstreamAdded);
                    }
                    if (upstreamModified != null) {
                        previouslyMatched.remove(upstreamModified);
                    }
                    newMapping.insert(previouslyMatched);
                }

                refilterUnmatchedRequested = false;
            } else if (refilterMatchedRequested) {
                // we need to take removed rows out of our rowSet so we do not read them, and also examine added or
                // modified rows
                try (final WritableRowSet matchedRows = getRowSet().copy()) {
                    if (upstreamAdded != null) {
                        matchedRows.insert(upstreamAdded);
                    }
                    if (upstreamModified != null) {
                        matchedRows.insert(upstreamModified);
                    }
                    newMapping = filterRows(matchedRows, matchedRows, false, filters);
                }
                refilterMatchedRequested = false;
            } else {
                throw new IllegalStateException("Refilter called when a refilter was not requested!");
            }

            // Compute added/removed in post-shift keyspace.
            final WritableRowSet added = newMapping.minus(getRowSet());
            final WritableRowSet postShiftRemovals = getRowSet().minus(newMapping);

            // Update our rowSet in post-shift keyspace.
            getRowSet().writableCast().update(added, postShiftRemovals);

            // Note that removed must be propagated to listeners in pre-shift keyspace.
            if (shiftData != null) {
                shiftData.unapply(postShiftRemovals);
            }
            removed.insert(postShiftRemovals);

            final WritableRowSet modified;
            if (upstreamModified == null || upstreamModified.isEmpty()) {
                modified = RowSetFactory.empty();
            } else {
                modified = upstreamModified.intersect(newMapping);
                modified.remove(added);
            }

            notifyListeners(new TableUpdateImpl(added, removed, modified,
                    shiftData == null ? RowSetShiftData.EMPTY : shiftData, modifiedColumnSet));
        }

        private void setWhereListener(MergedListener whereListener) {
            this.whereListener = whereListener;
        }
    }

    @Override
    public Table where(final Collection<? extends Filter> filters) {
        return whereInternal(WhereFilter.from(filters));
    }

    private QueryTable whereInternal(final WhereFilter... filters) {
        return QueryPerformanceRecorder.withNugget("where(" + Arrays.toString(filters) + ")", sizeForInstrumentation(),
                () -> {
                    for (int fi = 0; fi < filters.length; ++fi) {
                        if (!(filters[fi] instanceof ReindexingFilter)) {
                            continue;
                        }
                        final ReindexingFilter reindexingFilter = (ReindexingFilter) filters[fi];
                        final boolean first = fi == 0;
                        final boolean last = fi == filters.length - 1;
                        if (last && !reindexingFilter.requiresSorting()) {
                            // If this is the last (or only) filter, we can just run it as normal unless it requires
                            // sorting.
                            break;
                        }
                        QueryTable result = this;
                        if (!first) {
                            result = result.whereInternal(Arrays.copyOf(filters, fi));
                        }
                        if (reindexingFilter.requiresSorting()) {
                            result = (QueryTable) result.sort(reindexingFilter.getSortColumns());
                            reindexingFilter.sortingDone();
                        }
                        result = result.whereInternal(reindexingFilter);
                        if (!last) {
                            result = result.whereInternal(Arrays.copyOfRange(filters, fi + 1, filters.length));
                        }
                        return result;
                    }

                    for (WhereFilter filter : filters) {
                        filter.init(getDefinition());
                    }

                    return memoizeResult(MemoizedOperationKey.filter(filters), () -> {
                        final SwapListener swapListener =
                                createSwapListenerIfRefreshing(SwapListener::new);

                        final Mutable<QueryTable> result = new MutableObject<>();
                        initializeWithSnapshot("where", swapListener,
                                (prevRequested, beforeClock) -> {
                                    final boolean usePrev = prevRequested && isRefreshing();
                                    final FilteredTable filteredTable;
                                    try (final RowSet prevIfNeeded = usePrev ? rowSet.copyPrev() : null) {
                                        final RowSet rowSetToUpdate = usePrev ? prevIfNeeded : rowSet;
                                        final TrackingRowSet currentMapping = filterRows(rowSetToUpdate,
                                                rowSetToUpdate, usePrev, filters).toTracking();
                                        filteredTable = new FilteredTable(currentMapping, this, filters);
                                    }

                                    for (final WhereFilter filter : filters) {
                                        filter.setRecomputeListener(filteredTable);
                                    }

                                    final boolean refreshingFilters =
                                            Arrays.stream(filters).anyMatch(WhereFilter::isRefreshing);
                                    copyAttributes(filteredTable, CopyAttributeOperation.Filter);
                                    if (!refreshingFilters && isAddOnly()) {
                                        filteredTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
                                    }

                                    final List<NotificationQueue.Dependency> dependencies = Stream.concat(
                                            Stream.of(filters)
                                                    .filter(f -> f instanceof NotificationQueue.Dependency)
                                                    .map(f -> (NotificationQueue.Dependency) f),
                                            Stream.of(filters).filter(f -> f instanceof DependencyStreamProvider)
                                                    .flatMap(f -> ((DependencyStreamProvider) f)
                                                            .getDependencyStream()))
                                            .collect(Collectors.toList());
                                    if (swapListener != null) {
                                        final ListenerRecorder recorder =
                                                new ListenerRecorder("where(" + Arrays.toString(filters) + ")",
                                                        QueryTable.this, filteredTable);
                                        final WhereListener whereListener =
                                                new WhereListener(recorder, dependencies, filteredTable);
                                        filteredTable.setWhereListener(whereListener);
                                        recorder.setMergedListener(whereListener);
                                        swapListener.setListenerAndResult(recorder, filteredTable);
                                        filteredTable.addParentReference(swapListener);
                                        filteredTable.addParentReference(whereListener);
                                    } else if (refreshingFilters) {
                                        final StaticWhereListener whereListener =
                                                new StaticWhereListener(dependencies, filteredTable);
                                        filteredTable.setWhereListener(whereListener);
                                        filteredTable.addParentReference(whereListener);
                                    }

                                    result.setValue(filteredTable);

                                    return true;
                                });

                        return result.getValue();
                    });
                });
    }

    @SuppressWarnings("WeakerAccess")
    protected WritableRowSet filterRows(RowSet currentMapping, RowSet fullSet, boolean usePrev,
            WhereFilter... filters) {
        WritableRowSet matched = currentMapping.copy();

        for (WhereFilter filter : filters) {
            if (Thread.interrupted()) {
                throw new CancellationException("interrupted while filtering");
            }
            try (final SafeCloseable ignored = matched) { // Ensure we close old matched
                matched = filter.filter(matched, fullSet, this, usePrev);
            }
        }
        return matched;
    }

    @Override
    public Table whereIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return whereInInternal(rightTable, true, MatchPair.fromMatches(columnsToMatch));
    }

    @Override
    public Table whereNotIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return whereInInternal(rightTable, false, MatchPair.fromMatches(columnsToMatch));
    }

    private Table whereInInternal(final Table rightTable, final boolean inclusion,
            final MatchPair... columnsToMatch) {
        return QueryPerformanceRecorder.withNugget(
                "whereIn(rightTable, " + inclusion + ", " + matchString(columnsToMatch) + ")",
                sizeForInstrumentation(), () -> {
                    checkInitiateOperation(rightTable);

                    final Table distinctValues = rightTable.selectDistinct(MatchPair.getRightColumns(columnsToMatch));
                    final DynamicWhereFilter dynamicWhereFilter =
                            new DynamicWhereFilter((QueryTable) distinctValues, inclusion, columnsToMatch);
                    final Table where = whereInternal(dynamicWhereFilter);
                    if (distinctValues.isRefreshing()) {
                        where.addParentReference(distinctValues);
                    }
                    if (dynamicWhereFilter.isRefreshing()) {
                        where.addParentReference(dynamicWhereFilter);
                    }
                    return where;
                });
    }

    @Override
    public Table flatten() {
        if (!isFlat() && !isRefreshing() && rowSet.size() - 1 == rowSet.lastRowKey()) {
            // We're already flat, and we'll never update; so we can just return ourselves, after setting ourselves flat
            setFlat();
        }

        if (isFlat()) {
            if (isRefreshing()) {
                manageWithCurrentScope();
            }
            return this;
        }

        return getResult(new FlattenOperation(this));
    }

    protected void setFlat() {
        flat = true;
    }

    @Override
    public boolean isFlat() {
        if (flat) {
            Assert.assertion(rowSet.lastRowKey() == rowSet.size() - 1, "rowSet.lastRowKey() == rowSet.size() - 1",
                    rowSet,
                    "rowSet");
        }
        return flat;
    }

    @Override
    public void releaseCachedResources() {
        super.releaseCachedResources();
        columns.values().forEach(ColumnSource::releaseCachedResources);
    }

    @Override
    public Table select(Collection<? extends Selectable> columns) {
        return selectInternal(SelectColumn.from(columns));
    }

    private Table selectInternal(SelectColumn... selectColumns) {
        if (!isRefreshing() && !isFlat() && exceedsMaximumStaticSelectOverhead()) {
            // if we are static, we will pass the select through a flatten call, to ensure that our result is as
            // efficient in terms of memory as possible
            return ((QueryTable) flatten()).selectInternal(selectColumns);
        }
        return selectOrUpdate(Flavor.Select, selectColumns);
    }

    private boolean exceedsMaximumStaticSelectOverhead() {
        return SparseConstants.sparseStructureExceedsOverhead(this.getRowSet(), MAXIMUM_STATIC_SELECT_MEMORY_OVERHEAD);
    }

    @Override
    public Table update(final Collection<? extends Selectable> newColumns) {
        return selectOrUpdate(Flavor.Update, SelectColumn.from(newColumns));
    }

    /**
     * This does a certain amount of validation and can be used to get confidence that the formulas are valid. If it is
     * not valid, you will get an exception. Positive test (should pass validation): "X = 12", "Y = X + 1") Negative
     * test (should fail validation): "X = 12", "Y = Z + 1")
     *
     * DO NOT USE -- this API is in flux and may change or disappear in the future.
     */
    public SelectValidationResult validateSelect(final SelectColumn... selectColumns) {
        final SelectColumn[] clones = Arrays.stream(selectColumns).map(SelectColumn::copy).toArray(SelectColumn[]::new);
        SelectAndViewAnalyzer analyzer = SelectAndViewAnalyzer.create(SelectAndViewAnalyzer.Mode.SELECT_STATIC, columns,
                rowSet, getModifiedColumnSetForUpdates(), true, clones);
        return new SelectValidationResult(analyzer, clones);
    }

    private Table selectOrUpdate(Flavor flavor, final SelectColumn... selectColumns) {
        final String humanReadablePrefix = flavor.toString();
        final String updateDescription = humanReadablePrefix + '(' + selectColumnString(selectColumns) + ')';
        return memoizeResult(MemoizedOperationKey.selectUpdateViewOrUpdateView(selectColumns, flavor),
                () -> QueryPerformanceRecorder.withNugget(updateDescription, sizeForInstrumentation(), () -> {
                    checkInitiateOperation();
                    final SelectAndViewAnalyzer.Mode mode;
                    if (isRefreshing()) {
                        if (!isFlat() && (flavor == Flavor.Update && USE_REDIRECTED_COLUMNS_FOR_UPDATE)
                                || (flavor == Flavor.Select && USE_REDIRECTED_COLUMNS_FOR_SELECT)) {
                            mode = SelectAndViewAnalyzer.Mode.SELECT_REDIRECTED_REFRESHING;
                        } else {
                            mode = SelectAndViewAnalyzer.Mode.SELECT_REFRESHING;
                        }
                    } else {
                        mode = SelectAndViewAnalyzer.Mode.SELECT_STATIC;
                    }
                    final boolean publishTheseSources = flavor == Flavor.Update;
                    final SelectAndViewAnalyzer analyzer =
                            SelectAndViewAnalyzer.create(mode, columns, rowSet, getModifiedColumnSetForUpdates(),
                                    publishTheseSources, selectColumns);

                    // Init all the rows by cooking up a fake Update
                    final TableUpdate fakeUpdate = new TableUpdateImpl(
                            analyzer.alreadyFlattenedSources() ? RowSetFactory.flat(rowSet.size()) : rowSet.copy(),
                            RowSetFactory.empty(), RowSetFactory.empty(),
                            RowSetShiftData.EMPTY, ModifiedColumnSet.ALL);

                    final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
                    final SelectAndViewAnalyzer.JobScheduler jobScheduler;
                    if (QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE || (QueryTable.ENABLE_PARALLEL_SELECT_AND_UPDATE
                            && OperationInitializationThreadPool.NUM_THREADS > 1)
                            && analyzer.allowCrossColumnParallelization()) {
                        jobScheduler = new SelectAndViewAnalyzer.OperationInitializationPoolJobScheduler();
                    } else {
                        jobScheduler = SelectAndViewAnalyzer.ImmediateJobScheduler.INSTANCE;
                    }

                    final QueryTable resultTable;
                    final LivenessScope liveResultCapture = isRefreshing() ? new LivenessScope() : null;
                    try (final SafeCloseable ignored = liveResultCapture != null ? liveResultCapture::release : null) {
                        try (final RowSet emptyRowSet = RowSetFactory.empty();
                                final SelectAndViewAnalyzer.UpdateHelper updateHelper =
                                        new SelectAndViewAnalyzer.UpdateHelper(emptyRowSet, fakeUpdate)) {

                            try {
                                analyzer.applyUpdate(fakeUpdate, emptyRowSet, updateHelper, jobScheduler,
                                        liveResultCapture, analyzer.futureCompletionHandler(waitForResult));
                            } catch (Exception e) {
                                waitForResult.completeExceptionally(e);
                            }

                            try {
                                waitForResult.get();
                            } catch (InterruptedException e) {
                                throw new CancellationException("interrupted while computing select or update");
                            } catch (ExecutionException e) {
                                if (e.getCause() instanceof RuntimeException) {
                                    throw (RuntimeException) e.getCause();
                                } else {
                                    throw new UncheckedDeephavenException("Failure computing select or update",
                                            e.getCause());
                                }
                            } finally {
                                final BasePerformanceEntry baseEntry = jobScheduler.getAccumulatedPerformance();
                                if (baseEntry != null) {
                                    final QueryPerformanceNugget outerNugget =
                                            QueryPerformanceRecorder.getInstance().getOuterNugget();
                                    if (outerNugget != null) {
                                        outerNugget.addBaseEntry(baseEntry);
                                    }
                                }
                            }
                        }

                        final TrackingRowSet resultRowSet =
                                analyzer.flattenedResult() ? RowSetFactory.flat(rowSet.size()).toTracking() : rowSet;
                        resultTable = new QueryTable(resultRowSet, analyzer.getPublishedColumnSources());
                        if (liveResultCapture != null) {
                            analyzer.startTrackingPrev();
                            final Map<String, String[]> effects = analyzer.calcEffects();
                            final SelectOrUpdateListener soul = new SelectOrUpdateListener(updateDescription, this,
                                    resultTable, effects, analyzer);
                            liveResultCapture.transferTo(soul);
                            listenForUpdates(soul);
                            ConstituentDependency.install(resultTable, soul);
                        } else {
                            if (resultTable.getRowSet().isFlat()) {
                                resultTable.setFlat();
                            }
                            if (resultTable.getRowSet() == rowSet) {
                                propagateGrouping(selectColumns, resultTable);
                            }
                            for (final ColumnSource<?> columnSource : analyzer.getNewColumnSources().values()) {
                                if (columnSource instanceof PossiblyImmutableColumnSource) {
                                    ((PossiblyImmutableColumnSource) columnSource).setImmutable();
                                }
                            }
                        }
                    }
                    propagateFlatness(resultTable);
                    maybeUpdateSortableColumns(resultTable, selectColumns);
                    if (publishTheseSources) {
                        maybeCopyColumnDescriptions(resultTable, selectColumns);
                    } else {
                        maybeCopyColumnDescriptions(resultTable);
                    }
                    return resultTable;
                }));
    }

    private void propagateGrouping(SelectColumn[] selectColumns, QueryTable resultTable) {
        final Set<String> usedOutputColumns = new HashSet<>();
        for (SelectColumn selectColumn : selectColumns) {
            if (selectColumn instanceof SwitchColumn) {
                selectColumn = ((SwitchColumn) selectColumn).getRealColumn();
            }

            SourceColumn sourceColumn = null;
            if (selectColumn instanceof SourceColumn) {
                sourceColumn = (SourceColumn) selectColumn;
            }
            if (sourceColumn != null && !usedOutputColumns.contains(sourceColumn.getSourceName())) {
                final ColumnSource<?> originalColumnSource = ReinterpretUtils.maybeConvertToPrimitive(
                        getColumnSource(sourceColumn.getSourceName()));
                final ColumnSource<?> selectedColumnSource = ReinterpretUtils.maybeConvertToPrimitive(
                        resultTable.getColumnSource(sourceColumn.getName()));
                if (originalColumnSource != selectedColumnSource) {
                    if (originalColumnSource instanceof DeferredGroupingColumnSource) {
                        final DeferredGroupingColumnSource<?> deferredGroupingSelectedSource =
                                (DeferredGroupingColumnSource<?>) selectedColumnSource;
                        final GroupingProvider<?> groupingProvider =
                                ((DeferredGroupingColumnSource<?>) originalColumnSource).getGroupingProvider();
                        if (groupingProvider != null) {
                            // noinspection unchecked,rawtypes
                            deferredGroupingSelectedSource.setGroupingProvider((GroupingProvider) groupingProvider);
                        } else if (originalColumnSource.getGroupToRange() != null) {
                            // noinspection unchecked,rawtypes
                            deferredGroupingSelectedSource
                                    .setGroupToRange((Map) originalColumnSource.getGroupToRange());
                        }
                    } else if (originalColumnSource.getGroupToRange() != null) {
                        final DeferredGroupingColumnSource<?> deferredGroupingSelectedSource =
                                (DeferredGroupingColumnSource<?>) selectedColumnSource;
                        // noinspection unchecked,rawtypes
                        deferredGroupingSelectedSource.setGroupToRange((Map) originalColumnSource.getGroupToRange());
                    } else {
                        final RowSetIndexer indexer = RowSetIndexer.of(rowSet);
                        if (indexer.hasGrouping(originalColumnSource)) {
                            indexer.copyImmutableGroupings(originalColumnSource, selectedColumnSource);
                        }
                    }
                }
            }
            usedOutputColumns.add(selectColumn.getName());
        }
    }

    @Override
    public Table view(final Collection<? extends Selectable> viewColumns) {
        if (viewColumns == null || viewColumns.isEmpty()) {
            if (isRefreshing()) {
                manageWithCurrentScope();
            }
            return this;
        }
        return viewOrUpdateView(Flavor.View, SelectColumn.from(viewColumns));
    }

    @Override
    public Table updateView(final Collection<? extends Selectable> viewColumns) {
        return viewOrUpdateView(Flavor.UpdateView, SelectColumn.from(viewColumns));
    }

    private Table viewOrUpdateView(Flavor flavor, final SelectColumn... viewColumns) {
        if (viewColumns == null || viewColumns.length == 0) {
            if (isRefreshing()) {
                manageWithCurrentScope();
            }
            return this;
        }

        final String humanReadablePrefix = flavor.toString();

        // Assuming that the description is human-readable, we make it once here and use it twice.
        final String updateDescription = humanReadablePrefix + '(' + selectColumnString(viewColumns) + ')';

        return memoizeResult(MemoizedOperationKey.selectUpdateViewOrUpdateView(viewColumns, flavor),
                () -> QueryPerformanceRecorder.withNugget(
                        updateDescription, sizeForInstrumentation(), () -> {
                            final Mutable<Table> result = new MutableObject<>();

                            final SwapListener swapListener =
                                    createSwapListenerIfRefreshing(SwapListener::new);
                            initializeWithSnapshot(humanReadablePrefix, swapListener, (usePrev, beforeClockValue) -> {
                                final boolean publishTheseSources = flavor == Flavor.UpdateView;
                                final SelectAndViewAnalyzer analyzer =
                                        SelectAndViewAnalyzer.create(SelectAndViewAnalyzer.Mode.VIEW_EAGER,
                                                columns, rowSet, getModifiedColumnSetForUpdates(), publishTheseSources,
                                                viewColumns);
                                final QueryTable queryTable =
                                        new QueryTable(rowSet, analyzer.getPublishedColumnSources());
                                if (swapListener != null) {
                                    final Map<String, String[]> effects = analyzer.calcEffects();
                                    final TableUpdateListener listener =
                                            new ViewOrUpdateViewListener(updateDescription, this, queryTable, effects);
                                    swapListener.setListenerAndResult(listener, queryTable);
                                    queryTable.addParentReference(swapListener);
                                }

                                propagateFlatness(queryTable);

                                copyAttributes(queryTable,
                                        flavor == Flavor.UpdateView ? CopyAttributeOperation.UpdateView
                                                : CopyAttributeOperation.View);
                                maybeUpdateSortableColumns(queryTable, viewColumns);
                                if (publishTheseSources) {
                                    maybeCopyColumnDescriptions(queryTable, viewColumns);
                                } else {
                                    maybeCopyColumnDescriptions(queryTable);
                                }

                                result.setValue(queryTable);

                                return true;
                            });

                            return result.getValue();
                        }));
    }

    /**
     * A Shift-Aware listener for {Update,}View. It uses the LayeredColumnReferences class to calculate how columns
     * affect other columns, then creates a column set transformer which will be used by onUpdate to transform updates.
     */
    private static class ViewOrUpdateViewListener extends ListenerImpl {
        private final QueryTable dependent;
        private final ModifiedColumnSet.Transformer transformer;

        /**
         * @param description Description of this listener
         * @param parent The parent table
         * @param dependent The dependent table
         * @param effects A map from a column name to the column names that it affects
         */
        ViewOrUpdateViewListener(String description, QueryTable parent, QueryTable dependent,
                Map<String, String[]> effects) {
            super(description, parent, dependent);
            this.dependent = dependent;

            // Now calculate the other dependencies and invert
            final String[] parentNames = new String[effects.size()];
            final ModifiedColumnSet[] mcss = new ModifiedColumnSet[effects.size()];
            int nextIndex = 0;
            for (Map.Entry<String, String[]> entry : effects.entrySet()) {
                parentNames[nextIndex] = entry.getKey();
                mcss[nextIndex] = dependent.newModifiedColumnSet(entry.getValue());
                ++nextIndex;
            }
            transformer = parent.newModifiedColumnSetTransformer(parentNames, mcss);
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);
            downstream.modifiedColumnSet = dependent.getModifiedColumnSetForUpdates();
            transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
            dependent.notifyListeners(downstream);
        }
    }

    @Override
    public Table lazyUpdate(final Collection<? extends Selectable> newColumns) {
        final SelectColumn[] selectColumns = SelectColumn.from(newColumns);
        return QueryPerformanceRecorder.withNugget("lazyUpdate(" + selectColumnString(selectColumns) + ")",
                sizeForInstrumentation(), () -> {
                    checkInitiateOperation();

                    final SelectAndViewAnalyzer analyzer =
                            SelectAndViewAnalyzer.create(SelectAndViewAnalyzer.Mode.VIEW_LAZY,
                                    columns, rowSet, getModifiedColumnSetForUpdates(), true, selectColumns);
                    final QueryTable result = new QueryTable(rowSet, analyzer.getPublishedColumnSources());
                    if (isRefreshing()) {
                        listenForUpdates(new ListenerImpl(
                                "lazyUpdate(" + Arrays.deepToString(selectColumns) + ')', this, result));
                    }
                    propagateFlatness(result);
                    copyAttributes(result, CopyAttributeOperation.UpdateView);
                    maybeUpdateSortableColumns(result, selectColumns);
                    maybeCopyColumnDescriptions(result, selectColumns);

                    return result;
                });
    }

    @Override
    public Table dropColumns(String... columnNames) {
        return memoizeResult(MemoizedOperationKey.dropColumns(columnNames), () -> QueryPerformanceRecorder
                .withNugget("dropColumns(" + Arrays.toString(columnNames) + ")", sizeForInstrumentation(), () -> {
                    final Mutable<Table> result = new MutableObject<>();

                    final Set<String> existingColumns = new HashSet<>(definition.getColumnNames());
                    final Set<String> columnNamesToDrop = new HashSet<>(Arrays.asList(columnNames));
                    if (!existingColumns.containsAll(columnNamesToDrop)) {
                        columnNamesToDrop.removeAll(existingColumns);
                        throw new RuntimeException("Unknown columns: " + columnNamesToDrop
                                + ", available columns = " + getColumnSourceMap().keySet());
                    }
                    final Map<String, ColumnSource<?>> newColumns = new LinkedHashMap<>(columns);
                    for (String columnName : columnNames) {
                        newColumns.remove(columnName);
                    }

                    final SwapListener swapListener =
                            createSwapListenerIfRefreshing(SwapListener::new);

                    initializeWithSnapshot("dropColumns", swapListener, (usePrev, beforeClockValue) -> {
                        final QueryTable resultTable = new QueryTable(rowSet, newColumns);
                        propagateFlatness(resultTable);

                        copyAttributes(resultTable, CopyAttributeOperation.DropColumns);
                        maybeUpdateSortableColumns(resultTable);
                        maybeCopyColumnDescriptions(resultTable);

                        if (swapListener != null) {
                            final ModifiedColumnSet.Transformer mcsTransformer =
                                    newModifiedColumnSetTransformer(resultTable,
                                            resultTable.getColumnSourceMap().keySet()
                                                    .toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
                            final ListenerImpl listener = new ListenerImpl(
                                    "dropColumns(" + Arrays.deepToString(columnNames) + ')', this, resultTable) {
                                @Override
                                public void onUpdate(final TableUpdate upstream) {
                                    final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);
                                    final ModifiedColumnSet resultModifiedColumnSet =
                                            resultTable.getModifiedColumnSetForUpdates();
                                    mcsTransformer.clearAndTransform(upstream.modifiedColumnSet(),
                                            resultModifiedColumnSet);
                                    if (upstream.modified().isEmpty() || resultModifiedColumnSet.empty()) {
                                        downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                                        if (downstream.modified().isNonempty()) {
                                            downstream.modified().close();
                                            downstream.modified = RowSetFactory.empty();
                                        }
                                    } else {
                                        downstream.modifiedColumnSet = resultModifiedColumnSet;
                                    }
                                    resultTable.notifyListeners(downstream);
                                }
                            };
                            swapListener.setListenerAndResult(listener, resultTable);
                            resultTable.addParentReference(swapListener);
                        }

                        result.setValue(resultTable);

                        return true;
                    });

                    return result.getValue();
                }));
    }

    @Override
    public Table renameColumns(MatchPair... pairs) {
        return QueryPerformanceRecorder.withNugget("renameColumns(" + matchString(pairs) + ")",
                sizeForInstrumentation(), () -> {
                    if (pairs == null || pairs.length == 0) {
                        if (isRefreshing()) {
                            manageWithCurrentScope();
                        }
                        return this;
                    }

                    checkInitiateOperation();

                    Map<String, String> pairLookup = new HashMap<>();
                    for (MatchPair pair : pairs) {
                        if (pair.leftColumn == null || pair.leftColumn.equals("")) {
                            throw new IllegalArgumentException(
                                    "Bad left column in rename pair \"" + pair + "\"");
                        }
                        if (null == columns.get(pair.rightColumn)) {
                            throw new IllegalArgumentException("Column \"" + pair.rightColumn + "\" not found");
                        }
                        pairLookup.put(pair.rightColumn, pair.leftColumn);
                    }

                    int mcsPairIdx = 0;
                    final MatchPair[] modifiedColumnSetPairs = new MatchPair[columns.size()];

                    Map<String, ColumnSource<?>> newColumns = new LinkedHashMap<>();
                    for (Map.Entry<String, ? extends ColumnSource<?>> entry : columns.entrySet()) {
                        String oldName = entry.getKey();
                        ColumnSource<?> columnSource = entry.getValue();
                        String newName = pairLookup.get(oldName);
                        if (newName == null) {
                            newName = oldName;
                        }
                        modifiedColumnSetPairs[mcsPairIdx++] = new MatchPair(newName, oldName);
                        newColumns.put(newName, columnSource);
                    }

                    final QueryTable queryTable = new QueryTable(rowSet, newColumns);
                    if (isRefreshing()) {
                        final ModifiedColumnSet.Transformer mcsTransformer =
                                newModifiedColumnSetTransformer(queryTable, modifiedColumnSetPairs);
                        listenForUpdates(new ListenerImpl("renameColumns(" + Arrays.deepToString(pairs) + ')',
                                this, queryTable) {
                            @Override
                            public void onUpdate(final TableUpdate upstream) {
                                final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);
                                downstream.modifiedColumnSet = queryTable.getModifiedColumnSetForUpdates();
                                if (upstream.modified().isNonempty()) {
                                    mcsTransformer.clearAndTransform(upstream.modifiedColumnSet(),
                                            downstream.modifiedColumnSet);
                                } else {
                                    downstream.modifiedColumnSet.clear();
                                }
                                queryTable.notifyListeners(downstream);
                            }
                        });
                    }
                    propagateFlatness(queryTable);

                    maybeUpdateSortableColumns(queryTable, pairs);
                    maybeCopyColumnDescriptions(queryTable, pairs);

                    return queryTable;
                });
    }

    @Override
    public Table aj(final Table rightTable, final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
            AsOfMatchRule asOfMatchRule) {
        if (rightTable == null) {
            throw new IllegalArgumentException("aj() requires a non-null right hand side table.");
        }
        final Table rightTableCoalesced = rightTable.coalesce();
        return QueryPerformanceRecorder.withNugget(
                "aj(" + "rightTable, " + matchString(columnsToMatch) + ", " + matchString(columnsToAdd) + ")",
                () -> ajInternal(rightTableCoalesced, columnsToMatch, columnsToAdd, SortingOrder.Ascending,
                        asOfMatchRule));
    }

    @Override
    public Table raj(final Table rightTable, final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
            AsOfMatchRule asOfMatchRule) {
        if (rightTable == null) {
            throw new IllegalArgumentException("raj() requires a non-null right hand side table.");
        }
        final Table rightTableCoalesced = rightTable.coalesce();
        return QueryPerformanceRecorder.withNugget(
                "raj(" + "rightTable, " + matchString(columnsToMatch) + ", " + matchString(columnsToAdd) + ")",
                () -> ajInternal(rightTableCoalesced.reverse(), columnsToMatch, columnsToAdd, SortingOrder.Descending,
                        asOfMatchRule));
    }

    private Table ajInternal(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            final SortingOrder order, AsOfMatchRule asOfMatchRule) {
        if (rightTable == null) {
            throw new IllegalArgumentException("aj() requires a non-null right hand side table.");
        }

        columnsToAdd = createColumnsToAddIfMissing(rightTable, columnsToMatch, columnsToAdd);
        final List<MatchPair> revisedAdded = new ArrayList<>();
        final Set<String> addedColumns = new HashSet<>();
        for (MatchPair matchPair : columnsToMatch) {
            if (!columns.containsKey(matchPair.rightColumn)) {
                addedColumns.add(matchPair.rightColumn);
                revisedAdded.add(new MatchPair(matchPair.rightColumn, matchPair.rightColumn));
            }
        }
        for (MatchPair matchPair : columnsToAdd) {
            if (!addedColumns.contains(matchPair.rightColumn)) {
                revisedAdded.add(matchPair);
            } else if (!matchPair.leftColumn.equals(matchPair.rightColumn)) {
                for (int ii = 0; ii < revisedAdded.size(); ii++) {
                    final MatchPair pair = revisedAdded.get(ii);
                    if (pair.rightColumn.equals(matchPair.rightColumn)) {
                        revisedAdded.set(ii, matchPair);
                    }
                }
            }
        }
        columnsToAdd = revisedAdded.toArray(MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);

        final boolean disallowExactMatch;
        switch (asOfMatchRule) {
            case LESS_THAN:
                if (order != SortingOrder.Ascending) {
                    throw new IllegalArgumentException("Invalid as of match rule for raj: " + asOfMatchRule);
                }
                disallowExactMatch = true;
                break;
            case LESS_THAN_EQUAL:
                if (order != SortingOrder.Ascending) {
                    throw new IllegalArgumentException("Invalid as of match rule for raj: " + asOfMatchRule);
                }
                disallowExactMatch = false;
                break;
            case GREATER_THAN:
                if (order != SortingOrder.Descending) {
                    throw new IllegalArgumentException("Invalid as of match rule for aj: " + asOfMatchRule);
                }
                disallowExactMatch = true;
                break;
            case GREATER_THAN_EQUAL:
                if (order != SortingOrder.Descending) {
                    throw new IllegalArgumentException("Invalid as of match rule for aj: " + asOfMatchRule);
                }
                disallowExactMatch = false;
                break;
            default:
                throw new UnsupportedOperationException();
        }

        return AsOfJoinHelper.asOfJoin(this, (QueryTable) rightTable, columnsToMatch, columnsToAdd, order,
                disallowExactMatch);
    }

    @Override
    public Table naturalJoin(final Table rightTable, final MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return QueryPerformanceRecorder.withNugget(
                "naturalJoin(" + matchString(columnsToMatch) + ", " + matchString(columnsToAdd) + ")",
                () -> naturalJoinInternal(rightTable, columnsToMatch, columnsToAdd, false));
    }

    private Table naturalJoinInternal(final Table rightTable, final MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, boolean exactMatch) {
        columnsToAdd = createColumnsToAddIfMissing(rightTable, columnsToMatch, columnsToAdd);

        final QueryTable rightTableCoalesced = (QueryTable) rightTable.coalesce();

        return NaturalJoinHelper.naturalJoin(this, rightTableCoalesced, columnsToMatch, columnsToAdd, exactMatch);
    }

    private MatchPair[] createColumnsToAddIfMissing(Table rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd) {
        if (columnsToAdd.length == 0) {
            final Set<String> matchColumns = Arrays.stream(columnsToMatch).map(matchPair -> matchPair.leftColumn)
                    .collect(Collectors.toCollection(HashSet::new));
            final List<String> columnNames = rightTable.getDefinition().getColumnNames();
            return columnNames.stream().filter((name) -> !matchColumns.contains(name))
                    .map(name -> new MatchPair(name, name)).toArray(MatchPair[]::new);
        }
        return columnsToAdd;
    }

    private static String selectColumnString(final SelectColumn[] selectColumns) {
        final StringBuilder result = new StringBuilder();
        result.append('[');
        final Iterable<String> scs =
                Arrays.stream(selectColumns).map(SelectColumn::getName).filter(name -> name.length() > 0)::iterator;
        IterableUtils.appendCommaSeparatedList(result, scs);
        result.append("]");
        return result.toString();
    }

    static <T extends ColumnSource<?>> void startTrackingPrev(Collection<T> values) {
        values.forEach(ColumnSource::startTrackingPrevValues);
    }

    @Override
    public Table join(final Table rightTableCandidate, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            int numRightBitsToReserve) {
        return memoizeResult(
                MemoizedOperationKey.crossJoin(rightTableCandidate, columnsToMatch, columnsToAdd,
                        numRightBitsToReserve),
                () -> joinNoMemo(rightTableCandidate, columnsToMatch, columnsToAdd, numRightBitsToReserve));
    }

    private Table joinNoMemo(final Table rightTableCandidate, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            int numRightBitsToReserve) {
        final MatchPair[] realColumnsToAdd;
        if (columnsToAdd.length == 0) {
            Set<String> columnsForMatching =
                    Arrays.stream(columnsToMatch).filter(mp -> mp.rightColumn.equals(mp.leftColumn))
                            .map(x -> x.rightColumn).collect(Collectors.toCollection(HashSet::new));

            Set<String> rightColumnNames;
            try {
                rightColumnNames = rightTableCandidate.getColumnSourceMap().keySet();
            } catch (UnsupportedOperationException uoe) {
                throw new UnsupportedOperationException("Can not join a V2 table to a V1 table on the right side.",
                        uoe);
            }

            realColumnsToAdd = rightColumnNames.stream().filter(x -> !columnsForMatching.contains(x))
                    .map(x -> new MatchPair(x, x)).toArray(MatchPair[]::new);
        } else {
            realColumnsToAdd = columnsToAdd;
        }

        if (USE_CHUNKED_CROSS_JOIN) {
            final QueryTable coalescedRightTable = (QueryTable) rightTableCandidate.coalesce();
            return QueryPerformanceRecorder.withNugget(
                    "join(" + matchString(columnsToMatch) + ", " + matchString(realColumnsToAdd) + ", "
                            + numRightBitsToReserve + ")",
                    () -> CrossJoinHelper.join(this, coalescedRightTable, columnsToMatch, realColumnsToAdd,
                            numRightBitsToReserve));
        }

        final Set<String> columnsToMatchSet =
                Arrays.stream(columnsToMatch).map(MatchPair::rightColumn)
                        .collect(Collectors.toCollection(HashSet::new));

        final Map<String, Selectable> columnsToAddSelectColumns = new LinkedHashMap<>();
        final List<String> columnsToUngroupBy = new ArrayList<>();
        final String[] rightColumnsToMatch = new String[columnsToMatch.length];
        for (int i = 0; i < rightColumnsToMatch.length; i++) {
            rightColumnsToMatch[i] = columnsToMatch[i].rightColumn;
            columnsToAddSelectColumns.put(columnsToMatch[i].rightColumn, ColumnName.of(columnsToMatch[i].rightColumn));
        }
        final ArrayList<MatchPair> columnsToAddAfterRename = new ArrayList<>(realColumnsToAdd.length);
        for (MatchPair matchPair : realColumnsToAdd) {
            columnsToAddAfterRename.add(new MatchPair(matchPair.leftColumn, matchPair.leftColumn));
            if (!columnsToMatchSet.contains(matchPair.leftColumn)) {
                columnsToUngroupBy.add(matchPair.leftColumn);
            }
            columnsToAddSelectColumns.put(matchPair.leftColumn,
                    Selectable.of(ColumnName.of(matchPair.leftColumn), ColumnName.of(matchPair.rightColumn)));
        }

        return QueryPerformanceRecorder
                .withNugget("join(" + matchString(columnsToMatch) + ", " + matchString(realColumnsToAdd) + ")", () -> {
                    boolean sentinelAdded = false;
                    final Table rightTable;
                    if (columnsToUngroupBy.isEmpty()) {
                        rightTable = rightTableCandidate.updateView("__sentinel__=null");
                        columnsToUngroupBy.add("__sentinel__");
                        columnsToAddSelectColumns.put("__sentinel__", ColumnName.of("__sentinel__"));
                        columnsToAddAfterRename.add(new MatchPair("__sentinel__", "__sentinel__"));
                        sentinelAdded = true;
                    } else {
                        rightTable = rightTableCandidate;
                    }

                    final Table rightGrouped = rightTable.groupBy(rightColumnsToMatch)
                            .view(columnsToAddSelectColumns.values());
                    final Table naturalJoinResult = naturalJoin(rightGrouped, columnsToMatch,
                            columnsToAddAfterRename.toArray(MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY));
                    final Table ungroupedResult = naturalJoinResult
                            .ungroup(columnsToUngroupBy.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));

                    maybeCopyColumnDescriptions(ungroupedResult, rightTable, columnsToMatch, realColumnsToAdd);

                    return sentinelAdded ? ungroupedResult.dropColumns("__sentinel__") : ungroupedResult;
                });
    }

    /**
     * The leftColumns are the column sources for the snapshot-triggering table. The rightColumns are the column sources
     * for the table being snapshotted. The leftRowSet refers to snapshots that we want to take. Typically this rowSet
     * is expected to have size 1, but in some cases it could be larger. The rightRowSet refers to the rowSet of the
     * current table. Therefore we want to take leftRowSet.size() snapshots, each of which being rightRowSet.size() in
     * size.
     *
     * @param leftColumns Columns making up the triggering data
     * @param leftRowSet The currently triggering rows
     * @param rightColumns Columns making up the data being snapshotted
     * @param rightRowSet The data to snapshot
     * @param dest The ColumnSources in which to store the data. The keys are drawn from leftColumns.keys() and\
     *        rightColumns.keys()
     * @param destOffset The offset in the 'dest' ColumnSources at which to write data
     * @return The new dest ColumnSource size, calculated as {@code destOffset + leftRowSet.size() * rightRowSet.size()}
     */
    private static long snapshotHistoryInternal(
            @NotNull Map<String, ? extends ColumnSource<?>> leftColumns, @NotNull RowSet leftRowSet,
            @NotNull Map<String, ChunkSource.WithPrev<? extends Values>> rightColumns, @NotNull RowSet rightRowSet,
            @NotNull Map<String, ? extends WritableColumnSource<?>> dest, long destOffset) {
        assert leftColumns.size() + rightColumns.size() == dest.size();
        if (leftRowSet.isEmpty() || rightRowSet.isEmpty()) {
            // Nothing to do.
            return destOffset;
        }

        final long newCapacity = destOffset + leftRowSet.size() * rightRowSet.size();
        // Ensure all the capacities
        for (WritableColumnSource<?> ws : dest.values()) {
            ws.ensureCapacity(newCapacity);
        }

        final int rightSize = rightRowSet.intSize();
        long[] destOffsetHolder = new long[] {destOffset};
        // For each key on the snapshotting side
        leftRowSet.forAllRowKeys(snapshotKey -> {
            final long doff = destOffsetHolder[0];
            destOffsetHolder[0] += rightSize;
            try (final RowSet destRowSet = RowSetFactory.fromRange(doff, doff + rightSize - 1)) {
                SnapshotUtils.copyStampColumns(leftColumns, snapshotKey, dest, destRowSet);
                SnapshotUtils.copyDataColumns(rightColumns, rightRowSet, dest, destRowSet, false);
            }
        });
        return newCapacity;
    }

    public Table snapshotHistory(final Table rightTable) {
        return QueryPerformanceRecorder.withNugget("snapshotHistory", rightTable.sizeForInstrumentation(), () -> {
            checkInitiateOperation();

            // resultColumns initially contains the left columns, then we insert the right columns into it
            final Map<String, ArrayBackedColumnSource<?>> resultColumns =
                    SnapshotUtils.createColumnSourceMap(this.getColumnSourceMap(),
                            ArrayBackedColumnSource::getMemoryColumnSource);
            final Map<String, ArrayBackedColumnSource<?>> rightColumns =
                    SnapshotUtils.createColumnSourceMap(rightTable.getColumnSourceMap(),
                            ArrayBackedColumnSource::getMemoryColumnSource);
            resultColumns.putAll(rightColumns);

            // BTW, we don't track prev because these items are never modified or removed.
            final Table leftTable = this; // For readability.
            final Map<String, ? extends ColumnSource<?>> triggerStampColumns =
                    SnapshotUtils.generateTriggerStampColumns(leftTable);
            final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns =
                    SnapshotUtils.generateSnapshotDataColumns(rightTable);
            final long initialSize = snapshotHistoryInternal(triggerStampColumns, leftTable.getRowSet(),
                    snapshotDataColumns, rightTable.getRowSet(),
                    resultColumns, 0);
            final TrackingWritableRowSet resultRowSet =
                    RowSetFactory.flat(initialSize).toTracking();
            final QueryTable result = new QueryTable(resultRowSet, resultColumns);
            if (isRefreshing()) {
                listenForUpdates(new ShiftObliviousListenerImpl("snapshotHistory" + resultColumns.keySet(),
                        this, result) {
                    private long lastKey = rowSet.lastRowKey();

                    @Override
                    public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
                        Assert.assertion(removed.size() == 0, "removed.size() == 0",
                                removed, "removed");
                        Assert.assertion(modified.size() == 0, "modified.size() == 0",
                                modified, "modified");
                        if (added.size() == 0 || rightTable.size() == 0) {
                            return;
                        }
                        Assert.assertion(added.firstRowKey() > lastKey, "added.firstRowKey() > lastRowKey",
                                lastKey, "lastRowKey", added, "added");
                        final long oldSize = resultRowSet.size();
                        final long newSize = snapshotHistoryInternal(triggerStampColumns, added,
                                snapshotDataColumns, rightTable.getRowSet(),
                                resultColumns, oldSize);
                        final RowSet addedSnapshots = RowSetFactory.fromRange(oldSize, newSize - 1);
                        resultRowSet.insert(addedSnapshots);
                        lastKey = rowSet.lastRowKey();
                        result.notifyListeners(addedSnapshots, RowSetFactory.empty(),
                                RowSetFactory.empty());
                    }

                    @Override
                    public boolean canExecute(final long step) {
                        return rightTable.satisfied(step) && super.canExecute(step);
                    }
                });
            }
            result.setFlat();
            return result;
        });
    }

    public Table silent() {
        return new QueryTable(getRowSet(), getColumnSourceMap());
    }

    @Override
    public Table snapshot(Table baseTable, boolean doInitialSnapshot, String... stampColumns) {
        return QueryPerformanceRecorder.withNugget(
                "snapshot(baseTable, " + doInitialSnapshot + ", " + Arrays.toString(stampColumns) + ")",
                baseTable.sizeForInstrumentation(), () -> {

                    // 'stampColumns' specifies a subset of this table's columns to use, but if stampColumns is empty,
                    // we get
                    // a view containing all of the columns (in that case, basically we get this table back).
                    QueryTable viewTable = (QueryTable) view(stampColumns);
                    // Due to the above logic, we need to pull the actual set of column names back from the viewTable.
                    // Whatever viewTable came back from the above, we do the snapshot
                    return viewTable.snapshotInternal(baseTable, doInitialSnapshot,
                            viewTable.getDefinition().getColumnNamesArray());
                });
    }

    private Table snapshotInternal(Table tableToSnapshot, boolean doInitialSnapshot, String... stampColumns) {
        // TODO: we would like to make this operation UGP safe, instead of requiring the lock here; there are two tables
        // but we do only need to listen to one of them; however we are dependent on two of them
        checkInitiateOperation();

        // There are no LazySnapshotTableProviders in the system currently, but they may be used for multicast
        // distribution systems and similar integrations.

        // If this table provides a lazy snapshot version, we should use that instead for the snapshot, this allows us
        // to run the table only immediately before the snapshot occurs. Because we know that we are uninterested
        // in things like previous values, it can save a significant amount of CPU to only run the table when
        // needed.
        final boolean lazySnapshot = tableToSnapshot instanceof LazySnapshotTableProvider;
        if (lazySnapshot) {
            tableToSnapshot = ((LazySnapshotTableProvider) tableToSnapshot).getLazySnapshotTable();
        } else if (tableToSnapshot instanceof UncoalescedTable) {
            // something that needs coalescing I guess
            tableToSnapshot = tableToSnapshot.coalesce();
        }

        if (isRefreshing()) {
            checkInitiateOperation(tableToSnapshot);
        }

        // Establish the "right" columns using the same names and types as the table being snapshotted
        final Map<String, ArrayBackedColumnSource<?>> resultRightColumns =
                SnapshotUtils.createColumnSourceMap(tableToSnapshot.getColumnSourceMap(),
                        ArrayBackedColumnSource::getMemoryColumnSource);

        // Now make the "left" columns (namely, the "snapshot key" columns). Because this flavor of "snapshot" only
        // keeps a single snapshot, each snapshot key column will have the same value in every row. So for efficiency we
        // use a SingleValueColumnSource for these columns.
        final Map<String, SingleValueColumnSource<?>> resultLeftColumns = new LinkedHashMap<>();
        for (String stampColumn : stampColumns) {
            final Class<?> stampColumnType = getColumnSource(stampColumn).getType();
            resultLeftColumns.put(stampColumn, SingleValueColumnSource.getSingleValueColumnSource(stampColumnType));
        }

        // make our result table
        final Map<String, ColumnSource<?>> allColumns = new LinkedHashMap<>(resultRightColumns);
        allColumns.putAll(resultLeftColumns);
        if (allColumns.size() != resultLeftColumns.size() + resultRightColumns.size()) {
            throwColumnConflictMessage(resultLeftColumns.keySet(), resultRightColumns.keySet());
        }

        final QueryTable result =
                new QueryTable(RowSetFactory.empty().toTracking(), allColumns);
        final SnapshotInternalListener listener = new SnapshotInternalListener(this, lazySnapshot, tableToSnapshot,
                result, resultLeftColumns, resultRightColumns, result.getRowSet().writableCast());

        if (doInitialSnapshot) {
            if (!isRefreshing() && tableToSnapshot.isRefreshing() && !lazySnapshot) {
                // if we are making a static copy of the table, we must ensure that it does not change out from under us
                ConstructSnapshot.callDataSnapshotFunction("snapshotInternal",
                        ConstructSnapshot.makeSnapshotControl(false, tableToSnapshot.isRefreshing(),
                                (NotificationStepSource) tableToSnapshot),
                        (usePrev, beforeClockUnused) -> {
                            listener.doSnapshot(false, usePrev);
                            result.getRowSet().writableCast().initializePreviousValue();
                            return true;
                        });

            } else {
                listener.doSnapshot(false, false);
            }
        }

        if (isRefreshing()) {
            startTrackingPrev(allColumns.values());
            listenForUpdates(listener);
        }

        result.setFlat();

        return result;
    }

    @Override
    public Table snapshotIncremental(final Table tableToSnapshot, final boolean doInitialSnapshot,
            final String... stampColumns) {
        return QueryPerformanceRecorder.withNugget("snapshotIncremental(tableToSnapshot, " + doInitialSnapshot + ", "
                + Arrays.toString(stampColumns) + ")", tableToSnapshot.sizeForInstrumentation(), () -> {
                    checkInitiateOperation();

                    final QueryTable rightTable =
                            (QueryTable) (tableToSnapshot instanceof UncoalescedTable ? tableToSnapshot.coalesce()
                                    : tableToSnapshot);
                    rightTable.checkInitiateOperation();

                    // Use the given columns (if specified); otherwise an empty array means all of my columns
                    final String[] useStampColumns = stampColumns.length == 0
                            ? getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)
                            : stampColumns;

                    final Map<String, ColumnSource<?>> leftColumns = new LinkedHashMap<>();
                    for (String stampColumn : useStampColumns) {
                        leftColumns.put(stampColumn,
                                SnapshotUtils.maybeTransformToDirectVectorColumnSource(getColumnSource(stampColumn)));
                    }

                    final Map<String, SparseArrayColumnSource<?>> resultLeftColumns = new LinkedHashMap<>();
                    for (Map.Entry<String, ColumnSource<?>> entry : leftColumns.entrySet()) {
                        final String name = entry.getKey();
                        final ColumnSource<?> cs = entry.getValue();
                        final Class<?> type = cs.getType();
                        final SparseArrayColumnSource<?> stampDest = Vector.class.isAssignableFrom(type)
                                ? SparseArrayColumnSource.getSparseMemoryColumnSource(type, cs.getComponentType())
                                : SparseArrayColumnSource.getSparseMemoryColumnSource(type);

                        resultLeftColumns.put(name, stampDest);
                    }

                    final Map<String, SparseArrayColumnSource<?>> resultRightColumns =
                            SnapshotUtils.createColumnSourceMap(rightTable.getColumnSourceMap(),
                                    SparseArrayColumnSource::getSparseMemoryColumnSource);

                    final Map<String, SparseArrayColumnSource<?>> resultColumns =
                            new LinkedHashMap<>(resultRightColumns);
                    resultColumns.putAll(resultLeftColumns);
                    if (resultColumns.size() != resultLeftColumns.size() + resultRightColumns.size()) {
                        throwColumnConflictMessage(resultLeftColumns.keySet(), resultRightColumns.keySet());
                    }

                    final QueryTable resultTable =
                            new QueryTable(RowSetFactory.empty().toTracking(),
                                    resultColumns);

                    if (isRefreshing() && rightTable.isRefreshing()) {

                        // What's happening here: the left table gets "listener" (some complicated logic that has access
                        // to the coalescer) whereas the right table (above) gets the one-liner above (but which also
                        // has access to the same coalescer). So when the right table sees updates they are simply fed
                        // to the coalescer.
                        // The coalescer's job is just to remember what rows have changed. When the *left* table gets
                        // updates, then the SnapshotIncrementalListener gets called, which does all the snapshotting
                        // work.

                        final ListenerRecorder rightListenerRecorder =
                                new ListenerRecorder("snapshotIncremental (rightTable)", rightTable, resultTable);
                        rightTable.listenForUpdates(rightListenerRecorder);

                        final ListenerRecorder leftListenerRecorder =
                                new ListenerRecorder("snapshotIncremental (leftTable)", this, resultTable);
                        listenForUpdates(leftListenerRecorder);

                        final SnapshotIncrementalListener listener =
                                new SnapshotIncrementalListener(this, resultTable, resultColumns,
                                        rightListenerRecorder, leftListenerRecorder, rightTable, leftColumns);

                        rightListenerRecorder.setMergedListener(listener);
                        leftListenerRecorder.setMergedListener(listener);
                        resultTable.addParentReference(listener);

                        if (doInitialSnapshot) {
                            listener.doFirstSnapshot(true);
                        }

                        startTrackingPrev(resultColumns.values());
                        resultTable.getRowSet().writableCast().initializePreviousValue();
                    } else if (doInitialSnapshot) {
                        SnapshotIncrementalListener.copyRowsToResult(rightTable.getRowSet(), this,
                                SnapshotUtils.generateSnapshotDataColumns(rightTable),
                                leftColumns, resultColumns);
                        resultTable.getRowSet().writableCast().insert(rightTable.getRowSet());
                        resultTable.getRowSet().writableCast().initializePreviousValue();
                    } else if (isRefreshing()) {
                        // we are not doing an initial snapshot, but are refreshing so need to take a snapshot of our
                        // (static)
                        // right table on the very first tick of the leftTable
                        listenForUpdates(
                                new ListenerImpl("snapshotIncremental (leftTable)", this, resultTable) {
                                    @Override
                                    public void onUpdate(TableUpdate upstream) {
                                        SnapshotIncrementalListener.copyRowsToResult(rightTable.getRowSet(),
                                                QueryTable.this, SnapshotUtils.generateSnapshotDataColumns(rightTable),
                                                leftColumns, resultColumns);
                                        resultTable.getRowSet().writableCast().insert(rightTable.getRowSet());
                                        resultTable.notifyListeners(resultTable.getRowSet().copy(),
                                                RowSetFactory.empty(),
                                                RowSetFactory.empty());
                                        removeUpdateListener(this);
                                    }
                                });
                    }

                    return resultTable;
                });
    }

    private static void throwColumnConflictMessage(Set<String> left, Set<String> right) {
        Iterable<String> conflicts = left.stream().filter(right::contains)::iterator;
        throw new RuntimeException("Column name conflicts: " + IterableUtils.makeCommaSeparatedList(conflicts));
    }

    /**
     * If we have a column source that is a complex type, but can be reinterpreted or transformed into a simpler type,
     * do the transformation, otherwise return the original column source.
     *
     * @param columnSource the column source to possibly reinterpret
     * @return the transformed column source, or the original column source if there is not a relevant transformation
     */
    static ColumnSource<?> maybeTransformToPrimitive(final ColumnSource<?> columnSource) {
        if (DateTime.class.isAssignableFrom(columnSource.getType())) {
            if (columnSource.allowsReinterpret(long.class)) {
                return columnSource.reinterpret(long.class);
            } else {
                // noinspection unchecked
                final ColumnSource<DateTime> columnSourceAsDateTime = (ColumnSource<DateTime>) columnSource;
                return new DateTimeAsLongColumnSource(columnSourceAsDateTime);
            }
        }

        if (Boolean.class.isAssignableFrom(columnSource.getType())) {
            if (columnSource.allowsReinterpret(byte.class)) {
                return columnSource.reinterpret(byte.class);
            } else {
                // noinspection unchecked
                final ColumnSource<Boolean> columnSourceAsBoolean = (ColumnSource<Boolean>) columnSource;
                return new BooleanAsByteColumnSource(columnSourceAsBoolean);
            }
        }

        return columnSource;
    }

    @Override
    public Table sort(Collection<SortColumn> columnsToSortBy) {
        final SortPair[] sortPairs = SortPair.from(columnsToSortBy);
        if (sortPairs.length == 0) {
            if (isRefreshing()) {
                manageWithCurrentScope();
            }
            return this;
        } else if (sortPairs.length == 1) {
            final String columnName = sortPairs[0].getColumn();
            final SortingOrder order = sortPairs[0].getOrder();
            if (SortedColumnsAttribute.isSortedBy(this, columnName, order)) {
                if (isRefreshing()) {
                    manageWithCurrentScope();
                }
                return this;
            }
        }

        return getResult(new SortOperation(this, sortPairs));
    }

    /**
     * This is the smallest "base" that is used by the ungroup function. Each row from the input table is allocated
     * 2^minimumUngroupBase rows in the output table at startup. If rows are added to the table, this base may need to
     * grow. If a single row in the input has more than 2^base rows, then the base must change for all of the rows.
     */
    private static int minimumUngroupBase = 10;

    /**
     * For unit testing, it can be useful to reduce the minimum ungroup base.
     *
     * @param minimumUngroupBase the minimum ungroup base for newly created ungrouped tables.
     * @return The old value of minimumUngroupBase
     */
    static int setMinimumUngroupBase(int minimumUngroupBase) {
        final int oldValue = QueryTable.minimumUngroupBase;
        QueryTable.minimumUngroupBase = minimumUngroupBase;
        return oldValue;
    }

    /**
     * The reverse operation returns a new table that is the same as the original table, but the first row is last, and
     * the last row is first. This is an internal API to be used by .raj(), but is accessible for unit tests.
     *
     * @return the reversed table
     */
    @Override
    public Table reverse() {
        return getResult(new ReverseOperation(this));
    }

    @Override
    public Table ungroup(boolean nullFill, String... columnsToUngroupBy) {
        return QueryPerformanceRecorder.withNugget("ungroup(" + Arrays.toString(columnsToUngroupBy) + ")",
                sizeForInstrumentation(), () -> {
                    if (columnsToUngroupBy.length == 0) {
                        if (isRefreshing()) {
                            manageWithCurrentScope();
                        }
                        return this;
                    }

                    checkInitiateOperation();

                    final Map<String, ColumnSource<?>> arrayColumns = new HashMap<>();
                    final Map<String, ColumnSource<?>> vectorColumns = new HashMap<>();
                    for (String name : columnsToUngroupBy) {
                        ColumnSource<?> column = getColumnSource(name);
                        if (column.getType().isArray()) {
                            arrayColumns.put(name, column);
                        } else if (Vector.class.isAssignableFrom(column.getType())) {
                            vectorColumns.put(name, column);
                        } else {
                            throw new RuntimeException("Column " + name + " is not an array");
                        }
                    }
                    final long[] sizes = new long[intSize("ungroup")];
                    long maxSize = computeMaxSize(rowSet, arrayColumns, vectorColumns, null, sizes, nullFill);
                    final int initialBase = Math.max(64 - Long.numberOfLeadingZeros(maxSize), minimumUngroupBase);
                    final CrossJoinShiftState shiftState = new CrossJoinShiftState(initialBase);

                    final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
                    for (Map.Entry<String, ColumnSource<?>> es : getColumnSourceMap().entrySet()) {
                        final ColumnSource<?> column = es.getValue();
                        final String name = es.getKey();
                        final ColumnSource<?> result;
                        if (vectorColumns.containsKey(name) || arrayColumns.containsKey(name)) {
                            final UngroupedColumnSource<?> ungroupedSource =
                                    UngroupedColumnSource.getColumnSource(column);
                            ungroupedSource.initializeBase(initialBase);
                            result = ungroupedSource;
                        } else {
                            result = new BitShiftingColumnSource<>(shiftState, column);
                        }
                        resultMap.put(name, result);
                    }
                    final QueryTable result = new QueryTable(
                            getUngroupIndex(sizes, RowSetFactory.builderRandom(), initialBase, rowSet)
                                    .build().toTracking(),
                            resultMap);
                    if (isRefreshing()) {
                        startTrackingPrev(resultMap.values());

                        listenForUpdates(new ShiftObliviousListenerImpl(
                                "ungroup(" + Arrays.deepToString(columnsToUngroupBy) + ')',
                                this, result) {

                            @Override
                            public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
                                intSize("ungroup");

                                int newBase = shiftState.getNumShiftBits();
                                RowSetBuilderRandom ungroupAdded = RowSetFactory.builderRandom();
                                RowSetBuilderRandom ungroupModified = RowSetFactory.builderRandom();
                                RowSetBuilderRandom ungroupRemoved = RowSetFactory.builderRandom();
                                newBase = evaluateIndex(added, ungroupAdded, newBase);
                                newBase = evaluateModified(modified, ungroupModified, ungroupAdded, ungroupRemoved,
                                        newBase);
                                if (newBase > shiftState.getNumShiftBits()) {
                                    rebase(newBase + 1);
                                } else {
                                    evaluateRemovedIndex(removed, ungroupRemoved);
                                    final RowSet removedRowSet = ungroupRemoved.build();
                                    final RowSet addedRowSet = ungroupAdded.build();
                                    result.getRowSet().writableCast().update(addedRowSet, removedRowSet);
                                    final RowSet modifiedRowSet = ungroupModified.build();

                                    if (!modifiedRowSet.subsetOf(result.getRowSet())) {
                                        final RowSet missingModifications = modifiedRowSet.minus(result.getRowSet());
                                        log.error().append("Result TrackingWritableRowSet: ")
                                                .append(result.getRowSet().toString())
                                                .endl();
                                        log.error().append("Missing modifications: ")
                                                .append(missingModifications.toString()).endl();
                                        log.error().append("Added: ").append(addedRowSet.toString()).endl();
                                        log.error().append("Modified: ").append(modifiedRowSet.toString()).endl();
                                        log.error().append("Removed: ").append(removedRowSet.toString()).endl();

                                        for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                                            ColumnSource<?> arrayColumn = es.getValue();
                                            String name = es.getKey();

                                            RowSet.Iterator iterator = rowSet.iterator();
                                            for (int i = 0; i < rowSet.size(); i++) {
                                                final long next = iterator.nextLong();
                                                int size = (arrayColumn.get(next) == null ? 0
                                                        : Array.getLength(arrayColumn.get(next)));
                                                int prevSize = (arrayColumn.getPrev(next) == null ? 0
                                                        : Array.getLength(arrayColumn.getPrev(next)));
                                                log.error().append(name).append("[").append(i).append("] ").append(size)
                                                        .append(" -> ").append(prevSize).endl();
                                            }
                                        }

                                        for (Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
                                            ColumnSource<?> arrayColumn = es.getValue();
                                            String name = es.getKey();
                                            RowSet.Iterator iterator = rowSet.iterator();

                                            for (int i = 0; i < rowSet.size(); i++) {
                                                final long next = iterator.nextLong();
                                                long size = (arrayColumn.get(next) == null ? 0
                                                        : ((Vector<?>) arrayColumn.get(next)).size());
                                                long prevSize = (arrayColumn.getPrev(next) == null ? 0
                                                        : ((Vector<?>) arrayColumn.getPrev(next)).size());
                                                log.error().append(name).append("[").append(i).append("] ").append(size)
                                                        .append(" -> ").append(prevSize).endl();
                                            }
                                        }

                                        Assert.assertion(false, "modifiedRowSet.subsetOf(result.build())",
                                                modifiedRowSet, "modifiedRowSet", result.getRowSet(), "result.build()",
                                                shiftState.getNumShiftBits(), "shiftState.getNumShiftBits()", newBase,
                                                "newBase");
                                    }

                                    for (ColumnSource<?> source : resultMap.values()) {
                                        if (source instanceof UngroupedColumnSource) {
                                            ((UngroupedColumnSource<?>) source).setBase(newBase);
                                        }
                                    }

                                    result.notifyListeners(addedRowSet, removedRowSet, modifiedRowSet);
                                }
                            }

                            private void rebase(final int newBase) {
                                final WritableRowSet newRowSet = getUngroupIndex(
                                        computeSize(getRowSet(), arrayColumns, vectorColumns, nullFill),
                                        RowSetFactory.builderRandom(), newBase, getRowSet())
                                                .build();
                                final TrackingWritableRowSet rowSet = result.getRowSet().writableCast();
                                final RowSet added = newRowSet.minus(rowSet);
                                final RowSet removed = rowSet.minus(newRowSet);
                                final WritableRowSet modified = newRowSet;
                                modified.retain(rowSet);
                                rowSet.update(added, removed);
                                for (ColumnSource<?> source : resultMap.values()) {
                                    if (source instanceof UngroupedColumnSource) {
                                        ((UngroupedColumnSource<?>) source).setBase(newBase);
                                    }
                                }
                                shiftState.setNumShiftBitsAndUpdatePrev(newBase);
                                result.notifyListeners(added, removed, modified);
                            }

                            private int evaluateIndex(final RowSet rowSet, final RowSetBuilderRandom ungroupBuilder,
                                    final int newBase) {
                                if (rowSet.size() > 0) {
                                    final long[] modifiedSizes = new long[rowSet.intSize("ungroup")];
                                    final long maxSize = computeMaxSize(rowSet, arrayColumns, vectorColumns, null,
                                            modifiedSizes, nullFill);
                                    final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);
                                    getUngroupIndex(modifiedSizes, ungroupBuilder, shiftState.getNumShiftBits(),
                                            rowSet);
                                    return Math.max(newBase, minBase);
                                }
                                return newBase;
                            }

                            private void evaluateRemovedIndex(final RowSet rowSet,
                                    final RowSetBuilderRandom ungroupBuilder) {
                                if (rowSet.size() > 0) {
                                    final long[] modifiedSizes = new long[rowSet.intSize("ungroup")];
                                    computePrevSize(rowSet, arrayColumns, vectorColumns, modifiedSizes, nullFill);
                                    getUngroupIndex(modifiedSizes, ungroupBuilder, shiftState.getNumShiftBits(),
                                            rowSet);
                                }
                            }

                            private int evaluateModified(final RowSet rowSet,
                                    final RowSetBuilderRandom modifyBuilder,
                                    final RowSetBuilderRandom addedBuilded,
                                    final RowSetBuilderRandom removedBuilder,
                                    final int newBase) {
                                if (rowSet.size() > 0) {
                                    final long maxSize = computeModifiedIndicesAndMaxSize(rowSet, arrayColumns,
                                            vectorColumns, null, modifyBuilder, addedBuilded, removedBuilder,
                                            shiftState.getNumShiftBits(), nullFill);
                                    final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);
                                    return Math.max(newBase, minBase);
                                }
                                return newBase;
                            }
                        });
                    }
                    return result;
                });
    }

    private long computeModifiedIndicesAndMaxSize(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, String referenceColumn, RowSetBuilderRandom modifyBuilder,
            RowSetBuilderRandom addedBuilded, RowSetBuilderRandom removedBuilder, long base, boolean nullFill) {
        if (nullFill) {
            return computeModifiedIndicesAndMaxSizeNullFill(rowSet, arrayColumns, vectorColumns, referenceColumn,
                    modifyBuilder, addedBuilded, removedBuilder, base);
        }
        return computeModifiedIndicesAndMaxSizeNormal(rowSet, arrayColumns, vectorColumns, referenceColumn,
                modifyBuilder, addedBuilded, removedBuilder, base);
    }

    private long computeModifiedIndicesAndMaxSizeNullFill(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, String referenceColumn, RowSetBuilderRandom modifyBuilder,
            RowSetBuilderRandom addedBuilded, RowSetBuilderRandom removedBuilder, long base) {
        long maxSize = 0;
        final RowSet.Iterator iterator = rowSet.iterator();
        for (int i = 0; i < rowSet.size(); i++) {
            long maxCur = 0;
            long maxPrev = 0;
            final long next = iterator.nextLong();
            for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                Object array = arrayColumn.get(next);
                final int size = (array == null ? 0 : Array.getLength(array));
                maxCur = Math.max(maxCur, size);
                Object prevArray = arrayColumn.getPrev(next);
                final int prevSize = (prevArray == null ? 0 : Array.getLength(prevArray));
                maxPrev = Math.max(maxPrev, prevSize);
            }
            for (Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                Vector<?> array = (Vector<?>) arrayColumn.get(next);
                final long size = (array == null ? 0 : array.size());
                maxCur = Math.max(maxCur, size);
                Vector<?> prevArray = (Vector<?>) arrayColumn.getPrev(next);
                final long prevSize = (prevArray == null ? 0 : prevArray.size());
                maxPrev = Math.max(maxPrev, prevSize);
            }
            maxSize = maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, maxCur, next,
                    maxPrev, base);
        }
        return maxSize;
    }

    private long computeModifiedIndicesAndMaxSizeNormal(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, String referenceColumn, RowSetBuilderRandom modifyBuilder,
            RowSetBuilderRandom addedBuilded, RowSetBuilderRandom removedBuilder, long base) {
        long maxSize = 0;
        boolean sizeIsInitialized = false;
        long[] sizes = new long[rowSet.intSize("ungroup")];
        for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
            ColumnSource<?> arrayColumn = es.getValue();
            String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                RowSet.Iterator iterator = rowSet.iterator();
                for (int i = 0; i < rowSet.size(); i++) {
                    final long next = iterator.nextLong();
                    Object array = arrayColumn.get(next);
                    sizes[i] = (array == null ? 0 : Array.getLength(array));
                    Object prevArray = arrayColumn.getPrev(next);
                    int prevSize = (prevArray == null ? 0 : Array.getLength(prevArray));
                    maxSize = maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, sizes[i],
                            next, prevSize, base);
                }
            } else {
                RowSet.Iterator iterator = rowSet.iterator();
                for (int i = 0; i < rowSet.size(); i++) {
                    long k = iterator.nextLong();
                    Assert.assertion(sizes[i] == Array.getLength(arrayColumn.get(k)),
                            "sizes[i] == Array.getLength(arrayColumn.get(k))",
                            referenceColumn, "referenceColumn", name, "name", k, "row");
                }

            }
        }
        for (Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
            ColumnSource<?> arrayColumn = es.getValue();
            String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                RowSet.Iterator iterator = rowSet.iterator();
                for (int i = 0; i < rowSet.size(); i++) {
                    final long next = iterator.nextLong();
                    Vector<?> array = (Vector<?>) arrayColumn.get(next);
                    sizes[i] = (array == null ? 0 : array.size());
                    Vector<?> prevArray = (Vector<?>) arrayColumn.getPrev(next);
                    long prevSize = (prevArray == null ? 0 : prevArray.size());
                    maxSize = maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, sizes[i],
                            next, prevSize, base);
                }
            } else {
                RowSet.Iterator iterator = rowSet.iterator();
                for (int i = 0; i < rowSet.size(); i++) {
                    final long next = iterator.nextLong();
                    Assert.assertion(sizes[i] == 0 && arrayColumn.get(next) == null ||
                            sizes[i] == ((Vector<?>) arrayColumn.get(next)).size(),
                            "sizes[i] == ((Vector)arrayColumn.get(i)).size()",
                            referenceColumn, "referenceColumn", name, "arrayColumn.getName()", i, "row");
                }
            }
        }
        return maxSize;
    }

    private long maxAndIndexUpdateForRow(RowSetBuilderRandom modifyBuilder, RowSetBuilderRandom addedBuilded,
            RowSetBuilderRandom removedBuilder, long maxSize, long size, long rowKey, long prevSize, long base) {
        rowKey = rowKey << base;
        Require.requirement(rowKey >= 0 && (size == 0 || (rowKey + size - 1 >= 0)),
                "rowKey >= 0 && (size == 0 || (rowKey + size - 1 >= 0))");
        if (size == prevSize) {
            if (size > 0) {
                modifyBuilder.addRange(rowKey, rowKey + size - 1);
            }
        } else if (size < prevSize) {
            if (size > 0) {
                modifyBuilder.addRange(rowKey, rowKey + size - 1);
            }
            removedBuilder.addRange(rowKey + size, rowKey + prevSize - 1);
        } else {
            if (prevSize > 0) {
                modifyBuilder.addRange(rowKey, rowKey + prevSize - 1);
            }
            addedBuilded.addRange(rowKey + prevSize, rowKey + size - 1);
        }
        maxSize = Math.max(maxSize, size);
        return maxSize;
    }

    @SuppressWarnings("SameParameterValue")
    private static long computeMaxSize(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, String referenceColumn, long[] sizes, boolean nullFill) {
        if (nullFill) {
            return computeMaxSizeNullFill(rowSet, arrayColumns, vectorColumns, sizes);
        }

        return computeMaxSizeNormal(rowSet, arrayColumns, vectorColumns, referenceColumn, sizes);
    }

    private static long computeMaxSizeNullFill(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, long[] sizes) {
        long maxSize = 0;
        final RowSet.Iterator iterator = rowSet.iterator();
        for (int i = 0; i < rowSet.size(); i++) {
            long localMax = 0;
            final long nextIndex = iterator.nextLong();
            for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.get(nextIndex);
                final long size = (array == null ? 0 : Array.getLength(array));
                maxSize = Math.max(maxSize, size);
                localMax = Math.max(localMax, size);

            }
            for (Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                        && ((UngroupableColumnSource) arrayColumn).isUngroupable();
                final long size;
                if (isUngroupable) {
                    size = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(nextIndex);
                } else {
                    final Vector<?> vector = (Vector<?>) arrayColumn.get(nextIndex);
                    size = vector != null ? vector.size() : 0;
                }
                maxSize = Math.max(maxSize, size);
                localMax = Math.max(localMax, size);
            }
            sizes[i] = localMax;
        }
        return maxSize;
    }


    private static long computeMaxSizeNormal(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, String referenceColumn, long[] sizes) {
        long maxSize = 0;
        boolean sizeIsInitialized = false;
        for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
            ColumnSource<?> arrayColumn = es.getValue();
            String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                RowSet.Iterator iterator = rowSet.iterator();
                for (int i = 0; i < rowSet.size(); i++) {
                    Object array = arrayColumn.get(iterator.nextLong());
                    sizes[i] = (array == null ? 0 : Array.getLength(array));
                    maxSize = Math.max(maxSize, sizes[i]);
                }
            } else {
                RowSet.Iterator iterator = rowSet.iterator();
                for (int i = 0; i < rowSet.size(); i++) {
                    Assert.assertion(sizes[i] == Array.getLength(arrayColumn.get(iterator.nextLong())),
                            "sizes[i] == Array.getLength(arrayColumn.get(i))",
                            referenceColumn, "referenceColumn", name, "name", i, "row");
                }

            }
        }
        for (Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
            final ColumnSource<?> arrayColumn = es.getValue();
            final String name = es.getKey();
            final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                    && ((UngroupableColumnSource) arrayColumn).isUngroupable();

            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                RowSet.Iterator iterator = rowSet.iterator();
                for (int ii = 0; ii < rowSet.size(); ii++) {
                    if (isUngroupable) {
                        sizes[ii] = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(iterator.nextLong());
                    } else {
                        final Vector<?> vector = (Vector<?>) arrayColumn.get(iterator.nextLong());
                        sizes[ii] = vector != null ? vector.size() : 0;
                    }
                    maxSize = Math.max(maxSize, sizes[ii]);
                }
            } else {
                RowSet.Iterator iterator = rowSet.iterator();
                for (int i = 0; i < rowSet.size(); i++) {
                    final long expectedSize;
                    if (isUngroupable) {
                        expectedSize = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(iterator.nextLong());
                    } else {
                        final Vector<?> vector = (Vector<?>) arrayColumn.get(iterator.nextLong());
                        expectedSize = vector != null ? vector.size() : 0;
                    }
                    Assert.assertion(sizes[i] == expectedSize, "sizes[i] == ((Vector)arrayColumn.get(i)).size()",
                            referenceColumn, "referenceColumn", name, "arrayColumn.getName()", i, "row");
                }
            }
        }
        return maxSize;
    }

    private static void computePrevSize(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, long[] sizes, boolean nullFill) {
        if (nullFill) {
            computePrevSizeNullFill(rowSet, arrayColumns, vectorColumns, sizes);
        } else {
            computePrevSizeNormal(rowSet, arrayColumns, vectorColumns, sizes);
        }
    }

    private static void computePrevSizeNullFill(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, long[] sizes) {
        final RowSet.Iterator iterator = rowSet.iterator();
        for (int i = 0; i < rowSet.size(); i++) {
            long localMax = 0;
            final long nextIndex = iterator.nextLong();
            for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.getPrev(nextIndex);
                final long size = (array == null ? 0 : Array.getLength(array));
                localMax = Math.max(localMax, size);

            }
            for (Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                        && ((UngroupableColumnSource) arrayColumn).isUngroupable();
                final long size;
                if (isUngroupable) {
                    size = ((UngroupableColumnSource) arrayColumn).getUngroupedPrevSize(nextIndex);
                } else {
                    final Vector<?> vector = (Vector<?>) arrayColumn.getPrev(nextIndex);
                    size = vector != null ? vector.size() : 0;
                }
                localMax = Math.max(localMax, size);
            }
            sizes[i] = localMax;
        }
    }

    private static void computePrevSizeNormal(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, long[] sizes) {
        for (ColumnSource<?> arrayColumn : arrayColumns.values()) {
            RowSet.Iterator iterator = rowSet.iterator();
            for (int i = 0; i < rowSet.size(); i++) {
                Object array = arrayColumn.getPrev(iterator.nextLong());
                sizes[i] = (array == null ? 0 : Array.getLength(array));
            }
            return; // TODO: WTF??
        }
        for (ColumnSource<?> arrayColumn : vectorColumns.values()) {
            final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                    && ((UngroupableColumnSource) arrayColumn).isUngroupable();

            RowSet.Iterator iterator = rowSet.iterator();
            for (int i = 0; i < rowSet.size(); i++) {
                if (isUngroupable) {
                    sizes[i] = ((UngroupableColumnSource) arrayColumn).getUngroupedPrevSize(iterator.nextLong());
                } else {
                    Vector<?> array = (Vector<?>) arrayColumn.getPrev(iterator.nextLong());
                    sizes[i] = array == null ? 0 : array.size();
                }
            }
            return; // TODO: WTF??
        }
    }

    private static long[] computeSize(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns, boolean nullFill) {
        if (nullFill) {
            return computeSizeNullFill(rowSet, arrayColumns, vectorColumns);
        }

        return computeSizeNormal(rowSet, arrayColumns, vectorColumns);
    }

    private static long[] computeSizeNullFill(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns) {
        final long[] sizes = new long[rowSet.intSize("ungroup")];
        final RowSet.Iterator iterator = rowSet.iterator();
        for (int i = 0; i < rowSet.size(); i++) {
            long localMax = 0;
            final long nextIndex = iterator.nextLong();
            for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.get(nextIndex);
                final long size = (array == null ? 0 : Array.getLength(array));
                localMax = Math.max(localMax, size);

            }
            for (Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                        && ((UngroupableColumnSource) arrayColumn).isUngroupable();
                final long size;
                if (isUngroupable) {
                    size = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(nextIndex);
                } else {
                    final Vector<?> vector = (Vector<?>) arrayColumn.get(nextIndex);
                    size = vector != null ? vector.size() : 0;
                }
                localMax = Math.max(localMax, size);
            }
            sizes[i] = localMax;
        }
        return sizes;
    }

    private static long[] computeSizeNormal(RowSet rowSet, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> vectorColumns) {
        final long[] sizes = new long[rowSet.intSize("ungroup")];
        for (ColumnSource<?> arrayColumn : arrayColumns.values()) {
            RowSet.Iterator iterator = rowSet.iterator();
            for (int i = 0; i < rowSet.size(); i++) {
                Object array = arrayColumn.get(iterator.nextLong());
                sizes[i] = (array == null ? 0 : Array.getLength(array));
            }
            return sizes; // TODO: WTF??
        }
        for (ColumnSource<?> arrayColumn : vectorColumns.values()) {
            final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                    && ((UngroupableColumnSource) arrayColumn).isUngroupable();

            RowSet.Iterator iterator = rowSet.iterator();
            for (int i = 0; i < rowSet.size(); i++) {
                if (isUngroupable) {
                    sizes[i] = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(iterator.nextLong());
                } else {
                    Vector<?> array = (Vector<?>) arrayColumn.get(iterator.nextLong());
                    sizes[i] = array == null ? 0 : array.size();
                }
            }
            return sizes; // TODO: WTF??
        }
        return null;
    }

    private RowSetBuilderRandom getUngroupIndex(
            final long[] sizes, final RowSetBuilderRandom indexBuilder, final long base, final RowSet rowSet) {
        Assert.assertion(base >= 0 && base <= 63, "base >= 0 && base <= 63", base, "base");
        long mask = ((1L << base) - 1) << (64 - base);
        long lastKey = rowSet.lastRowKey();
        if ((lastKey > 0) && ((lastKey & mask) != 0)) {
            throw new IllegalStateException(
                    "Key overflow detected, perhaps you should flatten your table before calling ungroup.  "
                            + ",lastRowKey=" + lastKey + ", base=" + base);
        }

        int pos = 0;
        for (RowSet.Iterator iterator = rowSet.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            long nextShift = next << base;
            if (sizes[pos] != 0) {
                Assert.assertion(nextShift >= 0, "nextShift >= 0", nextShift, "nextShift", base, "base", next, "next");
                indexBuilder.addRange(nextShift, nextShift + sizes[pos++] - 1);
            } else {
                pos++;
            }
        }
        return indexBuilder;
    }

    @Override
    public Table selectDistinct(Collection<? extends Selectable> columns) {
        return QueryPerformanceRecorder.withNugget("selectDistinct(" + columns + ")",
                sizeForInstrumentation(),
                () -> {
                    final Collection<ColumnName> columnNames = ColumnName.cast(columns).orElse(null);
                    if (columnNames == null) {
                        return view(columns).selectDistinct();
                    }
                    final MemoizedOperationKey aggKey =
                            MemoizedOperationKey.aggBy(Collections.emptyList(), false, null, columnNames);
                    return memoizeResult(aggKey,
                            () -> aggNoMemo(AggregationProcessor.forSelectDistinct(), false, null, columnNames));
                });
    }

    /**
     * Get a {@link Table} that contains a sub-set of the rows from {@code this}. The result will share the same
     * {@link #getColumnSources() column sources} and {@link #getDefinition() definition} as this table.
     *
     * The result will not update on its own, the caller must also establish an appropriate listener to update
     * {@code rowSet} and propagate {@link TableUpdate updates}.
     *
     * No {@link QueryPerformanceNugget nugget} is opened for this table, to prevent operations that call this
     * repeatedly from having an inordinate performance penalty. If callers require a nugget, they must create one in
     * the enclosing operation.
     *
     * @param rowSet The result's {@link #getRowSet() row set}
     * @return A new table sharing this table's column sources with the specified row set
     */
    @Override
    public QueryTable getSubTable(@NotNull final TrackingRowSet rowSet) {
        return getSubTable(rowSet, null, null, CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
    }

    /**
     * Get a {@link Table} that contains a sub-set of the rows from {@code this}. The result will share the same
     * {@link #getColumnSources() column sources} and {@link #getDefinition() definition} as this table.
     *
     * The result will not update on its own, the caller must also establish an appropriate listener to update
     * {@code rowSet} and propagate {@link TableUpdate updates}.
     *
     * This method is intended to be used for composing alternative engine operations, in particular
     * {@link #partitionBy(boolean, String...)}.
     *
     * No {@link QueryPerformanceNugget nugget} is opened for this table, to prevent operations that call this
     * repeatedly from having an inordinate performance penalty. If callers require a nugget, they must create one in
     * the enclosing operation.
     *
     * @param rowSet The result's {@link #getRowSet() row set}
     * @param resultModifiedColumnSet The result's {@link #getModifiedColumnSetForUpdates() modified column set}, or
     *        {@code null} for default initialization
     * @param attributes The result's {@link #attributes}, * or {@code null} for default initialization
     * @param parents Parent references for the result table
     * @return A new table sharing this table's column sources with the specified row set
     */
    public QueryTable getSubTable(
            @NotNull final TrackingRowSet rowSet,
            @Nullable final ModifiedColumnSet resultModifiedColumnSet,
            @Nullable final Map<String, Object> attributes,
            @NotNull final Object... parents) {
        // There is no checkInitiateOperation check here, because partitionBy calls it internally and the RowSet
        // results are not updated internally, but rather externally.
        final QueryTable result = new QueryTable(definition, rowSet, columns, resultModifiedColumnSet, attributes);
        for (final Object parent : parents) {
            result.addParentReference(parent);
        }
        result.setLastNotificationStep(getLastNotificationStep());
        return result;
    }

    /**
     * Copies this table, but with a new set of attributes.
     *
     * @return an identical table; but with a new set of attributes
     */
    @Override
    public Table copy() {
        return copy(StandardOptions.COPY_ALL);
    }

    public Table copy(Predicate<String> shouldCopy) {
        return copy(definition, shouldCopy);
    }

    private enum StandardOptions implements Predicate<String> {
        COPY_ALL {
            @Override
            public boolean test(String attributeName) {
                return true;
            }
        },
        COPY_NONE {
            @Override
            public boolean test(String attributeName) {
                return false;
            }
        }
    }

    public Table copy(TableDefinition definition, Predicate<String> shouldCopy) {
        return QueryPerformanceRecorder.withNugget("copy()", sizeForInstrumentation(), () -> {
            final Mutable<Table> result = new MutableObject<>();

            final SwapListener swapListener = createSwapListenerIfRefreshing(SwapListener::new);
            initializeWithSnapshot("copy", swapListener, (usePrev, beforeClockValue) -> {
                final QueryTable resultTable = new CopiedTable(definition, this);
                propagateFlatness(resultTable);
                if (shouldCopy != StandardOptions.COPY_NONE) {
                    copyAttributes(resultTable, shouldCopy);
                }
                if (swapListener != null) {
                    final ListenerImpl listener = new ListenerImpl("copy()", this, resultTable);
                    swapListener.setListenerAndResult(listener, resultTable);
                    resultTable.addParentReference(swapListener);
                }

                result.setValue(resultTable);

                return true;
            });

            return result.getValue();
        });
    }

    @VisibleForTesting
    static class CopiedTable extends QueryTable {
        private final QueryTable parent;

        private CopiedTable(TableDefinition definition, QueryTable parent) {
            super(definition, parent.rowSet, parent.columns, null, null);
            this.parent = parent;
        }

        @TestUseOnly
        boolean checkParent(Table expectedParent) {
            return expectedParent == this.parent;
        }

        @Override
        public <R> R memoizeResult(MemoizedOperationKey memoKey, Supplier<R> operation) {
            if (memoKey == null || !memoizeResults) {
                return operation.get();
            }

            final boolean attributesCompatible = memoKey.attributesCompatible(parent.attributes, attributes);
            final Supplier<R> computeCachedOperation = attributesCompatible ? () -> {
                final R parentResult = parent.memoizeResult(memoKey, operation);
                if (parentResult instanceof QueryTable) {
                    final Table myResult = ((QueryTable) parentResult).copy(StandardOptions.COPY_NONE);
                    copyAttributes((QueryTable) parentResult, myResult, memoKey.getParentCopyType());
                    copyAttributes(myResult, memoKey.copyType());
                    // noinspection unchecked
                    return (R) myResult;
                }
                return operation.get();
            } : operation;

            return super.memoizeResult(memoKey, computeCachedOperation);
        }
    }

    @Override
    public Table updateBy(@NotNull final UpdateByControl control,
            @NotNull final Collection<? extends UpdateByOperation> ops,
            @NotNull final Collection<? extends ColumnName> byColumns) {
        return QueryPerformanceRecorder.withNugget("updateBy()", sizeForInstrumentation(),
                () -> UpdateBy.updateBy(this, ops, byColumns, control));
    }

    /**
     * For unit tests, provide a method to turn memoization on or off.
     *
     * @param memoizeResults should results be memoized?
     * @return the prior value
     */
    @VisibleForTesting
    public static boolean setMemoizeResults(boolean memoizeResults) {
        final boolean old = QueryTable.memoizeResults;
        QueryTable.memoizeResults = memoizeResults;
        return old;
    }

    /**
     * For unit testing, to simplify debugging.
     */
    @SuppressWarnings("unused")
    void clearMemoizedResults() {
        cachedOperations.clear();
    }

    /**
     * Saves a weak reference to the result of the given operation.
     *
     * @param memoKey a complete description of the operation, if null no memoization is performed
     * @param operation a supplier for the result
     * @return either the cached or newly generated result
     */
    public <R> R memoizeResult(MemoizedOperationKey memoKey, Supplier<R> operation) {
        if (memoKey == null || !memoizeResults) {
            return operation.get();
        }

        final MemoizedResult<R> cachedResult = getMemoizedResult(memoKey, ensureCachedOperations());
        return cachedResult.getOrCompute(operation);
    }

    private Map<MemoizedOperationKey, MemoizedResult<?>> ensureCachedOperations() {
        // noinspection unchecked
        return ensureField(CACHED_OPERATIONS_UPDATER, EMPTY_CACHED_OPERATIONS, ConcurrentHashMap::new);
    }

    @NotNull
    private static <R> MemoizedResult<R> getMemoizedResult(MemoizedOperationKey memoKey,
            Map<MemoizedOperationKey, MemoizedResult<?>> cachedOperations) {
        // noinspection unchecked
        return (MemoizedResult<R>) cachedOperations.computeIfAbsent(memoKey, k -> new MemoizedResult<>());
    }

    private static class MemoizedResult<R> {
        private volatile WeakReference<R> reference;

        R getOrCompute(Supplier<R> operation) {
            final R cachedResult = getIfValid();
            if (cachedResult != null) {
                maybeMarkSystemic(cachedResult);
                return cachedResult;
            }

            synchronized (this) {
                final R cachedResultLocked = getIfValid();
                if (cachedResultLocked != null) {
                    maybeMarkSystemic(cachedResultLocked);
                    return cachedResultLocked;
                }

                final R result;
                result = operation.get();

                reference = new WeakReference<>(result);

                return result;
            }
        }

        private void maybeMarkSystemic(R cachedResult) {
            if (cachedResult instanceof SystemicObject && SystemicObjectTracker.isSystemicThread()) {
                ((SystemicObject) cachedResult).markSystemic();
            }
        }

        R getIfValid() {
            if (reference != null) {
                final R cachedResult = reference.get();
                if (!isFailedTable(cachedResult) && Liveness.verifyCachedObjectForReuse(cachedResult)) {
                    return cachedResult;
                }
            }
            return null;
        }

        private boolean isFailedTable(R cachedResult) {
            return cachedResult instanceof Table && ((Table) cachedResult).isFailed();
        }
    }

    public <T extends DynamicNode & NotificationStepReceiver> T getResult(final Operation<T> operation) {
        if (operation instanceof MemoizableOperation) {
            return memoizeResult(((MemoizableOperation<T>) operation).getMemoizedOperationKey(),
                    () -> getResultNoMemo(operation));
        }
        return getResultNoMemo(operation);
    }

    private <T extends DynamicNode & NotificationStepReceiver> T getResultNoMemo(final Operation<T> operation) {
        return QueryPerformanceRecorder.withNugget(operation.getDescription(), sizeForInstrumentation(), () -> {
            final Mutable<T> resultTable = new MutableObject<>();

            final SwapListener swapListener;
            if (isRefreshing() && operation.snapshotNeeded()) {
                swapListener = operation.newSwapListener(this);
                swapListener.subscribeForUpdates();
            } else {
                swapListener = null;
            }

            initializeWithSnapshot(operation.getLogPrefix(), swapListener, (usePrev, beforeClockValue) -> {
                final Operation.Result<T> result = operation.initialize(usePrev, beforeClockValue);
                if (result == null) {
                    return false;
                }

                resultTable.setValue(result.resultNode);
                if (swapListener != null) {
                    swapListener.setListenerAndResult(Require.neqNull(result.resultListener, "resultListener"),
                            result.resultNode);
                    result.resultNode.addParentReference(swapListener);
                }

                return true;
            });

            return resultTable.getValue();
        });
    }

    private class WhereListener extends MergedListener {
        private final FilteredTable result;
        private final WritableRowSet currentMapping;
        private final WhereFilter[] filters;
        private final ModifiedColumnSet filterColumns;
        private final ListenerRecorder recorder;

        private WhereListener(ListenerRecorder recorder, Collection<NotificationQueue.Dependency> dependencies,
                FilteredTable result) {
            super(Collections.singleton(recorder), dependencies, "where(" + Arrays.toString(result.filters) + ")",
                    result);
            this.recorder = recorder;
            this.result = result;
            this.currentMapping = result.getRowSet().writableCast();
            this.filters = result.filters;

            boolean hasColumnArray = false;
            final Set<String> filterColumnNames = new TreeSet<>();
            for (WhereFilter filter : filters) {
                hasColumnArray |= !filter.getColumnArrays().isEmpty();
                filterColumnNames.addAll(filter.getColumns());
            }
            this.filterColumns = hasColumnArray ? null
                    : newModifiedColumnSet(filterColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        }

        @Override
        public void process() {
            ModifiedColumnSet sourceModColumns = recorder.getModifiedColumnSet();
            if (sourceModColumns == null) {
                sourceModColumns = result.getModifiedColumnSetForUpdates();
                sourceModColumns.clear();
            }

            if (result.refilterRequested()) {
                result.doRefilter(recorder.getAdded(), recorder.getRemoved(), recorder.getModified(),
                        recorder.getShifted(),
                        sourceModColumns);
                return;
            }

            // intersect removed with pre-shift keyspace
            final WritableRowSet removed = recorder.getRemoved().intersect(currentMapping);
            currentMapping.remove(removed);

            // shift keyspace
            recorder.getShifted().apply(currentMapping);

            // compute added against filters
            final WritableRowSet added =
                    filterRows(recorder.getAdded().copy().toTracking(), rowSet, false, filters);

            // check which modified keys match filters (Note: filterColumns will be null if we must always check)
            final boolean runFilters = filterColumns == null || sourceModColumns.containsAny(filterColumns);
            final RowSet matchingModifies = !runFilters ? RowSetFactory.empty()
                    : filterRows(recorder.getModified().copy().toTracking(), rowSet, false, filters);

            // which propagate as mods?
            final WritableRowSet modified =
                    (runFilters ? matchingModifies : recorder.getModified()).intersect(currentMapping);

            // remaining matchingModifies are adds
            added.insert(matchingModifies.minus(modified));

            final WritableRowSet modsToRemove;
            if (!runFilters) {
                modsToRemove = RowSetFactory.empty();
            } else {
                modsToRemove = recorder.getModified().minus(matchingModifies);
                modsToRemove.retain(currentMapping);
            }
            // note modsToRemove is currently in post-shift keyspace
            currentMapping.update(added, modsToRemove);

            // move modsToRemove into pre-shift keyspace and add to myRemoved
            recorder.getShifted().unapply(modsToRemove);
            removed.insert(modsToRemove);

            ModifiedColumnSet modifiedColumnSet = sourceModColumns;
            if (modified.isEmpty()) {
                modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                modifiedColumnSet.clear();
            }

            // note shifts are pass-through since filter will never translate keyspace
            result.notifyListeners(
                    new TableUpdateImpl(added, removed, modified, recorder.getShifted(), modifiedColumnSet));
        }
    }

    private static class StaticWhereListener extends MergedListener {
        private final FilteredTable result;

        private StaticWhereListener(Collection<NotificationQueue.Dependency> dependencies, FilteredTable result) {
            super(Collections.emptyList(), dependencies, "where(" + Arrays.toString(result.filters) + ")", result);
            this.result = result;
        }

        @Override
        public void process() {
            if (result.refilterRequested()) {
                result.doRefilter(null, null, null, null, ModifiedColumnSet.ALL);
            }
        }
    }

    private void checkInitiateOperation() {
        if (isRefreshing()) {
            UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        }
    }

    static void checkInitiateOperation(Table other) {
        if (other.isRefreshing()) {
            UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        }
    }

    @Override
    public <R> R apply(Function<Table, R> function) {
        if (function instanceof MemoizedOperationKey.Provider) {
            return memoizeResult(((MemoizedOperationKey.Provider) function).getMemoKey(), () -> super.apply(function));
        }

        return super.apply(function);
    }

    public Table wouldMatch(WouldMatchPair... matchers) {
        return getResult(new WouldMatchOperation(this, matchers));
    }

}
