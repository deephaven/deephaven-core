/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.Function;
import io.deephaven.base.StringUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.exceptions.QueryCancellationException;
import io.deephaven.db.tables.*;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.WouldMatchPair;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.util.IterableUtils;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.util.liveness.Liveness;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.v2.MemoizedOperationKey.SelectUpdateViewOrUpdateView.Flavor;
import io.deephaven.db.v2.by.*;
import io.deephaven.db.v2.locations.GroupingProvider;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.select.*;
import io.deephaven.db.v2.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.db.v2.snapshot.SnapshotIncrementalListener;
import io.deephaven.db.v2.snapshot.SnapshotInternalListener;
import io.deephaven.db.v2.snapshot.SnapshotUtils;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.sparse.SparseConstants;
import io.deephaven.db.v2.utils.*;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.internal.log.LoggerFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.db.tables.select.MatchPair.matchString;

/**
 * Primary coalesced table implementation.
 */
public class QueryTable extends BaseTable {

    public interface Operation<T extends DynamicNode & NotificationStepReceiver> {
        /**
         * The resulting table and listener of the operation.
         */
        class Result<T extends DynamicNode & NotificationStepReceiver> {
            public final T resultNode;
            public final ShiftAwareListener resultListener; // may be null if parent is non-ticking

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
                    final @Nullable ShiftAwareListener resultListener) {
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

        default ShiftAwareSwapListener newSwapListener(final QueryTable queryTable) {
            return new ShiftAwareSwapListener(queryTable);
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

    private static final Pattern COLUMN_NAME = Pattern.compile("[a-zA-Z_$][a-zA-Z0-9_$]*");

    private final Index index;
    private final LinkedHashMap<String, ColumnSource<?>> columns;
    protected transient ModifiedColumnSet modifiedColumnSet;

    // Cached data columns
    private transient Map<String, IndexedDataColumn> indexedDataColumns;

    // Flattened table support
    private transient boolean flat;

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
     * may be spread out across many blocks depending on the input Index). However, the select() can become slower
     * because it must look things up in a redirection index.
     *
     * Values less than zero disable overhead checking, and result in never flattening the input.
     *
     * A value of zero results in always flattening the input.
     */
    private static final double MAXIMUM_STATIC_SELECT_MEMORY_OVERHEAD =
            Configuration.getInstance().getDoubleWithDefault("QueryTable.maximumStaticSelectMemoryOverhead", 1.1);

    // Should we track the entire index of firstBy and lastBy operations.
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

    // Cached results
    transient Map<MemoizedOperationKey, MemoizedResult<?>> cachedOperations;

    public QueryTable(Index index, Map<String, ? extends ColumnSource<?>> columns) {
        this(TableDefinition.inferFrom(columns), index, columns);
    }

    /**
     * Creates a new abstract table, reusing a definition but creating a new column source map.
     *
     * @param definition the definition to use for this table
     * @param index the index of the new table
     * @param columns the column source map for the table, which will be copied into a new column source map
     */
    public QueryTable(TableDefinition definition, Index index, Map<String, ? extends ColumnSource<?>> columns) {
        this(definition, Require.neqNull(index, "index"), new LinkedHashMap<>(columns), null);
    }

    /**
     * Creates a new abstract table, reusing a definition and column source map.
     *
     * @param definition the definition to use for this table
     * @param index the index of the new table
     * @param columns the column source map for the table, which is not copied.
     * @param modifiedColumnSet optional {@link ModifiedColumnSet} that should be re-used if supplied
     */
    private QueryTable(TableDefinition definition, Index index, LinkedHashMap<String, ColumnSource<?>> columns,
            @Nullable ModifiedColumnSet modifiedColumnSet) {
        super(definition, "QueryTable"); // TODO: Better descriptions composed from query chain
        this.index = index;
        this.columns = columns;
        this.modifiedColumnSet = modifiedColumnSet;
        initializeTransientFields();

        TableDefinition inferred = TableDefinition.inferFrom(columns);
        definition.checkMutualCompatibility(inferred);
    }

    /**
     * Create a new query table with the {@link ColumnDefinition ColumnDefinitions} of {@code template}, but in the
     * order of {@code this}. The tables must be mutually compatible, as defined via
     * {@link TableDefinition#checkCompatibility(TableDefinition)}.
     *
     * @param template the new definition template to use
     * @return the new query table
     * @deprecated this is being used a workaround for testing purposes where previously mutations were being used at
     *             the {@link ColumnDefinition} level. Do not use this method without good reason.
     */
    @Deprecated
    public QueryTable withDefinitionUnsafe(TableDefinition template) {
        TableDefinition inOrder = template.checkMutualCompatibility(definition);
        return (QueryTable) copy(inOrder, true);
    }

    private void initializeTransientFields() {
        indexedDataColumns = new HashMap<>();
        cachedOperations = new ConcurrentHashMap<>();
        flat = false;
        if (modifiedColumnSet == null) {
            modifiedColumnSet = new ModifiedColumnSet(columns);
        }
    }

    private void readObject(ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException {
        objectInputStream.defaultReadObject();
        initializeTransientFields();
    }

    @Override
    public Index getIndex() {
        return index;
    }

    @Override
    public long size() {
        return index.size();
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
    public DataColumn getColumn(String columnName) {
        IndexedDataColumn<?> result;
        if ((result = indexedDataColumns.get(columnName)) == null) {
            indexedDataColumns.put(columnName, result = new IndexedDataColumn<>(columnName, this));
        }
        return result;
    }

    @Override
    public ModifiedColumnSet newModifiedColumnSet(final String... columnNames) {
        if (columnNames.length == 0) {
            return ModifiedColumnSet.EMPTY;
        }
        final ModifiedColumnSet newSet = new ModifiedColumnSet(modifiedColumnSet);
        newSet.setAll(columnNames);
        return newSet;
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
        return modifiedColumnSet;
    }

    @Override
    public ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(final String[] columnNames,
            final ModifiedColumnSet[] columnSets) {
        return modifiedColumnSet.newTransformer(columnNames, columnSets);
    }

    @Override
    public ModifiedColumnSet.Transformer newModifiedColumnSetIdentityTransformer(
            final Map<String, ColumnSource<?>> newColumns) {
        return modifiedColumnSet.newIdentityTransformer(newColumns);
    }

    @Override
    public ModifiedColumnSet.Transformer newModifiedColumnSetIdentityTransformer(final DynamicTable other) {
        if (other instanceof QueryTable) {
            return modifiedColumnSet.newIdentityTransformer(((QueryTable) other).columns);
        }
        return super.newModifiedColumnSetIdentityTransformer(other);
    }

    @Override
    public Object[] getRecord(long rowNo, String... columnNames) {
        final long key = index.get(rowNo);
        return (columnNames.length > 0 ? Arrays.stream(columnNames).map(this::getColumnSource)
                : columns.values().stream()).map(cs -> cs.get(key)).toArray(Object[]::new);
    }

    @Override
    public LocalTableMap byExternal(final boolean dropKeys, final String... keyColumnNames) {
        if (isStream()) {
            throw streamUnsupported("byExternal");
        }
        final SelectColumn[] groupByColumns =
                Arrays.stream(keyColumnNames).map(SourceColumn::new).toArray(SelectColumn[]::new);

        return memoizeResult(MemoizedOperationKey.byExternal(dropKeys, groupByColumns),
                () -> QueryPerformanceRecorder.withNugget(
                        "byExternal(" + dropKeys + ", " + Arrays.toString(keyColumnNames) + ')',
                        sizeForInstrumentation(),
                        () -> ByExternalAggregationFactory.byExternal(this, dropKeys,
                                (pt, st) -> pt.copyAttributes(st, CopyAttributeOperation.ByExternal),
                                Collections.emptyList(), groupByColumns)));
    }

    @Override
    public Table rollup(ComboAggregateFactory comboAggregateFactory, boolean includeConstituents,
            SelectColumn... columns) {
        if (isStream() && includeConstituents) {
            throw streamUnsupported("rollup with included constituents");
        }
        return memoizeResult(MemoizedOperationKey.rollup(comboAggregateFactory, columns, includeConstituents), () -> {
            final ComboAggregateFactory withRollup = comboAggregateFactory.forRollup(includeConstituents);
            ComboAggregateFactory aggregationStateFactory = withRollup;

            final QueryTable lowestLevel = byNoMemo(withRollup, columns);
            // now we need to reaggregate at each of the levels, combining the results
            final List<SelectColumn> reaggregateColumns = new ArrayList<>(Arrays.asList(columns));

            final ComboAggregateFactory rollupFactory = withRollup.rollupFactory();

            final List<String> nullColumns = new ArrayList<>(reaggregateColumns.size());

            QueryTable lastLevel = lowestLevel;
            while (!reaggregateColumns.isEmpty()) {
                final SelectColumn removedColumn = reaggregateColumns.remove(reaggregateColumns.size() - 1);

                nullColumns.add(0, removedColumn.getName());

                final Map<String, Class<?>> nullColumnsMap = new LinkedHashMap<>(nullColumns.size());
                final Table fLastLevel = lastLevel;
                nullColumns
                        .forEach(nc -> nullColumnsMap.put(nc, fLastLevel.getDefinition().getColumn(nc).getDataType()));

                aggregationStateFactory = rollupFactory.withNulls(nullColumnsMap);
                lastLevel = lastLevel.byNoMemo(aggregationStateFactory,
                        reaggregateColumns.toArray(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY));
            }

            final String[] rollupsToDrop = lastLevel.getColumnSourceMap().keySet().stream()
                    .filter(cn -> cn.endsWith(ComboAggregateFactory.ROLLUP_COLUMN_SUFFIX)).toArray(String[]::new);
            final QueryTable finalTable = (QueryTable) lastLevel.dropColumns(rollupsToDrop);
            final Object reverseLookup =
                    Require.neqNull(lastLevel.getAttribute(REVERSE_LOOKUP_ATTRIBUTE), "REVERSE_LOOKUP_ATTRIBUTE");
            finalTable.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);

            final Table result = HierarchicalTable.createFrom(finalTable, new RollupInfo(comboAggregateFactory, columns,
                    includeConstituents ? RollupInfo.LeafType.Constituent : RollupInfo.LeafType.Normal));
            result.setAttribute(Table.HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE, QueryTable.this);
            copyAttributes(result, CopyAttributeOperation.Rollup);
            maybeUpdateSortableColumns(result);

            return result;
        });
    }

    @Override
    public Table treeTable(String idColumn, String parentColumn) {
        if (isStream()) {
            throw streamUnsupported("treeTable");
        }
        return memoizeResult(MemoizedOperationKey.treeTable(idColumn, parentColumn), () -> {
            final LocalTableMap byExternalResult = ByExternalAggregationFactory.byExternal(this, false,
                    (pt, st) -> pt.copyAttributes(st, CopyAttributeOperation.ByExternal),
                    Collections.singletonList(null), parentColumn);
            final QueryTable rootTable = (QueryTable) byExternalResult.get(null);
            final Table result = HierarchicalTable.createFrom((QueryTable) rootTable.copy(),
                    new TreeTableInfo(idColumn, parentColumn));

            // If the parent table has an RLL attached to it, we can re-use it.
            final ReverseLookup reverseLookup;
            if (hasAttribute(PREPARED_RLL_ATTRIBUTE)) {
                reverseLookup = (ReverseLookup) getAttribute(PREPARED_RLL_ATTRIBUTE);
                final String[] listenerCols = reverseLookup.getKeyColumns();

                if (listenerCols.length != 1 || !listenerCols[0].equals(idColumn)) {
                    final String listenerColError =
                            StringUtils.joinStrings(Arrays.stream(listenerCols).map(col -> "'" + col + "'"), ", ");
                    throw new IllegalStateException(
                            "Table was prepared for Tree table with a different Id column. Expected `" + idColumn
                                    + "`, Actual " + listenerColError);
                }
            } else {
                reverseLookup = ReverseLookupListener.makeReverseLookupListenerWithSnapshot(QueryTable.this, idColumn);
            }

            result.setAttribute(HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, byExternalResult);
            result.setAttribute(HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE, QueryTable.this);
            result.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);
            copyAttributes(result, CopyAttributeOperation.Treetable);
            maybeUpdateSortableColumns(result);

            return result;
        });
    }

    @Override
    public Table slice(final long firstPositionInclusive, final long lastPositionExclusive) {
        if (firstPositionInclusive == lastPositionExclusive) {
            return getSubTable(Index.FACTORY.getEmptyIndex());
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
    public Table leftJoin(Table table, final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd) {
        return QueryPerformanceRecorder.withNugget(
                "leftJoin(" + table + "," + matchString(columnsToMatch) + "," + matchString(columnsToAdd) + ")",
                sizeForInstrumentation(), () -> {
                    String[] rightColumnKeys = new String[columnsToMatch.length];
                    for (int i = 0; i < columnsToMatch.length; i++) {
                        MatchPair match = columnsToMatch[i];
                        rightColumnKeys[i] = match.rightColumn;
                    }

                    Table groupedRight = table.by(rightColumnKeys);
                    return naturalJoin(groupedRight, columnsToMatch, columnsToAdd);
                });
    }

    @Override
    public Table exactJoin(Table table, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return QueryPerformanceRecorder.withNugget(
                "exactJoin(" + table + "," + Arrays.toString(columnsToMatch) + "," + Arrays.toString(columnsToMatch)
                        + ")",
                sizeForInstrumentation(),
                () -> naturalJoinInternal(table, columnsToMatch, columnsToAdd, true));
    }

    @Override
    public Table lastBy(SelectColumn... selectColumns) {
        return QueryPerformanceRecorder.withNugget("lastBy(" + Arrays.toString(selectColumns) + ")",
                sizeForInstrumentation(),
                () -> {
                    final Table result =
                            by(TRACKED_LAST_BY ? new TrackingLastByStateFactoryImpl() : new LastByStateFactoryImpl(),
                                    selectColumns);
                    copyAttributes(result, CopyAttributeOperation.LastBy);
                    return result;
                });
    }

    @Override
    public Table firstBy(SelectColumn... selectColumns) {
        return QueryPerformanceRecorder.withNugget("firstBy(" + Arrays.toString(selectColumns) + ")",
                sizeForInstrumentation(),
                () -> {
                    final Table result =
                            by(TRACKED_FIRST_BY ? new TrackingFirstByStateFactoryImpl() : new FirstByStateFactoryImpl(),
                                    selectColumns);
                    copyAttributes(result, CopyAttributeOperation.FirstBy);
                    return result;
                });
    }

    @Override
    public Table minBy(SelectColumn... selectColumns) {
        return QueryPerformanceRecorder.withNugget("minBy(" + Arrays.toString(selectColumns) + ")",
                sizeForInstrumentation(), () -> {
                    if (isRefreshing()) {
                        return by(new MinMaxByStateFactoryImpl(true), selectColumns);
                    } else {
                        return by(new AddOnlyMinMaxByStateFactoryImpl(true), selectColumns);
                    }
                });
    }

    @Override
    public Table maxBy(SelectColumn... selectColumns) {
        return QueryPerformanceRecorder.withNugget("maxBy(" + Arrays.toString(selectColumns) + ")",
                sizeForInstrumentation(), () -> {
                    if (isRefreshing()) {
                        return by(new MinMaxByStateFactoryImpl(false), selectColumns);
                    } else {
                        return by(new AddOnlyMinMaxByStateFactoryImpl(false), selectColumns);
                    }
                });
    }

    @Override
    public Table medianBy(SelectColumn... selectColumns) {
        return QueryPerformanceRecorder.withNugget("medianBy(" + Arrays.toString(selectColumns) + ")",
                sizeForInstrumentation(),
                () -> by(new PercentileByStateFactoryImpl(0.50, true), selectColumns));
    }

    @Override
    public Table countBy(String countColumnName, SelectColumn... groupByColumns) {
        return QueryPerformanceRecorder.withNugget(
                "countBy(" + countColumnName + "," + Arrays.toString(groupByColumns) + ")", sizeForInstrumentation(),
                () -> {
                    if (!COLUMN_NAME.matcher(countColumnName).matches()) { // TODO: Test more columns this way
                        throw new RuntimeException(countColumnName + " is not a valid column name");
                    }
                    return by(new CountByStateFactoryImpl(countColumnName), groupByColumns);
                });
    }

    public Table by(final AggregationStateFactory inputAggregationStateFactory, final SelectColumn... groupByColumns) {
        return memoizeResult(MemoizedOperationKey.by(inputAggregationStateFactory, groupByColumns),
                () -> byNoMemo(inputAggregationStateFactory, groupByColumns));
    }

    private QueryTable byNoMemo(AggregationStateFactory inputAggregationStateFactory,
            final SelectColumn... groupByColumns) {
        final String description = "by(" + inputAggregationStateFactory + ", " + Arrays.toString(groupByColumns) + ")";

        return QueryPerformanceRecorder.withNugget(description, sizeForInstrumentation(), () -> {

            final boolean isBy = inputAggregationStateFactory.getClass() == AggregationIndexStateFactory.class;
            final boolean isApplyToAllBy =
                    inputAggregationStateFactory.getClass() == AggregationFormulaStateFactory.class;
            final boolean isNumeric = inputAggregationStateFactory.getClass() == SumStateFactory.class ||
                    inputAggregationStateFactory.getClass() == AbsSumStateFactory.class ||
                    inputAggregationStateFactory.getClass() == AvgStateFactory.class ||
                    inputAggregationStateFactory.getClass() == VarStateFactory.class ||
                    inputAggregationStateFactory.getClass() == StdStateFactory.class;
            final boolean isSelectDistinct =
                    inputAggregationStateFactory.getClass() == SelectDistinctStateFactoryImpl.class;
            final boolean isCount = inputAggregationStateFactory.getClass() == CountByStateFactoryImpl.class;
            final boolean isMinMax = inputAggregationStateFactory instanceof MinMaxByStateFactoryImpl;
            final boolean isPercentile = inputAggregationStateFactory.getClass() == PercentileByStateFactoryImpl.class;
            final boolean isWeightedAvg =
                    inputAggregationStateFactory.getClass() == WeightedAverageStateFactoryImpl.class;
            final boolean isWeightedSum = inputAggregationStateFactory.getClass() == WeightedSumStateFactoryImpl.class;
            final boolean isSortedFirstOrLast = inputAggregationStateFactory instanceof SortedFirstOrLastByFactoryImpl;
            final boolean isFirst = inputAggregationStateFactory.getClass() == FirstByStateFactoryImpl.class
                    || inputAggregationStateFactory.getClass() == TrackingFirstByStateFactoryImpl.class;
            final boolean isLast = inputAggregationStateFactory.getClass() == LastByStateFactoryImpl.class
                    || inputAggregationStateFactory.getClass() == TrackingLastByStateFactoryImpl.class;
            final boolean isCombo = inputAggregationStateFactory instanceof ComboAggregateFactory;

            if (isBy) {
                if (isStream()) {
                    throw streamUnsupported("by");
                }
                if (USE_OLDER_CHUNKED_BY) {
                    return AggregationHelper.by(this, groupByColumns);
                }
                return ByAggregationFactory.by(this, groupByColumns);
            } else if (isApplyToAllBy) {
                if (isStream()) {
                    throw streamUnsupported("applyToAllBy");
                }
                final String formula = ((AggregationFormulaStateFactory) inputAggregationStateFactory).getFormula();
                final String columnParamName =
                        ((AggregationFormulaStateFactory) inputAggregationStateFactory).getColumnParamName();
                return FormulaAggregationFactory.applyToAllBy(this, formula, columnParamName, groupByColumns);
            } else if (isNumeric) {
                return ChunkedOperatorAggregationHelper.aggregation(new NonKeyColumnAggregationFactory(
                        (IterativeChunkedOperatorFactory) inputAggregationStateFactory), this, groupByColumns);
            } else if (isSortedFirstOrLast) {
                final boolean isSortedFirst =
                        ((SortedFirstOrLastByFactoryImpl) inputAggregationStateFactory).isSortedFirst();
                return ChunkedOperatorAggregationHelper.aggregation(
                        new SortedFirstOrLastByAggregationFactory(isSortedFirst, false,
                                ((SortedFirstOrLastByFactoryImpl) inputAggregationStateFactory).getSortColumnNames()),
                        this, groupByColumns);
            } else if (isFirst || isLast) {
                return ChunkedOperatorAggregationHelper.aggregation(new FirstOrLastByAggregationFactory(isFirst), this,
                        groupByColumns);
            } else if (isMinMax) {
                final boolean isMin = ((MinMaxByStateFactoryImpl) inputAggregationStateFactory).isMinimum();
                return ChunkedOperatorAggregationHelper.aggregation(
                        new NonKeyColumnAggregationFactory(
                                new MinMaxIterativeOperatorFactory(isMin, isStream() || isAddOnly())),
                        this, groupByColumns);
            } else if (isPercentile) {
                final double percentile = ((PercentileByStateFactoryImpl) inputAggregationStateFactory).getPercentile();
                final boolean averageMedian =
                        ((PercentileByStateFactoryImpl) inputAggregationStateFactory).getAverageMedian();
                return ChunkedOperatorAggregationHelper.aggregation(
                        new NonKeyColumnAggregationFactory(
                                new PercentileIterativeOperatorFactory(percentile, averageMedian)),
                        this, groupByColumns);
            } else if (isWeightedAvg || isWeightedSum) {
                final String weightName;
                if (isWeightedAvg) {
                    weightName = ((WeightedAverageStateFactoryImpl) inputAggregationStateFactory).getWeightName();
                } else {
                    weightName = ((WeightedSumStateFactoryImpl) inputAggregationStateFactory).getWeightName();
                }
                return ChunkedOperatorAggregationHelper.aggregation(
                        new WeightedAverageSumAggregationFactory(weightName, isWeightedSum), this, groupByColumns);
            } else if (isCount) {
                return ChunkedOperatorAggregationHelper.aggregation(
                        new CountAggregationFactory(
                                ((CountByStateFactoryImpl) inputAggregationStateFactory).getCountName()),
                        this, groupByColumns);
            } else if (isSelectDistinct) {
                if (getColumnSourceMap().isEmpty()) {
                    // if we have no input columns, then the only thing we can do is have an empty result
                    return new QueryTable(Index.FACTORY.getEmptyIndex(), Collections.emptyMap());
                }
                return ChunkedOperatorAggregationHelper.aggregation(new KeyOnlyAggregationFactory(), this,
                        groupByColumns);
            } else if (isCombo) {
                return ChunkedOperatorAggregationHelper.aggregation(
                        ((ComboAggregateFactory) inputAggregationStateFactory).makeAggregationContextFactory(), this,
                        groupByColumns);
            }

            throw new RuntimeException("Unknown aggregation factory: " + inputAggregationStateFactory);
        });
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
                            // : colName.size() > nRows ? colName.subArray(0, nRows)
                            // : colName
                            colName + '=' + "isNull(" + colName + ") ? null" +
                                    ':' + colName + ".size() > " + nRows + " ? " + colName + ".subArray(0, " + nRows
                                    + ')' +
                                    ':' + colName;
                else
                    updates[j++] =
                            // Get the last nRows rows:
                            // colName = isNull(colName) ? null
                            // : colName.size() > nRows ? colName.subArray(colName.size() - nRows, colName.size())
                            // : colName
                            colName + '=' + "isNull(" + colName + ") ? null" +
                                    ':' + colName + ".size() > " + nRows + " ? " + colName + ".subArray(" + colName
                                    + ".size() - " + nRows + ", " + colName + ".size())" +
                                    ':' + colName;
            }
        }

        return by(groupByColumns).updateView(updates).ungroup().updateView(casting);
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
    public Table applyToAllBy(String formulaColumn, String columnParamName, SelectColumn... groupByColumns) {
        final String[] formattingColumnNames = definition.getColumnNames().stream()
                .filter(name -> name.endsWith("__WTABLE_FORMAT")).toArray(String[]::new);
        final QueryTable noFormattingColumnsTable = (QueryTable) dropColumns(formattingColumnNames);
        return QueryPerformanceRecorder.withNugget(
                "applyToAllBy(" + formulaColumn + ',' + columnParamName + ',' + Arrays.toString(groupByColumns) + ")",
                sizeForInstrumentation(),
                () -> noFormattingColumnsTable.by(new AggregationFormulaStateFactory(formulaColumn, columnParamName),
                        groupByColumns));
    }

    @Override
    public Table sumBy(SelectColumn... groupByColumns) {
        return QueryPerformanceRecorder.withNugget("sumBy(" + Arrays.toString(groupByColumns) + ")",
                sizeForInstrumentation(),
                () -> by(new SumStateFactory(), groupByColumns));
    }

    @Override
    public Table absSumBy(SelectColumn... groupByColumns) {
        return QueryPerformanceRecorder.withNugget("absSumBy(" + Arrays.toString(groupByColumns) + ")",
                sizeForInstrumentation(),
                () -> by(new AbsSumStateFactory(), groupByColumns));
    }

    @Override
    public Table avgBy(SelectColumn... groupByColumns) {
        return QueryPerformanceRecorder.withNugget("avgBy(" + Arrays.toString(groupByColumns) + ")",
                sizeForInstrumentation(),
                () -> by(new AvgStateFactory(), groupByColumns));
    }

    @Override
    public Table wavgBy(String weightColumn, SelectColumn... groupByColumns) {
        return QueryPerformanceRecorder.withNugget(
                "wavgBy(" + weightColumn + ", " + Arrays.toString(groupByColumns) + ")", sizeForInstrumentation(),
                () -> by(new WeightedAverageStateFactoryImpl(weightColumn), groupByColumns));
    }

    @Override
    public Table wsumBy(String weightColumn, SelectColumn... groupByColumns) {
        return QueryPerformanceRecorder.withNugget(
                "wsumBy(" + weightColumn + ", " + Arrays.toString(groupByColumns) + ")", sizeForInstrumentation(),
                () -> by(new WeightedSumStateFactoryImpl(weightColumn), groupByColumns));
    }

    @Override
    public Table stdBy(SelectColumn... groupByColumns) {
        return QueryPerformanceRecorder.withNugget("stdBy(" + Arrays.toString(groupByColumns) + ")",
                sizeForInstrumentation(),
                () -> by(new StdStateFactory(), groupByColumns));
    }

    @Override
    public Table varBy(SelectColumn... groupByColumns) {
        return QueryPerformanceRecorder.withNugget("varBy(" + Arrays.toString(groupByColumns) + ")",
                sizeForInstrumentation(),
                () -> by(new VarStateFactory(), groupByColumns));
    }

    public static class FilteredTable extends QueryTable implements SelectFilter.RecomputeListener {
        private final QueryTable source;
        private final SelectFilter[] filters;
        private boolean refilterMatchedRequested = false;
        private boolean refilterUnmatchedRequested = false;
        private MergedListener whereListener;

        public FilteredTable(final Index currentMapping, final QueryTable source, final SelectFilter[] filters) {
            super(source.getDefinition(), currentMapping, source.columns, null);
            this.source = source;
            this.filters = filters;
            for (final SelectFilter f : filters) {
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
         * @param upstreamAdded index of keys that were added upstream
         * @param upstreamRemoved index of keys that were removed
         * @param upstreamModified index of keys that were modified upstream
         * @param shiftData sequence of shifts that apply to keyspace
         * @param modifiedColumnSet the set of columns that have any changes to indices in {@code modified}
         */
        private void doRefilter(final Index upstreamAdded, final Index upstreamRemoved, final Index upstreamModified,
                final IndexShiftData shiftData, final ModifiedColumnSet modifiedColumnSet) {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.modifiedColumnSet = modifiedColumnSet;

            // Remove upstream keys first, so that keys at rows that were removed and then added are propagated as such.
            // Note that it is a failure to propagate these as modifies, since modifiedColumnSet may not mark that all
            // columns have changed.
            update.removed = upstreamRemoved == null ? Index.FACTORY.getEmptyIndex()
                    : upstreamRemoved.intersect(getIndex());
            getIndex().remove(update.removed);

            // Update our index and compute removals due to splatting.
            if (shiftData != null) {
                final Index index = getIndex();
                shiftData.apply((beginRange, endRange, shiftDelta) -> {
                    final Index toShift = index.subindexByKey(beginRange, endRange);
                    index.removeRange(beginRange, endRange);
                    update.removed.insert(index.subindexByKey(beginRange + shiftDelta, endRange + shiftDelta));
                    index.removeRange(beginRange + shiftDelta, endRange + shiftDelta);
                    index.insert(toShift.shift(shiftDelta));
                });
            }

            final Index newMapping;
            if (refilterMatchedRequested && refilterUnmatchedRequested) {
                newMapping = whereInternal(source.getIndex().clone(), source.getIndex(), false, filters);
                refilterMatchedRequested = refilterUnmatchedRequested = false;
            } else if (refilterUnmatchedRequested) {
                // things that are added or removed are already reflected in source.getIndex
                final Index unmatchedRows = source.getIndex().minus(getIndex());
                // we must check rows that have been modified instead of just preserving them
                if (upstreamModified != null) {
                    unmatchedRows.insert(upstreamModified);
                }
                final Index unmatchedClone = unmatchedRows.clone();
                newMapping = whereInternal(unmatchedClone, unmatchedRows, false, filters);

                // add back what we previously matched, but for modifications and removals
                try (final Index previouslyMatched = getIndex().clone()) {
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
                // we need to take removed rows out of our index so we do not read them; and also examine added or
                // modified rows
                final Index matchedRows = getIndex().clone();
                if (upstreamAdded != null) {
                    matchedRows.insert(upstreamAdded);
                }
                if (upstreamModified != null) {
                    matchedRows.insert(upstreamModified);
                }
                final Index matchedClone = matchedRows.clone();
                newMapping = whereInternal(matchedClone, matchedRows, false, filters);
                refilterMatchedRequested = false;
            } else {
                throw new IllegalStateException("Refilter called when a refilter was not requested!");
            }

            // Compute added/removed in post-shift keyspace.
            update.added = newMapping.minus(getIndex());
            final Index postShiftRemovals = getIndex().minus(newMapping);

            // Update our index in post-shift keyspace.
            getIndex().update(update.added, postShiftRemovals);

            // Note that removed must be propagated to listeners in pre-shift keyspace.
            if (shiftData != null) {
                shiftData.unapply(postShiftRemovals);
            }
            update.removed.insert(postShiftRemovals);

            if (upstreamModified == null || upstreamModified.empty()) {
                update.modified = Index.FACTORY.getEmptyIndex();
            } else {
                update.modified = upstreamModified.intersect(newMapping);
                update.modified.remove(update.added);
            }

            update.shifted = shiftData == null ? IndexShiftData.EMPTY : shiftData;

            notifyListeners(update);
        }

        private void setWhereListener(MergedListener whereListener) {
            this.whereListener = whereListener;
        }
    }

    @Override
    public Table where(final SelectFilter... filters) {
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
                        Table result = this;
                        if (!first) {
                            result = result.where(Arrays.copyOf(filters, fi));
                        }
                        if (reindexingFilter.requiresSorting()) {
                            result = result.sort(reindexingFilter.getSortColumns());
                            reindexingFilter.sortingDone();
                        }
                        result = result.where(reindexingFilter);
                        if (!last) {
                            result = result.where(Arrays.copyOfRange(filters, fi + 1, filters.length));
                        }
                        return result;
                    }

                    for (SelectFilter filter : filters) {
                        filter.init(getDefinition());
                    }

                    return memoizeResult(MemoizedOperationKey.filter(filters), () -> {
                        final ShiftAwareSwapListener swapListener =
                                createSwapListenerIfRefreshing(ShiftAwareSwapListener::new);

                        final Mutable<QueryTable> result = new MutableObject<>();
                        initializeWithSnapshot("where", swapListener,
                                (prevRequested, beforeClock) -> {
                                    final boolean usePrev = prevRequested && isRefreshing();
                                    final Index indexToUse = usePrev ? index.getPrevIndex() : index;
                                    final Index currentMapping;

                                    currentMapping = whereInternal(indexToUse.clone(), indexToUse, usePrev, filters);
                                    Assert.eq(currentMapping.getPrevIndex().size(),
                                            "currentMapping.getPrevIndex.size()", currentMapping.size(),
                                            "currentMapping.size()");

                                    final FilteredTable filteredTable =
                                            new FilteredTable(currentMapping, this, filters);

                                    for (final SelectFilter filter : filters) {
                                        filter.setRecomputeListener(filteredTable);
                                    }

                                    final boolean refreshingFilters =
                                            Arrays.stream(filters).anyMatch(SelectFilter::isRefreshing);
                                    copyAttributes(filteredTable, CopyAttributeOperation.Filter);
                                    if (!refreshingFilters && isAddOnly()) {
                                        filteredTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
                                    }

                                    final List<NotificationQueue.Dependency> dependencies = Stream.concat(
                                            Stream.of(filters).filter(f -> f instanceof NotificationQueue.Dependency)
                                                    .map(f -> (NotificationQueue.Dependency) f),
                                            Stream.of(filters).filter(f -> f instanceof DependencyStreamProvider)
                                                    .flatMap(f -> ((DependencyStreamProvider) f).getDependencyStream()))
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
    protected Index whereInternal(Index currentMapping, Index fullSet, boolean usePrev, SelectFilter... filters) {
        for (SelectFilter filter : filters) {
            if (Thread.interrupted()) {
                throw new QueryCancellationException("interrupted while filtering");
            }
            currentMapping = filter.filter(currentMapping, fullSet, this, usePrev);
        }
        return currentMapping;
    }

    @Override
    public Table whereIn(final GroupStrategy groupStrategy, final Table rightTable, final boolean inclusion,
            final MatchPair... columnsToMatch) {
        return QueryPerformanceRecorder.withNugget(
                "whereIn(" + groupStrategy + " , rightTable, " + inclusion + ", " + matchString(columnsToMatch) + ")",
                sizeForInstrumentation(), () -> {
                    checkInitiateOperation(rightTable);

                    final Table distinctValues = rightTable.selectDistinct(MatchPair.getRightColumns(columnsToMatch));
                    final DynamicWhereFilter dynamicWhereFilter =
                            new DynamicWhereFilter(groupStrategy, distinctValues, inclusion, columnsToMatch);
                    final Table where = where(dynamicWhereFilter);
                    if (where instanceof DynamicTable) {
                        if (distinctValues.isLive()) {
                            ((DynamicTable) where).addParentReference(distinctValues);
                        }
                        if (dynamicWhereFilter.isRefreshing()) {
                            ((DynamicTable) where).addParentReference(dynamicWhereFilter);
                        }
                    }
                    return where;
                });
    }

    @Override
    public Table flatten() {
        if (!isFlat() && !isRefreshing() && index.size() - 1 == index.lastKey()) {
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
            Assert.assertion(index.lastKey() == index.size() - 1, "index.lastKey() == index.size() - 1", index,
                    "index");
        }
        return flat;
    }

    @Override
    public void releaseCachedResources() {
        super.releaseCachedResources();
        columns.values().forEach(ColumnSource::releaseCachedResources);
    }

    @Override
    public Table select(SelectColumn... selectColumns) {
        if (!isRefreshing() && !isFlat() && exceedsMaximumStaticSelectOverhead()) {
            // if we are static, we will pass the select through a flatten call, to ensure that our result is as
            // efficient in terms of memory as possible
            return flatten().select(selectColumns);
        }
        return selectOrUpdate(Flavor.Select, selectColumns);
    }

    private boolean exceedsMaximumStaticSelectOverhead() {
        if (MAXIMUM_STATIC_SELECT_MEMORY_OVERHEAD < 0) {
            return false;
        }
        if (MAXIMUM_STATIC_SELECT_MEMORY_OVERHEAD == 0) {
            return true;
        }

        final long requiredBlocks = (size() + SparseConstants.BLOCK_SIZE - 1) / SparseConstants.BLOCK_SIZE;
        final long acceptableBlocks = (long) (MAXIMUM_STATIC_SELECT_MEMORY_OVERHEAD * (double) requiredBlocks);
        final MutableLong lastBlock = new MutableLong(-1L);
        final MutableLong usedBlocks = new MutableLong(0);
        return !getIndex().forEachLongRange((s, e) -> {
            long startBlock = s >> SparseConstants.LOG_BLOCK_SIZE;
            final long endBlock = e >> SparseConstants.LOG_BLOCK_SIZE;
            final long lb = lastBlock.longValue();
            if (lb >= 0) {
                if (startBlock == lb) {
                    if (startBlock == endBlock) {
                        return true;
                    }
                    startBlock++;
                }
            }
            lastBlock.setValue(endBlock);
            usedBlocks.add(endBlock - startBlock + 1);
            return usedBlocks.longValue() <= acceptableBlocks;
        });
    }

    @Override
    public Table update(final SelectColumn... selectColumns) {
        return selectOrUpdate(Flavor.Update, selectColumns);
    }

    /**
     * This does a certain amount of validation and can be used to get confidence that the formulas are valid. If it is
     * not valid, you will get an exception. Positive test (should pass validation): "X = 12", "Y = X + 1") Negative
     * test (should fail validation): "X = 12", "Y = Z + 1")
     */
    @Override
    public SelectValidationResult validateSelect(final SelectColumn... selectColumns) {
        final SelectColumn[] clones = Arrays.stream(selectColumns).map(SelectColumn::copy).toArray(SelectColumn[]::new);
        SelectAndViewAnalyzer analyzer = SelectAndViewAnalyzer.create(SelectAndViewAnalyzer.Mode.SELECT_STATIC, columns,
                index, modifiedColumnSet, true, clones);
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
                        if ((flavor == Flavor.Update && USE_REDIRECTED_COLUMNS_FOR_UPDATE)
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
                            SelectAndViewAnalyzer.create(mode, columns, index, modifiedColumnSet,
                                    publishTheseSources, selectColumns);

                    // Init all the rows by cooking up a fake Update
                    final Index emptyIndex = Index.FACTORY.getEmptyIndex();
                    final ShiftAwareListener.Update fakeUpdate =
                            new ShiftAwareListener.Update(index, emptyIndex, emptyIndex,
                                    IndexShiftData.EMPTY, ModifiedColumnSet.ALL);
                    try (final SelectAndViewAnalyzer.UpdateHelper updateHelper =
                            new SelectAndViewAnalyzer.UpdateHelper(emptyIndex, fakeUpdate)) {
                        analyzer.applyUpdate(fakeUpdate, emptyIndex, updateHelper);
                    }

                    final QueryTable resultTable = new QueryTable(index, analyzer.getPublishedColumnSources());
                    if (isRefreshing()) {
                        analyzer.startTrackingPrev();
                        final Map<String, String[]> effects = analyzer.calcEffects();
                        final SelectOrUpdateListener soul =
                                new SelectOrUpdateListener(updateDescription, this, resultTable,
                                        effects, analyzer);
                        listenForUpdates(soul);
                    } else {
                        propagateGrouping(selectColumns, resultTable);
                        for (final ColumnSource<?> columnSource : analyzer.getNewColumnSources().values()) {
                            ((SparseArrayColumnSource<?>) columnSource).setImmutable();
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
                final ColumnSource<?> originalColumnSource = getColumnSource(sourceColumn.getSourceName());
                final ColumnSource<?> selectedColumnSource = resultTable.getColumnSource(sourceColumn.getName());
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
                    } else if (index.hasGrouping(originalColumnSource)) {
                        index.copyImmutableGroupings(originalColumnSource, selectedColumnSource);
                    }
                }
            }
            usedOutputColumns.add(selectColumn.getName());
        }
    }

    @Override
    public Table view(final SelectColumn... viewColumns) {
        if (viewColumns == null || viewColumns.length == 0) {
            if (isRefreshing()) {
                manageWithCurrentScope();
            }
            return this;
        }
        return viewOrUpdateView(Flavor.View, viewColumns);
    }

    @Override
    public Table updateView(final SelectColumn... viewColumns) {
        return viewOrUpdateView(Flavor.UpdateView, viewColumns);
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

                            final ShiftAwareSwapListener swapListener =
                                    createSwapListenerIfRefreshing(ShiftAwareSwapListener::new);
                            initializeWithSnapshot(humanReadablePrefix, swapListener, (usePrev, beforeClockValue) -> {
                                final boolean publishTheseSources = flavor == Flavor.UpdateView;
                                final SelectAndViewAnalyzer analyzer =
                                        SelectAndViewAnalyzer.create(SelectAndViewAnalyzer.Mode.VIEW_EAGER,
                                                columns, index, modifiedColumnSet, publishTheseSources, viewColumns);
                                final QueryTable queryTable =
                                        new QueryTable(index, analyzer.getPublishedColumnSources());
                                if (swapListener != null) {
                                    final Map<String, String[]> effects = analyzer.calcEffects();
                                    final ShiftAwareListener listener =
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
    private static class ViewOrUpdateViewListener extends ShiftAwareListenerImpl {
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
        public void onUpdate(final Update upstream) {
            final Update downstream = upstream.copy();
            downstream.modifiedColumnSet = dependent.modifiedColumnSet;
            transformer.clearAndTransform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);
            dependent.notifyListeners(downstream);
        }
    }

    /**
     * A Shift-Aware listener for Select or Update. It uses the SelectAndViewAnalyzer to calculate how columns affect
     * other columns, then creates a column set transformer which will be used by onUpdate to transform updates.
     */
    private static class SelectOrUpdateListener extends ShiftAwareListenerImpl {
        private final QueryTable dependent;
        private final ModifiedColumnSet.Transformer transformer;
        private final SelectAndViewAnalyzer analyzer;

        /**
         * @param description Description of this listener
         * @param parent The parent table
         * @param dependent The dependent table
         * @param effects A map from a column name to the column names that it affects
         */
        SelectOrUpdateListener(String description, QueryTable parent, QueryTable dependent,
                Map<String, String[]> effects,
                SelectAndViewAnalyzer analyzer) {
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
            this.analyzer = analyzer;
        }

        @Override
        public void onUpdate(final Update upstream) {
            // Attempt to minimize work by sharing computation across all columns:
            // - clear only the keys that no longer exist
            // - create parallel arrays of pre-shift-keys and post-shift-keys so we can move them in chunks

            try (final Index toClear = dependent.index.getPrevIndex();
                    final SelectAndViewAnalyzer.UpdateHelper updateHelper =
                            new SelectAndViewAnalyzer.UpdateHelper(dependent.index, upstream)) {
                toClear.remove(dependent.index);
                analyzer.applyUpdate(upstream, toClear, updateHelper);

                final Update downstream = upstream.copy();
                downstream.modifiedColumnSet = dependent.modifiedColumnSet;
                transformer.clearAndTransform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);
                dependent.notifyListeners(downstream);
            }
        }
    }

    @Override
    public Table lazyUpdate(SelectColumn... selectColumns) {
        return QueryPerformanceRecorder.withNugget("lazyUpdate(" + selectColumnString(selectColumns) + ")",
                sizeForInstrumentation(), () -> {
                    checkInitiateOperation();

                    final SelectAndViewAnalyzer analyzer =
                            SelectAndViewAnalyzer.create(SelectAndViewAnalyzer.Mode.VIEW_LAZY,
                                    columns, index, modifiedColumnSet, true, selectColumns);
                    final QueryTable result = new QueryTable(index, analyzer.getPublishedColumnSources());
                    if (isRefreshing()) {
                        listenForUpdates(new ShiftAwareListenerImpl(
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

                    final ShiftAwareSwapListener swapListener =
                            createSwapListenerIfRefreshing(ShiftAwareSwapListener::new);

                    initializeWithSnapshot("dropColumns", swapListener, (usePrev, beforeClockValue) -> {
                        final QueryTable resultTable = new QueryTable(index, newColumns);
                        propagateFlatness(resultTable);

                        copyAttributes(resultTable, CopyAttributeOperation.DropColumns);
                        maybeUpdateSortableColumns(resultTable);
                        maybeCopyColumnDescriptions(resultTable);

                        if (swapListener != null) {
                            final ModifiedColumnSet.Transformer mcsTransformer =
                                    newModifiedColumnSetTransformer(resultTable,
                                            resultTable.getColumnSourceMap().keySet()
                                                    .toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
                            final ShiftAwareListenerImpl listener = new ShiftAwareListenerImpl(
                                    "dropColumns(" + Arrays.deepToString(columnNames) + ')', this, resultTable) {
                                @Override
                                public void onUpdate(final Update upstream) {
                                    final Update downstream = upstream.copy();
                                    mcsTransformer.clearAndTransform(upstream.modifiedColumnSet,
                                            resultTable.modifiedColumnSet);
                                    if (upstream.modified.empty() || resultTable.modifiedColumnSet.empty()) {
                                        downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                                        if (downstream.modified.nonempty()) {
                                            downstream.modified.close();
                                            downstream.modified = Index.FACTORY.getEmptyIndex();
                                        }
                                    } else {
                                        downstream.modifiedColumnSet = resultTable.modifiedColumnSet;
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
                                    "Bad left column in rename pair \"" + pair.toString() + "\"");
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

                    final QueryTable queryTable = new QueryTable(index, newColumns);
                    if (isRefreshing()) {
                        final ModifiedColumnSet.Transformer mcsTransformer =
                                newModifiedColumnSetTransformer(queryTable, modifiedColumnSetPairs);
                        listenForUpdates(new ShiftAwareListenerImpl("renameColumns(" + Arrays.deepToString(pairs) + ')',
                                this, queryTable) {
                            @Override
                            public void onUpdate(final Update upstream) {
                                final Update downstream = upstream.copy();
                                downstream.modifiedColumnSet = queryTable.modifiedColumnSet;
                                if (upstream.modified.nonempty()) {
                                    mcsTransformer.clearAndTransform(upstream.modifiedColumnSet,
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
                Arrays.stream(columnsToMatch).map(MatchPair::right).collect(Collectors.toCollection(HashSet::new));

        final LinkedHashSet<SelectColumn> columnsToAddSelectColumns = new LinkedHashSet<>();
        final List<String> columnsToUngroupBy = new ArrayList<>();
        final String[] rightColumnsToMatch = new String[columnsToMatch.length];
        for (int i = 0; i < rightColumnsToMatch.length; i++) {
            rightColumnsToMatch[i] = columnsToMatch[i].rightColumn;
            columnsToAddSelectColumns.add(new SourceColumn(columnsToMatch[i].rightColumn));
        }
        final ArrayList<MatchPair> columnsToAddAfterRename = new ArrayList<>(realColumnsToAdd.length);
        for (MatchPair matchPair : realColumnsToAdd) {
            columnsToAddAfterRename.add(new MatchPair(matchPair.leftColumn, matchPair.leftColumn));
            if (!columnsToMatchSet.contains(matchPair.leftColumn)) {
                columnsToUngroupBy.add(matchPair.leftColumn);
            }
            columnsToAddSelectColumns.add(new SourceColumn(matchPair.rightColumn, matchPair.leftColumn));
        }

        return QueryPerformanceRecorder
                .withNugget("join(" + matchString(columnsToMatch) + ", " + matchString(realColumnsToAdd) + ")", () -> {
                    boolean sentinelAdded = false;
                    final Table rightTable;
                    if (columnsToUngroupBy.isEmpty()) {
                        rightTable = rightTableCandidate.updateView("__sentinel__=null");
                        columnsToUngroupBy.add("__sentinel__");
                        columnsToAddSelectColumns.add(new SourceColumn("__sentinel__"));
                        columnsToAddAfterRename.add(new MatchPair("__sentinel__", "__sentinel__"));
                        sentinelAdded = true;
                    } else {
                        rightTable = rightTableCandidate;
                    }

                    final Table rightGrouped = rightTable.by(rightColumnsToMatch)
                            .view(columnsToAddSelectColumns.toArray(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY));
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
     * for the table being snapshotted. The leftIndex refers to snapshots that we want to take. Typically this index is
     * expected to have size 1, but in some cases it could be larger. The rightIndex refers to the index of the current
     * table. Therefore we want to take leftIndex.size() snapshots, each of which being rightIndex.size() in size.
     *
     * @param leftColumns Columns making up the triggering data
     * @param leftIndex The currently triggering rows
     * @param rightColumns Columns making up the data being snapshotted
     * @param rightIndex The data to snapshot
     * @param dest The ColumnSources in which to store the data. The keys are drawn from leftColumns.keys() and\
     *        rightColumns.keys()
     * @param destOffset The offset in the 'dest' ColumnSources at which to write data
     * @return The new dest ColumnSource size, calculated as {@code destOffset + leftIndex.size() * rightIndex.size()}
     */
    private static long snapshotHistoryInternal(
            @NotNull Map<String, ? extends ColumnSource<?>> leftColumns, @NotNull Index leftIndex,
            @NotNull Map<String, ? extends ColumnSource<?>> rightColumns, @NotNull Index rightIndex,
            @NotNull Map<String, ? extends WritableSource<?>> dest, long destOffset) {
        assert leftColumns.size() + rightColumns.size() == dest.size();
        if (leftIndex.empty() || rightIndex.empty()) {
            // Nothing to do.
            return destOffset;
        }

        final long newCapacity = destOffset + leftIndex.size() * rightIndex.size();
        // Ensure all the capacities
        for (WritableSource<?> ws : dest.values()) {
            ws.ensureCapacity(newCapacity);
        }

        final int rightSize = rightIndex.intSize();
        long[] destOffsetHolder = new long[] {destOffset};
        // For each key on the snapshotting side
        leftIndex.forAllLongs(snapshotKey -> {
            final long doff = destOffsetHolder[0];
            destOffsetHolder[0] += rightSize;
            try (final Index destIndex = Index.FACTORY.getIndexByRange(doff, doff + rightSize - 1)) {
                SnapshotUtils.copyStampColumns(leftColumns, snapshotKey, dest, destIndex);
                SnapshotUtils.copyDataColumns(rightColumns, rightIndex, dest, destIndex, false);
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
            final long initialSize = snapshotHistoryInternal(leftTable.getColumnSourceMap(), leftTable.getIndex(),
                    rightTable.getColumnSourceMap(), rightTable.getIndex(),
                    resultColumns, 0);
            final Index resultIndex = Index.FACTORY.getFlatIndex(initialSize);
            final QueryTable result = new QueryTable(resultIndex, resultColumns);
            if (isRefreshing()) {
                listenForUpdates(new ListenerImpl("snapshotHistory" + resultColumns.keySet().toString(), this, result) {
                    private long lastKey = index.lastKey();

                    @Override
                    public void onUpdate(final Index added, final Index removed, final Index modified) {
                        Assert.assertion(removed.size() == 0, "removed.size() == 0",
                                removed, "removed");
                        Assert.assertion(modified.size() == 0, "modified.size() == 0",
                                modified, "modified");
                        if (added.size() == 0 || rightTable.size() == 0) {
                            return;
                        }
                        Assert.assertion(added.firstKey() > lastKey, "added.firstKey() > lastKey",
                                lastKey, "lastKey", added, "added");
                        final long oldSize = resultIndex.size();
                        final long newSize = snapshotHistoryInternal(leftTable.getColumnSourceMap(), added,
                                rightTable.getColumnSourceMap(), rightTable.getIndex(),
                                resultColumns, oldSize);
                        final Index addedSnapshots = Index.FACTORY.getIndexByRange(oldSize, newSize - 1);
                        resultIndex.insert(addedSnapshots);
                        lastKey = index.lastKey();
                        result.notifyListeners(addedSnapshots, Index.FACTORY.getEmptyIndex(),
                                Index.FACTORY.getEmptyIndex());
                    }

                    @Override
                    public boolean canExecute(final long step) {
                        return ((NotificationQueue.Dependency) rightTable).satisfied(step) && super.canExecute(step);
                    }
                });
            }
            result.setFlat();
            return result;
        });
    }

    public Table silent() {
        return new QueryTable(getIndex(), getColumnSourceMap());
    }

    @Override
    @Deprecated
    public void addColumnGrouping(String columnName) {
        // NB: This used to set the group to range map on the column source, but that's not a safe thing to do.
        getIndex().getGrouping(getColumnSource(columnName));
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
        // TODO: we would like to make this operation LTM safe, instead of requiring the lock here; there are two tables
        // but we do only need to listen to one of them; however we are dependent on two of them
        checkInitiateOperation();

        // There are no LazySnapshotTableProviders in the system currently, but they may be used for multicast
        // distribution systems and similar integrations.

        // If this table provides a lazy snapshot version, we should use that instead for the snapshot, this allows us
        // to refresh the table only immediately before the snapshot occurs. Because we know that we are uninterested
        // in things like previous values, it can save a significant amount of CPU to only refresh the table when
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

        final Index resultIndex = Index.FACTORY.getEmptyIndex();
        final QueryTable result = new QueryTable(resultIndex, allColumns);
        final SnapshotInternalListener listener = new SnapshotInternalListener(this, lazySnapshot, tableToSnapshot,
                result, resultLeftColumns, resultRightColumns, resultIndex);

        if (doInitialSnapshot) {
            if (!isRefreshing() && tableToSnapshot.isLive() && !lazySnapshot) {
                // if we are making a static copy of the table, we must ensure that it does not change out from under us
                ConstructSnapshot.callDataSnapshotFunction("snapshotInternal",
                        ConstructSnapshot.makeSnapshotControl(false, (NotificationStepSource) tableToSnapshot),
                        (usePrev, beforeClockUnused) -> {
                            listener.doSnapshot(false, usePrev);
                            resultIndex.initializePreviousValue();
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
                        final ColumnSource<?> cs = getColumnSource(stampColumn);
                        leftColumns.put(stampColumn, cs);
                    }

                    final Map<String, SparseArrayColumnSource<?>> resultLeftColumns = new LinkedHashMap<>();
                    for (Map.Entry<String, ColumnSource<?>> entry : leftColumns.entrySet()) {
                        final String name = entry.getKey();
                        final ColumnSource<?> cs = entry.getValue();
                        final Class<?> type = cs.getType();
                        final SparseArrayColumnSource<?> stampDest = DbArrayBase.class.isAssignableFrom(type)
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

                    final Index resultIndex = Index.FACTORY.getEmptyIndex();

                    final QueryTable resultTable = new QueryTable(resultIndex, resultColumns);

                    if (isRefreshing() && rightTable.isRefreshing()) {

                        // What's happening here: the left table gets "listener" (some complicated logic that has access
                        // to the
                        // coalescer) whereas the right table (above) gets the one-liner above (but which also has
                        // access to the
                        // same coalescer). So when the right table sees updates they are simply fed to the coalescer.
                        // The
                        // coalescer's job is just to remember what rows have changed. When the *left* table gets
                        // updates, then
                        // the SnapshotIncrementalListener gets called, which does all the snapshotting work.

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
                        resultTable.getIndex().initializePreviousValue();
                    } else if (doInitialSnapshot) {
                        SnapshotIncrementalListener.copyRowsToResult(rightTable.getIndex(), this, rightTable,
                                leftColumns, resultColumns);
                        resultTable.getIndex().insert(rightTable.getIndex());
                        resultTable.getIndex().initializePreviousValue();
                    } else if (isRefreshing()) {
                        // we are not doing an initial snapshot, but are refreshing so need to take a snapshot of our
                        // (static)
                        // right table on the very first tick of the leftTable
                        listenForUpdates(
                                new ShiftAwareListenerImpl("snapshotIncremental (leftTable)", this, resultTable) {
                                    @Override
                                    public void onUpdate(Update upstream) {
                                        SnapshotIncrementalListener.copyRowsToResult(rightTable.getIndex(),
                                                QueryTable.this, rightTable, leftColumns, resultColumns);
                                        resultTable.getIndex().insert(rightTable.getIndex());
                                        resultTable.notifyListeners(resultTable.getIndex(),
                                                Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex());
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
        if (DBDateTime.class.isAssignableFrom(columnSource.getType())) {
            if (columnSource.allowsReinterpret(long.class)) {
                return columnSource.reinterpret(long.class);
            } else {
                // noinspection unchecked
                final ColumnSource<DBDateTime> columnSourceAsDateTime = (ColumnSource<DBDateTime>) columnSource;
                return new DatetimeAsLongColumnSource(columnSourceAsDateTime);
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
    public Table sort(SortPair... sortPairs) {
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
                    final Map<String, ColumnSource<?>> dbArrayColumns = new HashMap<>();
                    for (String name : columnsToUngroupBy) {
                        ColumnSource<?> column = getColumnSource(name);
                        if (column.getType().isArray()) {
                            arrayColumns.put(name, column);
                        } else if (DbArrayBase.class.isAssignableFrom(column.getType())) {
                            dbArrayColumns.put(name, column);
                        } else {
                            throw new RuntimeException("Column " + name + " is not an array");
                        }
                    }
                    final long[] sizes = new long[intSize("ungroup")];
                    long maxSize = computeMaxSize(index, arrayColumns, dbArrayColumns, null, sizes, nullFill);
                    final int initialBase = Math.max(64 - Long.numberOfLeadingZeros(maxSize), minimumUngroupBase);
                    final CrossJoinShiftState shiftState = new CrossJoinShiftState(initialBase);

                    final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
                    for (Map.Entry<String, ColumnSource<?>> es : getColumnSourceMap().entrySet()) {
                        final ColumnSource<?> column = es.getValue();
                        final String name = es.getKey();
                        final ColumnSource<?> result;
                        if (dbArrayColumns.containsKey(name) || arrayColumns.containsKey(name)) {
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
                            getUngroupIndex(sizes, Index.FACTORY.getRandomBuilder(), initialBase, index).getIndex(),
                            resultMap);
                    if (isRefreshing()) {
                        startTrackingPrev(resultMap.values());

                        listenForUpdates(new ListenerImpl("ungroup(" + Arrays.deepToString(columnsToUngroupBy) + ')',
                                this, result) {

                            @Override
                            public void onUpdate(final Index added, final Index removed, final Index modified) {
                                intSize("ungroup");

                                int newBase = shiftState.getNumShiftBits();
                                Index.RandomBuilder ungroupAdded = Index.FACTORY.getRandomBuilder();
                                Index.RandomBuilder ungroupModified = Index.FACTORY.getRandomBuilder();
                                Index.RandomBuilder ungroupRemoved = Index.FACTORY.getRandomBuilder();
                                newBase = evaluateIndex(added, ungroupAdded, newBase);
                                newBase = evaluateModified(modified, ungroupModified, ungroupAdded, ungroupRemoved,
                                        newBase);
                                if (newBase > shiftState.getNumShiftBits()) {
                                    rebase(newBase + 1);
                                } else {
                                    evaluateRemovedIndex(removed, ungroupRemoved);
                                    final Index removedIndex = ungroupRemoved.getIndex();
                                    final Index addedIndex = ungroupAdded.getIndex();
                                    result.getIndex().update(addedIndex, removedIndex);
                                    final Index modifiedIndex = ungroupModified.getIndex();

                                    if (!modifiedIndex.subsetOf(result.getIndex())) {
                                        final Index missingModifications = modifiedIndex.minus(result.getIndex());
                                        log.error().append("Result Index: ").append(result.getIndex().toString())
                                                .endl();
                                        log.error().append("Missing modifications: ")
                                                .append(missingModifications.toString()).endl();
                                        log.error().append("Added: ").append(addedIndex.toString()).endl();
                                        log.error().append("Modified: ").append(modifiedIndex.toString()).endl();
                                        log.error().append("Removed: ").append(removedIndex.toString()).endl();

                                        for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                                            ColumnSource<?> arrayColumn = es.getValue();
                                            String name = es.getKey();

                                            Index.Iterator iterator = index.iterator();
                                            for (int i = 0; i < index.size(); i++) {
                                                final long next = iterator.nextLong();
                                                int size = (arrayColumn.get(next) == null ? 0
                                                        : Array.getLength(arrayColumn.get(next)));
                                                int prevSize = (arrayColumn.getPrev(next) == null ? 0
                                                        : Array.getLength(arrayColumn.getPrev(next)));
                                                log.error().append(name).append("[").append(i).append("] ").append(size)
                                                        .append(" -> ").append(prevSize).endl();
                                            }
                                        }

                                        for (Map.Entry<String, ColumnSource<?>> es : dbArrayColumns.entrySet()) {
                                            ColumnSource<?> arrayColumn = es.getValue();
                                            String name = es.getKey();
                                            Index.Iterator iterator = index.iterator();

                                            for (int i = 0; i < index.size(); i++) {
                                                final long next = iterator.nextLong();
                                                long size = (arrayColumn.get(next) == null ? 0
                                                        : ((DbArrayBase<?>) arrayColumn.get(next)).size());
                                                long prevSize = (arrayColumn.getPrev(next) == null ? 0
                                                        : ((DbArrayBase<?>) arrayColumn.getPrev(next)).size());
                                                log.error().append(name).append("[").append(i).append("] ").append(size)
                                                        .append(" -> ").append(prevSize).endl();
                                            }
                                        }

                                        Assert.assertion(false, "modifiedIndex.subsetOf(result.getIndex())",
                                                modifiedIndex, "modifiedIndex", result.getIndex(), "result.getIndex()",
                                                shiftState.getNumShiftBits(), "shiftState.getNumShiftBits()", newBase,
                                                "newBase");
                                    }

                                    for (ColumnSource<?> source : resultMap.values()) {
                                        if (source instanceof UngroupedColumnSource) {
                                            ((UngroupedColumnSource<?>) source).setBase(newBase);
                                        }
                                    }

                                    result.notifyListeners(addedIndex, removedIndex, modifiedIndex);
                                }
                            }

                            private void rebase(final int newBase) {
                                final Index newIndex = getUngroupIndex(
                                        computeSize(getIndex(), arrayColumns, dbArrayColumns, nullFill),
                                        Index.FACTORY.getRandomBuilder(), newBase, getIndex())
                                                .getIndex();
                                final Index index = result.getIndex();
                                final Index added = newIndex.minus(index);
                                final Index removed = index.minus(newIndex);
                                final Index modified = newIndex;
                                modified.retain(index);
                                index.update(added, removed);
                                for (ColumnSource<?> source : resultMap.values()) {
                                    if (source instanceof UngroupedColumnSource) {
                                        ((UngroupedColumnSource<?>) source).setBase(newBase);
                                    }
                                }
                                shiftState.setNumShiftBitsAndUpdatePrev(newBase);
                                result.notifyListeners(added, removed, modified);
                            }

                            private int evaluateIndex(final Index index, final Index.RandomBuilder ungroupBuilder,
                                    final int newBase) {
                                if (index.size() > 0) {
                                    final long[] modifiedSizes = new long[index.intSize("ungroup")];
                                    final long maxSize = computeMaxSize(index, arrayColumns, dbArrayColumns, null,
                                            modifiedSizes, nullFill);
                                    final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);
                                    getUngroupIndex(modifiedSizes, ungroupBuilder, shiftState.getNumShiftBits(), index);
                                    return Math.max(newBase, minBase);
                                }
                                return newBase;
                            }

                            private void evaluateRemovedIndex(final Index index,
                                    final Index.RandomBuilder ungroupBuilder) {
                                if (index.size() > 0) {
                                    final long[] modifiedSizes = new long[index.intSize("ungroup")];
                                    computePrevSize(index, arrayColumns, dbArrayColumns, modifiedSizes, nullFill);
                                    getUngroupIndex(modifiedSizes, ungroupBuilder, shiftState.getNumShiftBits(), index);
                                }
                            }

                            private int evaluateModified(final Index index,
                                    final Index.RandomBuilder modifyBuilder,
                                    final Index.RandomBuilder addedBuilded,
                                    final Index.RandomBuilder removedBuilder,
                                    final int newBase) {
                                if (index.size() > 0) {
                                    final long maxSize = computeModifiedIndicesAndMaxSize(index, arrayColumns,
                                            dbArrayColumns, null, modifyBuilder, addedBuilded, removedBuilder,
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

    private long computeModifiedIndicesAndMaxSize(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, String referenceColumn, Index.RandomBuilder modifyBuilder,
            Index.RandomBuilder addedBuilded, Index.RandomBuilder removedBuilder, long base, boolean nullFill) {
        if (nullFill) {
            return computeModifiedIndicesAndMaxSizeNullFill(index, arrayColumns, dbArrayColumns, referenceColumn,
                    modifyBuilder, addedBuilded, removedBuilder, base);
        }
        return computeModifiedIndicesAndMaxSizeNormal(index, arrayColumns, dbArrayColumns, referenceColumn,
                modifyBuilder, addedBuilded, removedBuilder, base);
    }

    private long computeModifiedIndicesAndMaxSizeNullFill(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, String referenceColumn, Index.RandomBuilder modifyBuilder,
            Index.RandomBuilder addedBuilded, Index.RandomBuilder removedBuilder, long base) {
        long maxSize = 0;
        final Index.Iterator iterator = index.iterator();
        for (int i = 0; i < index.size(); i++) {
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
            for (Map.Entry<String, ColumnSource<?>> es : dbArrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                DbArrayBase<?> array = (DbArrayBase<?>) arrayColumn.get(next);
                final long size = (array == null ? 0 : array.size());
                maxCur = Math.max(maxCur, size);
                DbArrayBase<?> prevArray = (DbArrayBase<?>) arrayColumn.getPrev(next);
                final long prevSize = (prevArray == null ? 0 : prevArray.size());
                maxPrev = Math.max(maxPrev, prevSize);
            }
            maxSize = maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, maxCur, next,
                    maxPrev, base);
        }
        return maxSize;
    }

    private long computeModifiedIndicesAndMaxSizeNormal(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, String referenceColumn, Index.RandomBuilder modifyBuilder,
            Index.RandomBuilder addedBuilded, Index.RandomBuilder removedBuilder, long base) {
        long maxSize = 0;
        boolean sizeIsInitialized = false;
        long[] sizes = new long[index.intSize("ungroup")];
        for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
            ColumnSource<?> arrayColumn = es.getValue();
            String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                Index.Iterator iterator = index.iterator();
                for (int i = 0; i < index.size(); i++) {
                    final long next = iterator.nextLong();
                    Object array = arrayColumn.get(next);
                    sizes[i] = (array == null ? 0 : Array.getLength(array));
                    Object prevArray = arrayColumn.getPrev(next);
                    int prevSize = (prevArray == null ? 0 : Array.getLength(prevArray));
                    maxSize = maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, sizes[i],
                            next, prevSize, base);
                }
            } else {
                Index.Iterator iterator = index.iterator();
                for (int i = 0; i < index.size(); i++) {
                    long k = iterator.nextLong();
                    Assert.assertion(sizes[i] == Array.getLength(arrayColumn.get(k)),
                            "sizes[i] == Array.getLength(arrayColumn.get(k))",
                            referenceColumn, "referenceColumn", name, "name", k, "row");
                }

            }
        }
        for (Map.Entry<String, ColumnSource<?>> es : dbArrayColumns.entrySet()) {
            ColumnSource<?> arrayColumn = es.getValue();
            String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                Index.Iterator iterator = index.iterator();
                for (int i = 0; i < index.size(); i++) {
                    final long next = iterator.nextLong();
                    DbArrayBase<?> array = (DbArrayBase<?>) arrayColumn.get(next);
                    sizes[i] = (array == null ? 0 : array.size());
                    DbArrayBase<?> prevArray = (DbArrayBase<?>) arrayColumn.getPrev(next);
                    long prevSize = (prevArray == null ? 0 : prevArray.size());
                    maxSize = maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, sizes[i],
                            next, prevSize, base);
                }
            } else {
                Index.Iterator iterator = index.iterator();
                for (int i = 0; i < index.size(); i++) {
                    final long next = iterator.nextLong();
                    Assert.assertion(sizes[i] == 0 && arrayColumn.get(next) == null ||
                            sizes[i] == ((DbArrayBase<?>) arrayColumn.get(next)).size(),
                            "sizes[i] == ((DbArrayBase)arrayColumn.get(i)).size()",
                            referenceColumn, "referenceColumn", name, "arrayColumn.getName()", i, "row");
                }
            }
        }
        return maxSize;
    }

    private long maxAndIndexUpdateForRow(Index.RandomBuilder modifyBuilder, Index.RandomBuilder addedBuilded,
            Index.RandomBuilder removedBuilder, long maxSize, long size, long rowKey, long prevSize, long base) {
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
    private static long computeMaxSize(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, String referenceColumn, long[] sizes, boolean nullFill) {
        if (nullFill) {
            return computeMaxSizeNullFill(index, arrayColumns, dbArrayColumns, sizes);
        }

        return computeMaxSizeNormal(index, arrayColumns, dbArrayColumns, referenceColumn, sizes);
    }

    private static long computeMaxSizeNullFill(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, long[] sizes) {
        long maxSize = 0;
        final Index.Iterator iterator = index.iterator();
        for (int i = 0; i < index.size(); i++) {
            long localMax = 0;
            final long nextIndex = iterator.nextLong();
            for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.get(nextIndex);
                final long size = (array == null ? 0 : Array.getLength(array));
                maxSize = Math.max(maxSize, size);
                localMax = Math.max(localMax, size);

            }
            for (Map.Entry<String, ColumnSource<?>> es : dbArrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                        && ((UngroupableColumnSource) arrayColumn).isUngroupable();
                final long size;
                if (isUngroupable) {
                    size = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(nextIndex);
                } else {
                    final DbArrayBase<?> dbArrayBase = (DbArrayBase<?>) arrayColumn.get(nextIndex);
                    size = dbArrayBase != null ? dbArrayBase.size() : 0;
                }
                maxSize = Math.max(maxSize, size);
                localMax = Math.max(localMax, size);
            }
            sizes[i] = localMax;
        }
        return maxSize;
    }


    private static long computeMaxSizeNormal(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, String referenceColumn, long[] sizes) {
        long maxSize = 0;
        boolean sizeIsInitialized = false;
        for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
            ColumnSource<?> arrayColumn = es.getValue();
            String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                Index.Iterator iterator = index.iterator();
                for (int i = 0; i < index.size(); i++) {
                    Object array = arrayColumn.get(iterator.nextLong());
                    sizes[i] = (array == null ? 0 : Array.getLength(array));
                    maxSize = Math.max(maxSize, sizes[i]);
                }
            } else {
                Index.Iterator iterator = index.iterator();
                for (int i = 0; i < index.size(); i++) {
                    Assert.assertion(sizes[i] == Array.getLength(arrayColumn.get(iterator.nextLong())),
                            "sizes[i] == Array.getLength(arrayColumn.get(i))",
                            referenceColumn, "referenceColumn", name, "name", i, "row");
                }

            }
        }
        for (Map.Entry<String, ColumnSource<?>> es : dbArrayColumns.entrySet()) {
            final ColumnSource<?> arrayColumn = es.getValue();
            final String name = es.getKey();
            final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                    && ((UngroupableColumnSource) arrayColumn).isUngroupable();

            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                Index.Iterator iterator = index.iterator();
                for (int ii = 0; ii < index.size(); ii++) {
                    if (isUngroupable) {
                        sizes[ii] = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(iterator.nextLong());
                    } else {
                        final DbArrayBase<?> dbArrayBase = (DbArrayBase<?>) arrayColumn.get(iterator.nextLong());
                        sizes[ii] = dbArrayBase != null ? dbArrayBase.size() : 0;
                    }
                    maxSize = Math.max(maxSize, sizes[ii]);
                }
            } else {
                Index.Iterator iterator = index.iterator();
                for (int i = 0; i < index.size(); i++) {
                    final long expectedSize;
                    if (isUngroupable) {
                        expectedSize = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(iterator.nextLong());
                    } else {
                        final DbArrayBase<?> dbArrayBase = (DbArrayBase<?>) arrayColumn.get(iterator.nextLong());
                        expectedSize = dbArrayBase != null ? dbArrayBase.size() : 0;
                    }
                    Assert.assertion(sizes[i] == expectedSize, "sizes[i] == ((DbArrayBase)arrayColumn.get(i)).size()",
                            referenceColumn, "referenceColumn", name, "arrayColumn.getName()", i, "row");
                }
            }
        }
        return maxSize;
    }

    private static void computePrevSize(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, long[] sizes, boolean nullFill) {
        if (nullFill) {
            computePrevSizeNullFill(index, arrayColumns, dbArrayColumns, sizes);
        } else {
            computePrevSizeNormal(index, arrayColumns, dbArrayColumns, sizes);
        }
    }

    private static void computePrevSizeNullFill(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, long[] sizes) {
        final Index.Iterator iterator = index.iterator();
        for (int i = 0; i < index.size(); i++) {
            long localMax = 0;
            final long nextIndex = iterator.nextLong();
            for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.getPrev(nextIndex);
                final long size = (array == null ? 0 : Array.getLength(array));
                localMax = Math.max(localMax, size);

            }
            for (Map.Entry<String, ColumnSource<?>> es : dbArrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                        && ((UngroupableColumnSource) arrayColumn).isUngroupable();
                final long size;
                if (isUngroupable) {
                    size = ((UngroupableColumnSource) arrayColumn).getUngroupedPrevSize(nextIndex);
                } else {
                    final DbArrayBase<?> dbArrayBase = (DbArrayBase<?>) arrayColumn.getPrev(nextIndex);
                    size = dbArrayBase != null ? dbArrayBase.size() : 0;
                }
                localMax = Math.max(localMax, size);
            }
            sizes[i] = localMax;
        }
    }

    private static void computePrevSizeNormal(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, long[] sizes) {
        for (ColumnSource<?> arrayColumn : arrayColumns.values()) {
            Index.Iterator iterator = index.iterator();
            for (int i = 0; i < index.size(); i++) {
                Object array = arrayColumn.getPrev(iterator.nextLong());
                sizes[i] = (array == null ? 0 : Array.getLength(array));
            }
            return; // TODO: WTF??
        }
        for (ColumnSource<?> arrayColumn : dbArrayColumns.values()) {
            final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                    && ((UngroupableColumnSource) arrayColumn).isUngroupable();

            Index.Iterator iterator = index.iterator();
            for (int i = 0; i < index.size(); i++) {
                if (isUngroupable) {
                    sizes[i] = ((UngroupableColumnSource) arrayColumn).getUngroupedPrevSize(iterator.nextLong());
                } else {
                    DbArrayBase<?> array = (DbArrayBase<?>) arrayColumn.getPrev(iterator.nextLong());
                    sizes[i] = array == null ? 0 : array.size();
                }
            }
            return; // TODO: WTF??
        }
    }

    private static long[] computeSize(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns, boolean nullFill) {
        if (nullFill) {
            return computeSizeNullFill(index, arrayColumns, dbArrayColumns);
        }

        return computeSizeNormal(index, arrayColumns, dbArrayColumns);
    }

    private static long[] computeSizeNullFill(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns) {
        final long[] sizes = new long[index.intSize("ungroup")];
        final Index.Iterator iterator = index.iterator();
        for (int i = 0; i < index.size(); i++) {
            long localMax = 0;
            final long nextIndex = iterator.nextLong();
            for (Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.get(nextIndex);
                final long size = (array == null ? 0 : Array.getLength(array));
                localMax = Math.max(localMax, size);

            }
            for (Map.Entry<String, ColumnSource<?>> es : dbArrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                        && ((UngroupableColumnSource) arrayColumn).isUngroupable();
                final long size;
                if (isUngroupable) {
                    size = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(nextIndex);
                } else {
                    final DbArrayBase<?> dbArrayBase = (DbArrayBase<?>) arrayColumn.get(nextIndex);
                    size = dbArrayBase != null ? dbArrayBase.size() : 0;
                }
                localMax = Math.max(localMax, size);
            }
            sizes[i] = localMax;
        }
        return sizes;
    }

    private static long[] computeSizeNormal(Index index, Map<String, ColumnSource<?>> arrayColumns,
            Map<String, ColumnSource<?>> dbArrayColumns) {
        final long[] sizes = new long[index.intSize("ungroup")];
        for (ColumnSource<?> arrayColumn : arrayColumns.values()) {
            Index.Iterator iterator = index.iterator();
            for (int i = 0; i < index.size(); i++) {
                Object array = arrayColumn.get(iterator.nextLong());
                sizes[i] = (array == null ? 0 : Array.getLength(array));
            }
            return sizes; // TODO: WTF??
        }
        for (ColumnSource<?> arrayColumn : dbArrayColumns.values()) {
            final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                    && ((UngroupableColumnSource) arrayColumn).isUngroupable();

            Index.Iterator iterator = index.iterator();
            for (int i = 0; i < index.size(); i++) {
                if (isUngroupable) {
                    sizes[i] = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(iterator.nextLong());
                } else {
                    DbArrayBase<?> array = (DbArrayBase<?>) arrayColumn.get(iterator.nextLong());
                    sizes[i] = array == null ? 0 : array.size();
                }
            }
            return sizes; // TODO: WTF??
        }
        return null;
    }

    private IndexBuilder getUngroupIndex(
            final long[] sizes, final Index.RandomBuilder indexBuilder, final long base, final Index index) {
        Assert.assertion(base >= 0 && base <= 63, "base >= 0 && base <= 63", base, "base");
        long mask = ((1L << base) - 1) << (64 - base);
        long lastKey = index.lastKey();
        if ((lastKey > 0) && ((lastKey & mask) != 0)) {
            throw new IllegalStateException(
                    "Key overflow detected, perhaps you should flatten your table before calling ungroup.  "
                            + ",lastKey=" + lastKey + ", base=" + base);
        }

        int pos = 0;
        for (Index.Iterator iterator = index.iterator(); iterator.hasNext();) {
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
    public Table selectDistinct(SelectColumn... columns) {
        return QueryPerformanceRecorder.withNugget("selectDistinct(" + Arrays.toString(columns) + ")",
                sizeForInstrumentation(),
                () -> by(new SelectDistinctStateFactoryImpl(), columns));
    }

    @Override
    public QueryTable getSubTable(Index index) {
        return getSubTable(index, null, CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
    }

    public QueryTable getSubTable(@NotNull final Index index, @Nullable final ModifiedColumnSet resultModifiedColumnSet,
            @NotNull final Object... parents) {
        return QueryPerformanceRecorder.withNugget("getSubTable", sizeForInstrumentation(), () -> {
            // there is no operation check here, because byExternal calls it internally; and the Index results are
            // not updated internally, but rather externally.
            final QueryTable result = new QueryTable(definition, index, columns, resultModifiedColumnSet);
            for (Object parent : parents) {
                result.addParentReference(parent);
            }

            result.setLastNotificationStep(getLastNotificationStep());

            return result;
        });
    }

    /**
     * Copies this table, but with a new set of attributes.
     *
     * @return an identical table; but with a new set of attributes
     */
    @Override
    public Table copy() {
        return copy(true);
    }

    public Table copy(boolean copyAttributes) {
        return copy(definition, copyAttributes);
    }

    public Table copy(TableDefinition definition, boolean copyAttributes) {
        return QueryPerformanceRecorder.withNugget("copy()", sizeForInstrumentation(), () -> {
            final Mutable<Table> result = new MutableObject<>();

            final ShiftAwareSwapListener swapListener = createSwapListenerIfRefreshing(ShiftAwareSwapListener::new);
            initializeWithSnapshot("copy", swapListener, (usePrev, beforeClockValue) -> {
                final QueryTable resultTable = new CopiedTable(definition, this);
                propagateFlatness(resultTable);
                if (copyAttributes) {
                    copyAttributes(resultTable, a -> true);
                }

                if (swapListener != null) {
                    final ShiftAwareListenerImpl listener = new ShiftAwareListenerImpl("copy()", this, resultTable);
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
            super(definition, parent.index, parent.columns, null);
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
                    final Table myResult = ((QueryTable) parentResult).copy(false);
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

        final MemoizedResult<R> cachedResult = getMemoizedResult(memoKey, cachedOperations);
        return cachedResult.getOrCompute(operation);
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
            return cachedResult instanceof DynamicTable && ((DynamicTable) cachedResult).isFailed();
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

            final ShiftAwareSwapListener swapListener;
            if (isRefreshing()) {
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
        private final Index currentMapping;
        private final SelectFilter[] filters;
        private final ModifiedColumnSet filterColumns;
        private final ListenerRecorder recorder;

        private WhereListener(ListenerRecorder recorder, Collection<NotificationQueue.Dependency> dependencies,
                FilteredTable result) {
            super(Collections.singleton(recorder), dependencies, "where(" + Arrays.toString(result.filters) + ")",
                    result);
            this.recorder = recorder;
            this.result = result;
            this.currentMapping = result.getIndex();
            this.filters = result.filters;

            boolean hasColumnArray = false;
            final Set<String> filterColumnNames = new TreeSet<>();
            for (SelectFilter filter : filters) {
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
                result.modifiedColumnSet.clear();
                sourceModColumns = result.modifiedColumnSet;
            }

            if (result.refilterRequested()) {
                result.doRefilter(recorder.getAdded(), recorder.getRemoved(), recorder.getModified(),
                        recorder.getShifted(),
                        sourceModColumns);
                return;
            }

            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();

            // intersect removed with pre-shift keyspace
            update.removed = recorder.getRemoved().intersect(currentMapping);
            currentMapping.remove(update.removed);

            // shift keyspace
            recorder.getShifted().apply(currentMapping);

            // compute added against filters
            update.added = whereInternal(recorder.getAdded().clone(), index, false, filters);

            // check which modified keys match filters (Note: filterColumns will be null if we must always check)
            final boolean runFilters = filterColumns == null || sourceModColumns.containsAny(filterColumns);
            final Index matchingModifies = !runFilters ? Index.FACTORY.getEmptyIndex()
                    : whereInternal(recorder.getModified(), index, false, filters);

            // which propagate as mods?
            update.modified = (runFilters ? matchingModifies : recorder.getModified()).intersect(currentMapping);

            // remaining matchingModifies are adds
            update.added.insert(matchingModifies.minus(update.modified));

            final Index modsToRemove;
            if (!runFilters) {
                modsToRemove = Index.FACTORY.getEmptyIndex();
            } else {
                modsToRemove = recorder.getModified().minus(matchingModifies);
                modsToRemove.retain(currentMapping);
            }
            // note modsToRemove is currently in post-shift keyspace
            currentMapping.update(update.added, modsToRemove);

            // move modsToRemove into pre-shift keyspace and add to myRemoved
            recorder.getShifted().unapply(modsToRemove);
            update.removed.insert(modsToRemove);

            update.modifiedColumnSet = sourceModColumns;
            if (update.modified.empty()) {
                result.modifiedColumnSet.clear();
                update.modifiedColumnSet = result.modifiedColumnSet;
            }

            // note shifts are pass-through since filter will never translate keyspace
            update.shifted = recorder.getShifted();

            result.notifyListeners(update);
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
            LiveTableMonitor.DEFAULT.checkInitiateTableOperation();
        }
    }

    static void checkInitiateOperation(Table other) {
        if (other.isLive()) {
            LiveTableMonitor.DEFAULT.checkInitiateTableOperation();
        }
    }

    @Override
    public <R> R apply(Function.Unary<R, Table> function) {
        if (function instanceof MemoizedOperationKey.Provider) {
            return memoizeResult(((MemoizedOperationKey.Provider) function).getMemoKey(), () -> super.apply(function));
        }

        return super.apply(function);
    }

    public Table wouldMatch(WouldMatchPair... matchers) {
        return getResult(new WouldMatchOperation(this, matchers));
    }
}
