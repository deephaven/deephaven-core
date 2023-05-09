/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.Pair;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.select.SwitchColumn;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.util.InverseWrappedRowSetRowRedirection;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseablePair;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public abstract class SelectAndViewAnalyzer implements LogOutputAppendable {
    public enum Mode {
        VIEW_LAZY, VIEW_EAGER, SELECT_STATIC, SELECT_REFRESHING, SELECT_REDIRECTED_REFRESHING, SELECT_REDIRECTED_STATIC
    }

    public static void initializeSelectColumns(
            final Map<String, ColumnDefinition<?>> parentColumnMap,
            final SelectColumn[] selectColumns) {
        final Map<String, ColumnDefinition<?>> targetColumnMap = new HashMap<>(parentColumnMap);
        for (SelectColumn column : selectColumns) {
            column.initDef(targetColumnMap);
            final ColumnDefinition<?> columnDefinition =
                    ColumnDefinition.fromGenericType(column.getName(), column.getReturnedType());
            targetColumnMap.put(column.getName(), columnDefinition);
        }
    }

    public static SelectAndViewAnalyzerWrapper create(
            QueryTable sourceTable, Mode mode, Map<String, ColumnSource<?>> columnSources,
            TrackingRowSet rowSet, ModifiedColumnSet parentMcs, boolean publishTheseSources, boolean useShiftedColumns,
            SelectColumn... selectColumns) {
        return create(sourceTable, mode, columnSources, rowSet, parentMcs, publishTheseSources, useShiftedColumns,
                true, selectColumns);
    }

    public static SelectAndViewAnalyzerWrapper create(
            final QueryTable sourceTable,
            final Mode mode,
            final Map<String, ColumnSource<?>> columnSources,
            TrackingRowSet rowSet,
            final ModifiedColumnSet parentMcs,
            final boolean publishTheseSources,
            boolean useShiftedColumns,
            final boolean allowInternalFlatten,
            final SelectColumn... selectColumns) {
        SelectAndViewAnalyzer analyzer = createBaseLayer(columnSources, publishTheseSources);
        final Map<String, ColumnDefinition<?>> columnDefinitions = new LinkedHashMap<>();
        final RowRedirection rowRedirection;
        if (mode == Mode.SELECT_REDIRECTED_STATIC) {
            rowRedirection = new InverseWrappedRowSetRowRedirection(rowSet);
        } else if (mode == Mode.SELECT_REDIRECTED_REFRESHING && rowSet.size() < Integer.MAX_VALUE) {
            final WritableRowRedirection writableRowRedirection =
                    WritableRowRedirection.FACTORY.createRowRedirection(rowSet.intSize());
            analyzer = analyzer.createRedirectionLayer(rowSet, writableRowRedirection);
            rowRedirection = writableRowRedirection;
        } else {
            rowRedirection = null;
        }

        final TrackingRowSet originalRowSet = rowSet;
        boolean flatResult = rowSet.isFlat();
        // if we preserve a column, we set this to false
        boolean flattenedResult = !flatResult
                && allowInternalFlatten
                && (columnSources.isEmpty() || !publishTheseSources)
                && mode == Mode.SELECT_STATIC;
        int numberOfInternallyFlattenedColumns = 0;

        List<SelectColumn> processedCols = new LinkedList<>();
        List<SelectColumn> remainingCols = null;
        FormulaColumn shiftColumn = null;
        boolean shiftColumnHasPositiveOffset = false;

        final HashSet<String> resultColumns = flattenedResult ? new HashSet<>() : null;
        for (final SelectColumn sc : selectColumns) {
            if (remainingCols != null) {
                remainingCols.add(sc);
                continue;
            }

            analyzer.updateColumnDefinitionsFromTopLayer(columnDefinitions);
            sc.initDef(columnDefinitions);
            sc.initInputs(rowSet, analyzer.getAllColumnSources());

            // When flattening the result, intermediate columns generate results in position space. When we discover
            // that a select column depends on an intermediate result, then we must flatten all parent columns so
            // that all dependent columns are in the same result-key space.
            if (!flatResult && flattenedResult && Stream.concat(sc.getColumns().stream(), sc.getColumnArrays().stream())
                    .anyMatch(resultColumns::contains)) {
                analyzer = analyzer.createStaticFlattenLayer(rowSet);
                rowSet = RowSetFactory.flat(rowSet.size()).toTracking();
                flatResult = true;

                // we must re-initialize the column inputs as they may have changed post-flatten
                sc.initInputs(rowSet, analyzer.getAllColumnSources());
            } else if (!flatResult && flattenedResult) {
                resultColumns.add(sc.getName());
            }

            final Stream<String> allDependencies =
                    Stream.concat(sc.getColumns().stream(), sc.getColumnArrays().stream());
            final String[] distinctDeps = allDependencies.distinct().toArray(String[]::new);
            final ModifiedColumnSet mcsBuilder = new ModifiedColumnSet(parentMcs);

            if (useShiftedColumns && hasConstantArrayAccess(sc)) {
                remainingCols = new LinkedList<>();
                shiftColumn = sc instanceof FormulaColumn
                        ? (FormulaColumn) sc
                        : (FormulaColumn) ((SwitchColumn) sc).getRealColumn();
                shiftColumnHasPositiveOffset = hasPositiveOffsetConstantArrayAccess(sc);
                continue;
            }

            // shifted columns appear to not be safe for refresh, so we do not validate them until they are rewritten
            // using the intermediary shifted column
            if (sourceTable.isRefreshing()) {
                sc.validateSafeForRefresh(sourceTable);
            }

            processedCols.add(sc);

            if (hasConstantValue(sc)) {
                final WritableColumnSource<?> constViewSource =
                        SingleValueColumnSource.getSingleValueColumnSource(sc.getReturnedType());
                analyzer = analyzer.createLayerForConstantView(
                        sc.getName(), sc, constViewSource, distinctDeps, mcsBuilder, flattenedResult,
                        flatResult && flattenedResult);
                continue;
            }

            if (shouldPreserve(sc)) {
                if (numberOfInternallyFlattenedColumns > 0) {
                    // we must preserve this column, but have already created an analyzer for the internally flattened
                    // column, therefore must start over without permitting internal flattening
                    return create(sourceTable, mode, columnSources, originalRowSet, parentMcs, publishTheseSources,
                            false, selectColumns);
                }
                analyzer =
                        analyzer.createLayerForPreserve(sc.getName(), sc, sc.getDataView(), distinctDeps, mcsBuilder);
                // we can not flatten future columns because we are preserving this column
                flattenedResult = false;
                continue;
            }

            final long targetDestinationCapacity =
                    rowSet.isEmpty() ? 0 : (flattenedResult ? rowSet.size() : rowSet.lastRowKey() + 1);
            switch (mode) {
                case VIEW_LAZY: {
                    final ColumnSource<?> viewCs = sc.getLazyView();
                    analyzer = analyzer.createLayerForView(sc.getName(), sc, viewCs, distinctDeps, mcsBuilder);
                    break;
                }
                case VIEW_EAGER: {
                    final ColumnSource<?> viewCs = sc.getDataView();
                    analyzer = analyzer.createLayerForView(sc.getName(), sc, viewCs, distinctDeps, mcsBuilder);
                    break;
                }
                case SELECT_STATIC: {
                    // We need to call newDestInstance because only newDestInstance has the knowledge to endow our
                    // created array with the proper componentType (in the case of Vectors).
                    final WritableColumnSource<?> scs =
                            flatResult || flattenedResult ? sc.newFlatDestInstance(targetDestinationCapacity)
                                    : sc.newDestInstance(targetDestinationCapacity);
                    analyzer =
                            analyzer.createLayerForSelect(rowSet, sc.getName(), sc, scs, null, distinctDeps, mcsBuilder,
                                    false, flattenedResult, flatResult && flattenedResult);
                    if (flattenedResult) {
                        numberOfInternallyFlattenedColumns++;
                    }
                    break;
                }
                case SELECT_REDIRECTED_STATIC: {
                    final WritableColumnSource<?> underlyingSource = sc.newDestInstance(rowSet.size());
                    final WritableColumnSource<?> scs = WritableRedirectedColumnSource.maybeRedirect(
                            rowRedirection, underlyingSource, rowSet.size());
                    analyzer =
                            analyzer.createLayerForSelect(rowSet, sc.getName(), sc, scs, underlyingSource, distinctDeps,
                                    mcsBuilder, true, false, false);
                    break;
                }
                case SELECT_REDIRECTED_REFRESHING:
                case SELECT_REFRESHING: {
                    // We need to call newDestInstance because only newDestInstance has the knowledge to endow our
                    // created array with the proper componentType (in the case of Vectors).
                    // TODO(kosak): use DeltaAwareColumnSource
                    WritableColumnSource<?> scs = sc.newDestInstance(targetDestinationCapacity);
                    WritableColumnSource<?> underlyingSource = null;
                    if (rowRedirection != null) {
                        underlyingSource = scs;
                        scs = WritableRedirectedColumnSource.maybeRedirect(
                                rowRedirection, underlyingSource, rowSet.intSize());
                    }
                    analyzer =
                            analyzer.createLayerForSelect(rowSet, sc.getName(), sc, scs, underlyingSource, distinctDeps,
                                    mcsBuilder, rowRedirection != null, false, false);
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported case " + mode);
            }
        }
        return new SelectAndViewAnalyzerWrapper(analyzer, shiftColumn, shiftColumnHasPositiveOffset, remainingCols,
                processedCols);
    }

    private static boolean hasConstantArrayAccess(final SelectColumn sc) {
        if (sc instanceof FormulaColumn) {
            return ((FormulaColumn) sc).hasConstantArrayAccess();
        } else if (sc instanceof SwitchColumn) {
            final SelectColumn realColumn = ((SwitchColumn) sc).getRealColumn();
            if (realColumn instanceof FormulaColumn) {
                return ((FormulaColumn) realColumn).hasConstantArrayAccess();
            }
        }
        return false;
    }

    private static boolean hasPositiveOffsetConstantArrayAccess(final SelectColumn sc) {
        Pair<String, Map<Long, List<MatchPair>>> shifts = null;
        if (sc instanceof FormulaColumn) {
            shifts = ((FormulaColumn) sc).getFormulaShiftColPair();
        } else if (sc instanceof SwitchColumn) {
            final SelectColumn realColumn = ((SwitchColumn) sc).getRealColumn();
            if (realColumn instanceof FormulaColumn) {
                shifts = ((FormulaColumn) realColumn).getFormulaShiftColPair();
            }
        }
        if (shifts == null) {
            throw new IllegalStateException("Column " + sc.getName() + " does not have constant array access");
        }
        return shifts.getSecond().keySet().stream().max(Long::compareTo).orElse(0L) > 0;
    }


    private static boolean hasConstantValue(final SelectColumn sc) {
        if (sc instanceof FormulaColumn) {
            return ((FormulaColumn) sc).hasConstantValue();
        } else if (sc instanceof SwitchColumn) {
            final SelectColumn realColumn = ((SwitchColumn) sc).getRealColumn();
            if (realColumn instanceof FormulaColumn) {
                return ((FormulaColumn) realColumn).hasConstantValue();
            }
        }
        return false;
    }

    private static boolean shouldPreserve(final SelectColumn sc) {
        if (!(sc instanceof SourceColumn)
                && (!(sc instanceof SwitchColumn) || !(((SwitchColumn) sc).getRealColumn() instanceof SourceColumn))) {
            return false;
        }
        final ColumnSource<?> sccs = sc.getDataView();
        return sccs instanceof InMemoryColumnSource && ((InMemoryColumnSource) sccs).isInMemory()
                && !Vector.class.isAssignableFrom(sc.getReturnedType());
    }

    static final int BASE_LAYER_INDEX = 0;
    static final int REDIRECTION_LAYER_INDEX = 1;

    /**
     * The layerIndex is used to identify each layer uniquely within the bitsets for completion.
     */
    private final int layerIndex;

    public SelectAndViewAnalyzer(int layerIndex) {
        this.layerIndex = layerIndex;
    }

    int getLayerIndex() {
        return layerIndex;
    }

    /**
     * Set the bits in bitset that represent the base layer and optional redirection layer. No other jobs can be
     * executed until all of these bits are set.
     *
     * @param bitset the bitset to manipulate.
     */
    abstract void setBaseBits(BitSet bitset);

    /**
     * Set the bits in bitset that represent all the new columns. This is used to identify when the select or update
     * operation is complete.
     *
     * @param bitset the bitset to manipulate.
     */
    public void setAllNewColumns(BitSet bitset) {
        getInner().setAllNewColumns(bitset);
        bitset.set(getLayerIndex());
    }

    private static SelectAndViewAnalyzer createBaseLayer(Map<String, ColumnSource<?>> sources,
            boolean publishTheseSources) {
        return new BaseLayer(sources, publishTheseSources);
    }

    private RedirectionLayer createRedirectionLayer(TrackingRowSet resultRowSet,
            WritableRowRedirection rowRedirection) {
        return new RedirectionLayer(this, resultRowSet, rowRedirection);
    }

    private StaticFlattenLayer createStaticFlattenLayer(TrackingRowSet parentRowSet) {
        return new StaticFlattenLayer(this, parentRowSet);
    }

    private SelectAndViewAnalyzer createLayerForSelect(RowSet parentRowset, String name, SelectColumn sc,
            WritableColumnSource<?> cs, WritableColumnSource<?> underlyingSource,
            String[] parentColumnDependencies, ModifiedColumnSet mcsBuilder, boolean isRedirected,
            boolean flattenResult, boolean alreadyFlattened) {
        return new SelectColumnLayer(parentRowset, this, name, sc, cs, underlyingSource, parentColumnDependencies,
                mcsBuilder, isRedirected, flattenResult, alreadyFlattened);
    }

    private SelectAndViewAnalyzer createLayerForConstantView(String name, SelectColumn sc, WritableColumnSource<?> cs,
            String[] parentColumnDependencies, ModifiedColumnSet mcsBuilder, boolean flattenResult,
            boolean alreadyFlattened) {
        return new ConstantColumnLayer(this, name, sc, cs, parentColumnDependencies, mcsBuilder, flattenResult,
                alreadyFlattened);
    }

    private SelectAndViewAnalyzer createLayerForView(String name, SelectColumn sc, ColumnSource<?> cs,
            String[] parentColumnDependencies, ModifiedColumnSet mcsBuilder) {
        return new ViewColumnLayer(this, name, sc, cs, parentColumnDependencies, mcsBuilder);
    }

    private SelectAndViewAnalyzer createLayerForPreserve(String name, SelectColumn sc, ColumnSource<?> cs,
            String[] parentColumnDependencies, ModifiedColumnSet mcsBuilder) {
        return new PreserveColumnLayer(this, name, sc, cs, parentColumnDependencies, mcsBuilder);
    }

    abstract void populateModifiedColumnSetRecurse(ModifiedColumnSet mcsBuilder, Set<String> remainingDepsToSatisfy);

    enum GetMode {
        All, New, Published
    }

    public final Map<String, ColumnSource<?>> getAllColumnSources() {
        return getColumnSourcesRecurse(GetMode.All);
    }

    public final Map<String, ColumnSource<?>> getNewColumnSources() {
        return getColumnSourcesRecurse(GetMode.New);
    }

    public final Map<String, ColumnSource<?>> getPublishedColumnSources() {
        return getColumnSourcesRecurse(GetMode.Published);
    }

    abstract Map<String, ColumnSource<?>> getColumnSourcesRecurse(GetMode mode);

    public static class UpdateHelper implements SafeCloseable {
        private RowSet existingRows;
        private SafeCloseablePair<RowSet, RowSet> shiftedWithModifies;
        private SafeCloseablePair<RowSet, RowSet> shiftedWithoutModifies;

        private final RowSet parentRowSet;
        private final TableUpdate upstream;

        public UpdateHelper(RowSet parentRowSet, TableUpdate upstream) {
            this.parentRowSet = parentRowSet;
            this.upstream = upstream;
        }

        private RowSet getExisting() {
            if (existingRows == null) {
                existingRows = parentRowSet.minus(upstream.added());
            }
            return existingRows;
        }

        private void ensure(boolean withModifies) {
            if (withModifies && shiftedWithModifies == null) {
                shiftedWithModifies = SafeCloseablePair
                        .downcast(upstream.shifted().extractParallelShiftedRowsFromPostShiftRowSet(getExisting()));
            } else if (!withModifies && shiftedWithoutModifies == null) {
                try (final RowSet candidates = getExisting().minus(upstream.modified())) {
                    shiftedWithoutModifies = SafeCloseablePair
                            .downcast(upstream.shifted().extractParallelShiftedRowsFromPostShiftRowSet(candidates));
                }
            }
        }

        RowSet getPreShifted(boolean withModifies) {
            if (!withModifies && upstream.modified().isEmpty()) {
                return getPreShifted(true);
            }
            ensure(withModifies);
            return withModifies ? shiftedWithModifies.first : shiftedWithoutModifies.first;
        }

        RowSet getPostShifted(boolean withModifies) {
            if (!withModifies && upstream.modified().isEmpty()) {
                return getPostShifted(true);
            }
            ensure(withModifies);
            return withModifies ? shiftedWithModifies.second : shiftedWithoutModifies.second;
        }

        @Override
        public void close() {
            if (existingRows != null) {
                existingRows.close();
                existingRows = null;
            }
            if (shiftedWithModifies != null) {
                shiftedWithModifies.close();
                shiftedWithModifies = null;
            }
            if (shiftedWithoutModifies != null) {
                shiftedWithoutModifies.close();
                shiftedWithoutModifies = null;
            }
        }
    }

    /**
     * Apply this update to this SelectAndViewAnalyzer.
     *
     * @param upstream the upstream update
     * @param toClear rows that used to exist and no longer exist
     * @param helper convenience class that memoizes reusable calculations for this update
     * @param jobScheduler scheduler for parallel sub-tasks
     * @param liveResultOwner {@link LivenessNode node} to be used to manage/unmanage results that happen to be
     *        {@link io.deephaven.engine.liveness.LivenessReferent liveness referents}
     * @param onCompletion Called when an inner column is complete. The outer layer should pass the {@code onCompletion}
     */
    public abstract void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper,
            JobScheduler jobScheduler, @Nullable LivenessNode liveResultOwner,
            SelectLayerCompletionHandler onCompletion);

    /**
     * Our job here is to calculate the effects: a map from incoming column to a list of columns that it effects. We do
     * this in two stages. In the first stage we create a map from column to (set of dependent columns). In the second
     * stage we reverse that map.
     */
    public final Map<String, String[]> calcEffects(boolean forcePublishAllResources) {
        final Map<String, Set<String>> dependsOn = calcDependsOnRecurse(forcePublishAllResources);

        // Now create effects, which is the inverse of dependsOn:
        // An entry W -> [X, Y, Z] in effects means that W affects X, Y, and Z
        final Map<String, List<String>> effects = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : dependsOn.entrySet()) {
            final String depender = entry.getKey();
            for (final String dependee : entry.getValue()) {
                effects.computeIfAbsent(dependee, dummy -> new ArrayList<>()).add(depender);
            }
        }
        // Convert effects type into result type
        final Map<String, String[]> result = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : effects.entrySet()) {
            final String[] value = entry.getValue().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
            result.put(entry.getKey(), value);
        }
        return result;
    }

    abstract Map<String, Set<String>> calcDependsOnRecurse(boolean forcePublishAllResources);

    public abstract SelectAndViewAnalyzer getInner();

    public abstract void updateColumnDefinitionsFromTopLayer(Map<String, ColumnDefinition<?>> columnDefinitions);

    public abstract void startTrackingPrev();

    /**
     * Was the result internally flattened? Only the STATIC_SELECT case flattens the result. If the result preserves any
     * columns, then flattening is not permitted. Because all the other layers cannot internally flatten, the default
     * implementation returns false.
     */
    public boolean flattenedResult() {
        return false;
    }

    /**
     * Have the column sources already been flattened? Only the STATIC_SELECT case flattens the result. A static flatten
     * layer is only added if SelectColumn depends on an intermediate result.
     */
    public boolean alreadyFlattenedSources() {
        return false;
    }

    /**
     * Return the layerIndex for a given string column.
     *
     * <p>
     * This is executed recursively, because later columns in a select statement hide earlier columns.
     * </p>
     *
     * @param column the name of the column
     *
     * @return the layerIndex
     */
    abstract int getLayerIndexFor(String column);

    /**
     * Can all of our columns permit parallel updates?
     */
    abstract public boolean allowCrossColumnParallelization();

    /**
     * A class that handles the completion of one select column. The handlers are chained together so that when a column
     * completes all of the downstream dependencies may execute.
     */
    public static abstract class SelectLayerCompletionHandler {
        /**
         * Note that the completed columns are shared among the entire chain of completion handlers.
         */
        private final BitSet completedColumns;
        private final SelectLayerCompletionHandler nextHandler;
        private final BitSet requiredColumns;
        private volatile boolean fired = false;

        /**
         * Create a new completion handler that calls nextHandler after its own processing. The completedColumns BitSet
         * is shared among all handlers.
         *
         * @param requiredColumns the columns required for this layer
         * @param nextHandler the next handler to call
         */
        SelectLayerCompletionHandler(BitSet requiredColumns, SelectLayerCompletionHandler nextHandler) {
            this.requiredColumns = requiredColumns;
            this.completedColumns = nextHandler.completedColumns;
            this.nextHandler = nextHandler;
        }

        /**
         * Create the final completion handler, which has no next handler.
         *
         * @param requiredColumns the columns required for this handler to fire
         * @param completedColumns the set of completed columns, shared with all of the other handlers
         */
        public SelectLayerCompletionHandler(BitSet requiredColumns, BitSet completedColumns) {
            this.requiredColumns = requiredColumns;
            this.completedColumns = completedColumns;
            this.nextHandler = null;
        }

        /**
         * Called when a single column is completed.
         *
         * If we are ready, then we call {@link #onAllRequiredColumnsCompleted()}.
         *
         * We may not be ready, but other columns downstream of us may be ready, so they are also notified (the
         * nextHandler).
         *
         * @param completedColumn the layerIndex of the completedColumn
         */
        void onLayerCompleted(int completedColumn) {
            if (!fired) {
                boolean readyToFire = false;
                synchronized (completedColumns) {
                    if (!fired) {
                        completedColumns.set(completedColumn);
                        if (requiredColumns.get(completedColumn) || requiredColumns.isEmpty()) {
                            readyToFire = requiredColumns.stream().allMatch(completedColumns::get);
                            if (readyToFire) {
                                fired = true;
                            }
                        }
                    }
                }
                if (readyToFire) {
                    onAllRequiredColumnsCompleted();
                }
            }
            if (nextHandler != null) {
                nextHandler.onLayerCompleted(completedColumn);
            }
        }

        protected void onError(Exception error) {
            if (nextHandler != null) {
                nextHandler.onError(error);
            }
        }

        /**
         * Called when all of the required columns are completed.
         */
        protected abstract void onAllRequiredColumnsCompleted();
    }

    /**
     * Create a completion handler that signals a future when the update is completed.
     *
     * @param waitForResult a void future indicating success or failure
     *
     * @return a completion handler that will signal the future
     */
    public SelectLayerCompletionHandler futureCompletionHandler(CompletableFuture<Void> waitForResult) {
        final BitSet completedColumns = new BitSet();
        final BitSet requiredColumns = new BitSet();

        setAllNewColumns(requiredColumns);

        return new SelectLayerCompletionHandler(requiredColumns, completedColumns) {
            boolean errorOccurred = false;

            @Override
            public void onAllRequiredColumnsCompleted() {
                if (errorOccurred) {
                    return;
                }
                waitForResult.complete(null);
            }

            @Override
            protected void onError(Exception error) {
                if (errorOccurred) {
                    return;
                }
                errorOccurred = true;
                waitForResult.completeExceptionally(error);
            }
        };
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }
}
