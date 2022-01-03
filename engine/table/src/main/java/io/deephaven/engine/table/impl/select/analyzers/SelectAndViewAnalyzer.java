package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.TableMapTransformThreadPool;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.select.SwitchColumn;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseablePair;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

public abstract class SelectAndViewAnalyzer implements LogOutputAppendable {
    public enum Mode {
        VIEW_LAZY, VIEW_EAGER, SELECT_STATIC, SELECT_REFRESHING, SELECT_REDIRECTED_REFRESHING
    }

    public static SelectAndViewAnalyzer create(Mode mode, Map<String, ColumnSource<?>> columnSources,
            TrackingRowSet rowSet, ModifiedColumnSet parentMcs, boolean publishTheseSources,
            SelectColumn... selectColumns) {
        SelectAndViewAnalyzer analyzer = createBaseLayer(columnSources, publishTheseSources);
        final Map<String, ColumnDefinition<?>> columnDefinitions = new LinkedHashMap<>();
        final WritableRowRedirection rowRedirection;
        if (mode == Mode.SELECT_REDIRECTED_REFRESHING && rowSet.size() < Integer.MAX_VALUE) {
            rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(rowSet.intSize());
            analyzer = analyzer.createRedirectionLayer(rowSet, rowRedirection);
        } else {
            rowRedirection = null;
        }

        for (final SelectColumn sc : selectColumns) {
            final Map<String, ColumnSource<?>> columnsOfInterest = analyzer.getAllColumnSources();
            analyzer.updateColumnDefinitionsFromTopLayer(columnDefinitions);
            sc.initDef(columnDefinitions);
            sc.initInputs(rowSet, columnsOfInterest);
            final Stream<String> allDependencies =
                    Stream.concat(sc.getColumns().stream(), sc.getColumnArrays().stream());
            final String[] distinctDeps = allDependencies.distinct().toArray(String[]::new);
            final ModifiedColumnSet mcsBuilder = new ModifiedColumnSet(parentMcs);

            if (sc instanceof SourceColumn
                    || (sc instanceof SwitchColumn && ((SwitchColumn) sc).getRealColumn() instanceof SourceColumn)) {
                final ColumnSource<?> sccs = sc.getDataView();
                if ((sccs instanceof SparseArrayColumnSource || sccs instanceof ArrayBackedColumnSource)
                        && !Vector.class.isAssignableFrom(sc.getReturnedType())) {
                    analyzer = analyzer.createLayerForPreserve(sc.getName(), sc, sc.getDataView(), distinctDeps,
                            mcsBuilder);
                    continue;
                }
            }

            final long targetSize = rowSet.isEmpty() ? 0 : rowSet.lastRowKey() + 1;
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
                    final WritableColumnSource<?> scs = sc.newDestInstance(targetSize);
                    analyzer =
                            analyzer.createLayerForSelect(sc.getName(), sc, scs, null, distinctDeps, mcsBuilder, false);
                    break;
                }
                case SELECT_REDIRECTED_REFRESHING:
                case SELECT_REFRESHING: {
                    // We need to call newDestInstance because only newDestInstance has the knowledge to endow our
                    // created array with the proper componentType (in the case of Vectors).
                    // TODO(kosak): use DeltaAwareColumnSource
                    WritableColumnSource<?> scs = sc.newDestInstance(targetSize);
                    WritableColumnSource<?> underlyingSource = null;
                    if (rowRedirection != null) {
                        underlyingSource = scs;
                        scs = new WritableRedirectedColumnSource<>(rowRedirection, underlyingSource, rowSet.intSize());
                    }
                    analyzer = analyzer.createLayerForSelect(sc.getName(), sc, scs, underlyingSource, distinctDeps,
                            mcsBuilder, rowRedirection != null);
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported case " + mode);
            }
        }
        return analyzer;
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
     * Set the bits in bitset that represent all of the new columns, this is used to identify when the select or update
     * operaiton is complete
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

    private SelectAndViewAnalyzer createLayerForSelect(String name, SelectColumn sc,
            WritableColumnSource<?> cs, WritableColumnSource<?> underlyingSource,
            String[] parentColumnDependencies, ModifiedColumnSet mcsBuilder, boolean isRedirected) {
        return new SelectColumnLayer(this, name, sc, cs, underlyingSource, parentColumnDependencies, mcsBuilder,
                isRedirected);
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
     * @param onCompletion called when an inner column is complete, the outer layer should pass the completion on to
     *        other layers and if it all of it's dependencies have been satisfied schedule execution of that column
     *        update
     */
    public abstract void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper,
            JobScheduler jobScheduler, SelectLayerCompletionHandler onCompletion);

    /**
     * Our job here is to calculate the effects: a map from incoming column to a list of columns that it effects. We do
     * this in two stages. In the first stage we create a map from column to (set of dependent columns). In the second
     * stage we reverse that map.
     */
    public final Map<String, String[]> calcEffects() {
        final Map<String, Set<String>> dependsOn = calcDependsOnRecurse();

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

    abstract Map<String, Set<String>> calcDependsOnRecurse();

    public abstract SelectAndViewAnalyzer getInner();

    public abstract void updateColumnDefinitionsFromTopLayer(Map<String, ColumnDefinition<?>> columnDefinitions);

    public abstract void startTrackingPrev();

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

        SelectLayerCompletionHandler(BitSet requiredColumns, SelectLayerCompletionHandler nextHandler) {
            this.completedColumns = nextHandler.completedColumns;
            this.nextHandler = nextHandler;
            this.requiredColumns = requiredColumns;
        }

        public SelectLayerCompletionHandler(BitSet completedColumns, BitSet requiredColumns) {
            this.completedColumns = completedColumns;
            this.nextHandler = null;
            this.requiredColumns = requiredColumns;
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
                final boolean readyToFire;
                synchronized (completedColumns) {
                    completedColumns.set(completedColumn);
                    if (requiredColumns.get(completedColumn)) {
                        readyToFire = !fired && requiredColumns.stream().allMatch(completedColumns::get);
                        if (readyToFire) {
                            fired = true;
                        }
                    } else {
                        readyToFire = false;
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
     * An interface for submitting jobs to be executed and accumulating their performance.
     */
    public interface JobScheduler {
        /**
         * Cause runnable to be executed.
         *
         * @param runnable the runnable to execute
         * @param description a description for logging
         * @param onError a routine to call if an exception occurs while running runnable
         */
        void submit(Runnable runnable, final LogOutputAppendable description, final Consumer<Exception> onError);

        /**
         * The performance statistics of runnable, or null if it was executed in the current thread.
         */
        UpdatePerformanceTracker.SubEntry getAccumulatedPerformance();
    }

    public static class UpdateGraphProcessorJobScheduler implements SelectAndViewAnalyzer.JobScheduler {
        final UpdatePerformanceTracker.SubEntry accumulatedSubEntry = new UpdatePerformanceTracker.SubEntry();

        @Override
        public void submit(final Runnable runnable, final LogOutputAppendable description,
                final Consumer<Exception> onError) {
            UpdateGraphProcessor.DEFAULT.addNotification(new AbstractNotification(false) {
                @Override
                public boolean canExecute(long step) {
                    return true;
                }

                @Override
                public void run() {
                    final UpdatePerformanceTracker.SubEntry subEntry = new UpdatePerformanceTracker.SubEntry();
                    subEntry.onSubEntryStart();
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        onError.accept(e);
                    } finally {
                        subEntry.onSubEntryEnd();
                        synchronized (accumulatedSubEntry) {
                            accumulatedSubEntry.accumulate(subEntry);
                        }
                    }
                }

                @Override
                public LogOutput append(LogOutput output) {
                    return output.append("{Notification(").append(System.identityHashCode(this)).append(" for ")
                            .append(description).append("}");
                }
            });
        }

        @Override
        public UpdatePerformanceTracker.SubEntry getAccumulatedPerformance() {
            return accumulatedSubEntry;
        }
    }

    public static class TableMapTransformJobScheduler implements SelectAndViewAnalyzer.JobScheduler {
        final UpdatePerformanceTracker.SubEntry accumulatedSubEntry = new UpdatePerformanceTracker.SubEntry();

        @Override
        public void submit(final Runnable runnable, final LogOutputAppendable description,
                final Consumer<Exception> onError) {
            TableMapTransformThreadPool.executorService.submit(() -> {
                final UpdatePerformanceTracker.SubEntry subEntry = new UpdatePerformanceTracker.SubEntry();
                subEntry.onSubEntryStart();
                try {
                    runnable.run();
                } catch (Exception e) {
                    onError.accept(e);
                } finally {
                    subEntry.onSubEntryEnd();
                    synchronized (accumulatedSubEntry) {
                        accumulatedSubEntry.accumulate(subEntry);
                    }
                }
            });
        }

        @Override
        public UpdatePerformanceTracker.SubEntry getAccumulatedPerformance() {
            return accumulatedSubEntry;
        }
    }

    public static class ImmediateJobScheduler implements SelectAndViewAnalyzer.JobScheduler {
        public static final ImmediateJobScheduler INSTANCE = new ImmediateJobScheduler();

        @Override
        public void submit(final Runnable runnable, final LogOutputAppendable description,
                final Consumer<Exception> onError) {
            try {
                runnable.run();
            } catch (Exception e) {
                onError.accept(e);
            }
        }

        @Override
        public UpdatePerformanceTracker.SubEntry getAccumulatedPerformance() {
            return null;
        }
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

        return new SelectLayerCompletionHandler(completedColumns, requiredColumns) {
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
