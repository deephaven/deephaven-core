//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.base.Pair;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.ShiftedColumnsFactory;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.select.SwitchColumn;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.PossiblyImmutableColumnSource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.util.InverseWrappedRowSetRowRedirection;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.WrappedRowSetRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseablePair;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SelectAndViewAnalyzer implements LogOutputAppendable {
    private static final Consumer<ColumnSource<?>> NOOP = ignore -> {
    };

    public enum Mode {
        VIEW_LAZY, VIEW_EAGER, SELECT_STATIC, SELECT_REFRESHING, SELECT_REDIRECTED_REFRESHING, SELECT_REDIRECTED_STATIC
    }
    public enum UpdateFlavor {
        Select, View, Update, UpdateView, LazyUpdate
    }

    public static void initializeSelectColumns(
            final Map<String, ColumnDefinition<?>> parentColumnMap,
            final SelectColumn[] selectColumns) {
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        initializeSelectColumns(parentColumnMap, selectColumns, compilationProcessor);
        compilationProcessor.compile();
    }

    public static void initializeSelectColumns(
            final Map<String, ColumnDefinition<?>> parentColumnMap,
            final SelectColumn[] selectColumns,
            final QueryCompilerRequestProcessor compilationProcessor) {
        final Map<String, ColumnDefinition<?>> targetColumnMap = new HashMap<>(parentColumnMap);
        for (SelectColumn column : selectColumns) {
            column.initDef(targetColumnMap, compilationProcessor);
            final ColumnDefinition<?> columnDefinition = ColumnDefinition.fromGenericType(
                    column.getName(), column.getReturnedType(), column.getReturnedComponentType());
            targetColumnMap.put(column.getName(), columnDefinition);
        }
    }

    public static AnalyzerContext createContext(
            final QueryTable parentTable,
            final Mode mode,
            final boolean publishParentSources,
            boolean useShiftedColumns,
            final SelectColumn... selectColumns) {
        final UpdateGraph updateGraph = parentTable.getUpdateGraph();

        final Map<String, ColumnSource<?>> columnSources = parentTable.getColumnSourceMap();
        final TrackingRowSet rowSet = parentTable.getRowSet();

        final boolean parentIsFlat = parentTable.isFlat();
        final boolean flatResult = !parentIsFlat
                && (columnSources.isEmpty() || !publishParentSources)
                && mode == Mode.SELECT_STATIC;
        final AnalyzerContext context = new AnalyzerContext(parentTable, publishParentSources, flatResult);

        final Map<String, ColumnDefinition<?>> columnDefinitions = new LinkedHashMap<>();
        final RowRedirection rowRedirection;
        if (mode == Mode.SELECT_REDIRECTED_STATIC) {
            rowRedirection = new InverseWrappedRowSetRowRedirection(rowSet);
        } else if (mode == Mode.SELECT_REDIRECTED_REFRESHING && rowSet.size() < Integer.MAX_VALUE) {
            final WritableRowRedirection writableRowRedirection =
                    WritableRowRedirection.FACTORY.createRowRedirection(rowSet.intSize());
            context.addLayer(new RedirectionLayer(context, rowSet, writableRowRedirection));
            rowRedirection = writableRowRedirection;
        } else {
            rowRedirection = null;
        }

        // First pass to initialize all columns and to compile formulas in one batch.
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        for (Map.Entry<String, ColumnSource<?>> entry : columnSources.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource<?> cs = entry.getValue();
            final ColumnDefinition<?> cd = ColumnDefinition.fromGenericType(name, cs.getType(), cs.getComponentType());
            columnDefinitions.put(name, cd);
        }

        final Set<String> resultColumnNames = new HashSet<>();
        for (final SelectColumn sc : selectColumns) {
            if (context.remainingCols != null) {
                context.remainingCols.add(sc);
                continue;
            }

            sc.initDef(columnDefinitions, compilationProcessor);
            final ColumnDefinition<?> cd = ColumnDefinition.fromGenericType(
                    sc.getName(), sc.getReturnedType(), sc.getReturnedComponentType());
            columnDefinitions.put(sc.getName(), cd);

            if (useShiftedColumns && hasConstantArrayAccess(sc)) {
                context.remainingCols = new LinkedList<>();
                context.shiftColumn = sc instanceof FormulaColumn
                        ? (FormulaColumn) sc
                        : (FormulaColumn) ((SwitchColumn) sc).getRealColumn();
                context.shiftColumnHasPositiveOffset = hasPositiveOffsetConstantArrayAccess(sc);
                continue;
            }

            // In our first pass, determine whether any columns will be preserved so that we don't prematurely flatten.
            final SourceColumn realColumn = tryToGetSourceColumn(sc);

            if (realColumn != null && !resultColumnNames.contains(realColumn.getSourceName())) {
                // if we are preserving a column, then we cannot change key space
                context.flatResult &= !shouldPreserve(columnSources.get(realColumn.getSourceName()));
            }

            // TODO (deephaven#5760): If layers may define more than one column, we'll need to add all of them here.
            resultColumnNames.add(sc.getName());

            context.processedCols.add(sc);
        }

        compilationProcessor.compile();

        // Second pass builds the analyzer and destination columns
        final HashMap<String, ColumnSource<?>> resultAlias = new HashMap<>();
        for (final SelectColumn sc : context.processedCols) {

            // if this select column depends on result column then its updates must happen in result-key-space
            // note: if flatResult is true then we are not preserving any parent columns
            final boolean useResultKeySpace = context.flatResult
                    && Stream.concat(sc.getColumns().stream(), sc.getColumnArrays().stream())
                            .anyMatch(columnName -> context.getLayerIndexFor(columnName) != Layer.PARENT_TABLE_INDEX);

            sc.initInputs(rowSet, useResultKeySpace ? context.allSourcesInResultKeySpace : context.allSources);

            // TODO (deephaven-core#5760): If layers may define more than one column, we'll need to fix resultAlias.
            // new columns shadow known aliases
            resultAlias.remove(sc.getName());

            final Stream<String> allDependencies =
                    Stream.concat(sc.getColumns().stream(), sc.getColumnArrays().stream());
            final String[] distinctDeps = allDependencies.distinct().toArray(String[]::new);
            final ModifiedColumnSet mcsBuilder = new ModifiedColumnSet(parentTable.getModifiedColumnSetForUpdates());

            if (useShiftedColumns && hasConstantArrayAccess(sc)) {
                // we use the first shifted column to split between processed columns and remaining columns
                throw new IllegalStateException("Found ShiftedColumn in processed column list");
            }

            // shifted columns appear to not be safe for refresh, so we do not validate them until they are rewritten
            // using the intermediary shifted column
            if (parentTable.isRefreshing()) {
                sc.validateSafeForRefresh(parentTable);
            }

            if (hasConstantValue(sc)) {
                final WritableColumnSource<?> constViewSource =
                        SingleValueColumnSource.getSingleValueColumnSource(sc.getReturnedType());
                context.addLayer(new ConstantColumnLayer(context, sc, constViewSource, distinctDeps, mcsBuilder));
                continue;
            }

            final SourceColumn realColumn = tryToGetSourceColumn(sc);
            if (realColumn != null) {
                if (shouldPreserve(sc.getDataView())) {
                    context.addLayer(new PreserveColumnLayer(context, sc, sc.getDataView(), distinctDeps, mcsBuilder));
                    continue;
                }
                // look for an existing alias that can be preserved instead
                final ColumnSource<?> alias = resultAlias.get(realColumn.getSourceName());
                if (alias != null) {
                    context.addLayer(new PreserveColumnLayer(context, sc, alias, distinctDeps, mcsBuilder));
                    continue;
                }
            }

            // if this is a SourceColumn, then results are eligible for aliasing
            final Consumer<ColumnSource<?>> maybeCreateAlias = realColumn == null ? NOOP
                    : cs -> resultAlias.put(realColumn.getSourceName(), cs);

            final long targetDestinationCapacity =
                    rowSet.isEmpty() ? 0 : (context.flatResult ? rowSet.size() : rowSet.lastRowKey() + 1);
            switch (mode) {
                case VIEW_LAZY: {
                    final ColumnSource<?> viewCs = sc.getLazyView();
                    maybeCreateAlias.accept(viewCs);
                    context.addLayer(new ViewColumnLayer(context, sc, viewCs, distinctDeps, mcsBuilder));
                    break;
                }
                case VIEW_EAGER: {
                    final ColumnSource<?> viewCs = sc.getDataView();
                    maybeCreateAlias.accept(viewCs);
                    context.addLayer(new ViewColumnLayer(context, sc, viewCs, distinctDeps, mcsBuilder));
                    break;
                }
                case SELECT_STATIC: {
                    // We need to call newDestInstance because only newDestInstance has the knowledge to endow our
                    // created array with the proper componentType (in the case of Vectors).
                    final WritableColumnSource<?> scs = parentIsFlat || context.flatResult
                            ? sc.newFlatDestInstance(targetDestinationCapacity)
                            : sc.newDestInstance(targetDestinationCapacity);
                    maybeSetStaticColumnSourceImmutable(scs);
                    maybeCreateAlias.accept(scs);
                    context.addLayer(new SelectColumnLayer(
                            updateGraph, rowSet, context, sc, scs, null, distinctDeps, mcsBuilder, false,
                            useResultKeySpace));
                    break;
                }
                case SELECT_REDIRECTED_STATIC: {
                    final WritableColumnSource<?> underlyingSource = sc.newDestInstance(rowSet.size());
                    final WritableColumnSource<?> scs = WritableRedirectedColumnSource.maybeRedirect(
                            rowRedirection, underlyingSource, rowSet.size());
                    maybeSetStaticColumnSourceImmutable(scs);
                    maybeCreateAlias.accept(scs);
                    context.addLayer(new SelectColumnLayer(
                            updateGraph, rowSet, context, sc, scs, underlyingSource, distinctDeps, mcsBuilder, true,
                            useResultKeySpace));
                    break;
                }
                case SELECT_REDIRECTED_REFRESHING:
                case SELECT_REFRESHING: {
                    // We need to call newDestInstance because only newDestInstance has the knowledge to endow our
                    // created array with the proper componentType (in the case of Vectors).
                    WritableColumnSource<?> scs = sc.newDestInstance(targetDestinationCapacity);
                    WritableColumnSource<?> underlyingSource = null;
                    if (rowRedirection != null) {
                        underlyingSource = scs;
                        scs = WritableRedirectedColumnSource.maybeRedirect(
                                rowRedirection, underlyingSource, rowSet.intSize());
                    }
                    maybeCreateAlias.accept(scs);
                    context.addLayer(new SelectColumnLayer(
                            updateGraph, rowSet, context, sc, scs, underlyingSource, distinctDeps, mcsBuilder,
                            rowRedirection != null, useResultKeySpace));
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported case " + mode);
            }
        }

        return context;
    }

    private static void maybeSetStaticColumnSourceImmutable(final ColumnSource<?> columnSource) {
        if (columnSource instanceof PossiblyImmutableColumnSource) {
            ((PossiblyImmutableColumnSource) columnSource).setImmutable();
        }
    }

    private static @Nullable SourceColumn tryToGetSourceColumn(final SelectColumn sc) {
        final SourceColumn realColumn;
        if (sc instanceof SourceColumn) {
            realColumn = (SourceColumn) sc;
        } else if ((sc instanceof SwitchColumn) && ((SwitchColumn) sc).getRealColumn() instanceof SourceColumn) {
            realColumn = (SourceColumn) ((SwitchColumn) sc).getRealColumn();
        } else {
            realColumn = null;
        }
        return realColumn;
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

    private static boolean shouldPreserve(final ColumnSource<?> columnSource) {
        return columnSource instanceof InMemoryColumnSource && ((InMemoryColumnSource) columnSource).isInMemory()
                && !Vector.class.isAssignableFrom(columnSource.getType());
    }

    /** The layers that make up this analyzer. */
    private final Layer[] layers;

    /** Whether the result should be flat. */
    private final boolean flatResult;

    private final BitSet requiredLayers = new BitSet();
    private final BitSet remainingLayers = new BitSet();

    private SelectAndViewAnalyzer(
            final Layer[] layers,
            final boolean flatResult) {
        this.layers = layers;
        this.flatResult = flatResult;
        for (final Layer layer : layers) {
            if (layer.hasRefreshingLogic()) {
                requiredLayers.set(layer.getLayerIndex());
            } else {
                this.layers[layer.getLayerIndex()] = null;
            }
        }
    }

    public final static class AnalyzerContext {

        /** The analyzer that we are building. */
        private final List<Layer> layers = new ArrayList<>();
        /**
         * The sources that are available to the analyzer, including parent columns. Parent columns are in parent key
         * space, others are in result key space.
         */
        private final Map<String, ColumnSource<?>> allSources = new LinkedHashMap<>();
        /** The sources that are available to the analyzer, including parent columns, in result key space. */
        private final Map<String, ColumnSource<?>> allSourcesInResultKeySpace;
        /** The sources that are published to the child table. */
        private final Map<String, ColumnSource<?>> publishedSources = new LinkedHashMap<>();
        /** A mapping from result column name to the layer index that created it. */
        private final TObjectIntMap<String> columnToLayerIndex;
        /** The select columns that have been processed so far. */
        private final List<SelectColumn> processedCols = new ArrayList<>();

        /** A holder for the shift column, if any. */
        private FormulaColumn shiftColumn;
        /** Whether the shift column has a positive offset. */
        private boolean shiftColumnHasPositiveOffset;
        /** The columns that will need to be processed after the shift column. */
        private List<SelectColumn> remainingCols;
        /** Whether the result should be flat. */
        private boolean flatResult;
        /** The layer that will be used to process redirection, if we have one. */
        private int redirectionLayer = Layer.UNSET_INDEX;

        AnalyzerContext(
                final QueryTable parentTable,
                final boolean publishParentSources,
                final boolean flatResult) {
            final Map<String, ColumnSource<?>> parentSources = parentTable.getColumnSourceMap();
            columnToLayerIndex = new TObjectIntHashMap<>(parentSources.size(), 0.5f, Layer.UNSET_INDEX);

            this.flatResult = flatResult;

            allSources.putAll(parentSources);
            for (final String columnName : allSources.keySet()) {
                columnToLayerIndex.put(columnName, Layer.PARENT_TABLE_INDEX);
            }

            if (publishParentSources) {
                publishedSources.putAll(parentSources);
            }

            if (!flatResult) {
                // result key space is the same as parent key space
                allSourcesInResultKeySpace = allSources;
            } else {
                allSourcesInResultKeySpace = new HashMap<>();

                final RowRedirection rowRedirection = new WrappedRowSetRowRedirection(parentTable.getRowSet());
                allSources.forEach((name, cs) -> allSourcesInResultKeySpace.put(name,
                        RedirectedColumnSource.maybeRedirect(rowRedirection, cs)));
            }
        }

        /**
         * Add a layer to the analyzer.
         *
         * @param layer the layer to add
         */
        void addLayer(final Layer layer) {
            if (layer instanceof RedirectionLayer) {
                if (redirectionLayer != Layer.UNSET_INDEX) {
                    throw new IllegalStateException("Cannot have more than one redirection layer");
                }
                redirectionLayer = layers.size();
            }

            layer.populateColumnSources(allSources);
            if (flatResult) {
                layer.populateColumnSources(allSourcesInResultKeySpace);
            }
            layer.populateColumnSources(publishedSources);

            layers.add(layer);

            for (final String columnName : layer.getLayerColumnNames()) {
                columnToLayerIndex.put(columnName, layer.getLayerIndex());
            }
        }

        /**
         * @return the next layerIndex to use
         */
        int getNextLayerIndex() {
            return layers.size();
        }

        /**
         * Return the layerIndex for a given string column.
         *
         * @param column the name of the column
         *
         * @return the layerIndex
         */
        int getLayerIndexFor(String column) {
            final int layerIndex = columnToLayerIndex.get(column);
            if (layerIndex == Layer.UNSET_INDEX) {
                throw new IllegalStateException("Column " + column + " not found in any layer of the analyzer");
            }
            return layerIndex;
        }

        /**
         * Populate the ModifiedColumnSet with all indirect/direct dependencies on the parent table.
         *
         * @param mcsBuilder the result ModifiedColumnSet to populate
         * @param dependencies the immediate dependencies
         */
        void populateParentDependenciesMCS(
                final ModifiedColumnSet mcsBuilder,
                final String[] dependencies) {
            for (final String dep : dependencies) {
                final int layerIndex = getLayerIndexFor(dep);
                if (layerIndex == Layer.PARENT_TABLE_INDEX) {
                    // this is a preserved parent column
                    mcsBuilder.setAll(dep);
                } else {
                    mcsBuilder.setAll(layers.get(layerIndex).getModifiedColumnSet());
                }
            }
        }

        /**
         * Populate the layer dependency set with the layer indices that the dependencies are in.
         *
         * @param layerDependencySet the result bitset to populate
         * @param dependencies the dependencies
         */
        void populateLayerDependencySet(
                final BitSet layerDependencySet,
                final String[] dependencies) {
            for (final String dep : dependencies) {
                final int layerIndex = getLayerIndexFor(dep);
                if (layerIndex != Layer.PARENT_TABLE_INDEX) {
                    // note that implicitly preserved columns do not belong to a layer.
                    layerDependencySet.or(layers.get(layerIndex).getLayerDependencySet());
                }
            }
        }

        /**
         * Set the redirection layer in the bitset if the analyzer has any redirection.
         *
         * @param layerDependencies the result bitset to populate
         */
        void setRedirectionLayer(final BitSet layerDependencies) {
            if (redirectionLayer != Layer.UNSET_INDEX) {
                layerDependencies.set(redirectionLayer);
            }
        }

        /**
         * @return the column sources that are published through the child table
         */
        public Map<String, ColumnSource<?>> getPublishedColumnSources() {
            // Note that if we have a shift column that we forcibly publish all columns.
            return shiftColumn == null ? publishedSources : allSources;
        }

        /**
         * @return the final analyzer
         */
        public SelectAndViewAnalyzer createAnalyzer() {
            return new SelectAndViewAnalyzer(layers.toArray(Layer[]::new), flatResult);
        }

        /**
         * @return which select columns were included in the result (not including the shift, or post-shift, columns)
         */
        public List<SelectColumn> getProcessedColumns() {
            return processedCols;
        }

        /**
         * @return whether the result should be flat
         */
        public boolean isFlatResult() {
            return flatResult;
        }

        /**
         * Our job here is to calculate the effects: a map from incoming column to a list of columns that it effects. We
         * do this in two stages. In the first stage we create a map from column to (set of dependent columns). In the
         * second stage we reverse that map.
         *
         * @return the effects map
         */
        public Map<String, String[]> calcEffects() {
            final Map<String, ColumnSource<?>> resultMap = getPublishedColumnSources();

            // Create the mapping from result column to dependent source columns.
            final Map<String, String[]> dependsOn = new HashMap<>();
            for (final String columnName : resultMap.keySet()) {
                final int layerIndex = getLayerIndexFor(columnName);
                final String[] dependencies;
                if (layerIndex == Layer.PARENT_TABLE_INDEX) {
                    dependencies = new String[] {columnName};
                } else {
                    dependencies = layers.get(layerIndex).getModifiedColumnSet().dirtyColumnNames();
                }
                dependsOn.put(columnName, dependencies);
            }

            // Now create the mapping from source column to result columns.
            final Map<String, List<String>> effects = new HashMap<>();
            for (Map.Entry<String, String[]> entry : dependsOn.entrySet()) {
                final String depender = entry.getKey();
                for (final String dependee : entry.getValue()) {
                    effects.computeIfAbsent(dependee, dummy -> new ArrayList<>()).add(depender);
                }
            }

            // Convert effects type into result type
            final Map<String, String[]> result = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : effects.entrySet()) {
                final String[] value = entry.getValue().toArray(String[]::new);
                result.put(entry.getKey(), value);
            }
            return result;
        }

        /**
         * Shift columns introduce intermediary table operations. This method applies remaining work to the result built
         * so far.
         *
         * @param parentTable the source table
         * @param resultSoFar the intermediate result
         * @param updateFlavor the update flavor
         * @return the final result
         */
        public QueryTable applyShiftsAndRemainingColumns(
                @NotNull final QueryTable parentTable,
                @NotNull QueryTable resultSoFar,
                final UpdateFlavor updateFlavor) {
            if (shiftColumn != null) {
                resultSoFar = (QueryTable) ShiftedColumnsFactory.getShiftedColumnsTable(
                        resultSoFar, shiftColumn, updateFlavor);
            }

            // shift columns may introduce modifies that are not present in the original table; set these before using
            if (parentTable.isRefreshing()) {
                if (shiftColumn == null && parentTable.isAddOnly()) {
                    resultSoFar.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                }
                if ((shiftColumn == null || !shiftColumnHasPositiveOffset) && parentTable.isAppendOnly()) {
                    // note if the shift offset is non-positive, then this result is still append-only
                    resultSoFar.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
                }
                if (parentTable.hasAttribute(Table.TEST_SOURCE_TABLE_ATTRIBUTE)) {
                    // be convenient for test authors by propagating the test source table attribute
                    resultSoFar.setAttribute(Table.TEST_SOURCE_TABLE_ATTRIBUTE, true);
                }
                if (parentTable.isBlink()) {
                    // blink tables, although possibly not useful, can have shift columns
                    resultSoFar.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
                }
            }

            boolean isMultiStateSelect = shiftColumn != null || remainingCols != null;
            if (isMultiStateSelect && (updateFlavor == UpdateFlavor.Select || updateFlavor == UpdateFlavor.View)) {
                List<SelectColumn> newResultColumns = new LinkedList<>();
                for (SelectColumn processed : processedCols) {
                    newResultColumns.add(new SourceColumn(processed.getName()));
                }
                if (shiftColumn != null) {
                    newResultColumns.add(new SourceColumn(shiftColumn.getName()));
                }
                if (remainingCols != null) {
                    newResultColumns.addAll(remainingCols);
                }

                if (updateFlavor == UpdateFlavor.Select) {
                    resultSoFar = (QueryTable) resultSoFar.select(newResultColumns);
                } else {
                    resultSoFar = (QueryTable) resultSoFar.view(newResultColumns);
                }
            } else if (remainingCols != null) {
                switch (updateFlavor) {
                    case Update: {
                        resultSoFar = (QueryTable) resultSoFar.update(remainingCols);
                        break;
                    }
                    case UpdateView: {
                        resultSoFar = (QueryTable) resultSoFar.updateView(remainingCols);
                        break;
                    }
                    case LazyUpdate: {
                        resultSoFar = (QueryTable) resultSoFar.lazyUpdate(remainingCols);
                        break;
                    }
                    default:
                        throw new IllegalStateException("Unexpected update flavor: " + updateFlavor);
                }
            }

            return resultSoFar;
        }
    }

    static abstract class Layer implements LogOutputAppendable {
        private static final BitSet EMPTY_BITSET = new BitSet();

        public static final int UNSET_INDEX = -1;
        public static final int PARENT_TABLE_INDEX = -2;

        /**
         * The layerIndex is used to identify each layer uniquely within the bitsets for completion.
         */
        private final int layerIndex;

        Layer(int layerIndex) {
            this.layerIndex = layerIndex;
        }

        /**
         * @return which index in the layer stack this layer is
         */
        int getLayerIndex() {
            return layerIndex;
        }

        /**
         * @return whether this layer has refreshing logic and needs to be updated
         */
        boolean hasRefreshingLogic() {
            return true;
        }

        /**
         * @return the modified column set of the parent table that this layer indirectly depends on
         */
        ModifiedColumnSet getModifiedColumnSet() {
            return failNoRefreshingLogic();
        }

        /**
         * @return the layer dependency set indicating which layers this layer depends on
         */
        BitSet getLayerDependencySet() {
            return EMPTY_BITSET;
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        void startTrackingPrev() {
            // default is that there is nothing to do
        }

        /**
         * @return the column names created by this layer
         */
        abstract Set<String> getLayerColumnNames();

        /**
         * Populate the column sources for this layer.
         *
         * @param result the map to populate
         */
        abstract void populateColumnSources(Map<String, ColumnSource<?>> result);

        /**
         * @return true if this layer allows parallelization across columns
         */
        abstract boolean allowCrossColumnParallelization();

        /**
         * Apply this update to this Layer.
         *
         * @param upstream the upstream update
         * @param toClear rows that used to exist and no longer exist
         * @param helper convenience class that memoizes reusable calculations for this update
         * @param jobScheduler scheduler for parallel sub-tasks
         * @param liveResultOwner {@link LivenessNode node} to be used to manage/unmanage results that happen to be
         *        {@link io.deephaven.engine.liveness.LivenessReferent liveness referents}
         * @param onSuccess called when the update completed successfully
         * @param onError called when the update failed
         */
        Runnable createUpdateHandler(
                TableUpdate upstream,
                RowSet toClear,
                UpdateHelper helper,
                JobScheduler jobScheduler,
                @Nullable LivenessNode liveResultOwner,
                Runnable onSuccess,
                Consumer<Exception> onError) {
            return failNoRefreshingLogic();
        }

        private <T> T failNoRefreshingLogic() {
            throw new UnsupportedOperationException(String.format(
                    "%s does not have any refreshing logic", this.getClass().getSimpleName()));
        }
    }

    public static class UpdateHelper implements SafeCloseable {
        private RowSet existingRows;
        private TableUpdate upstreamInResultSpace;
        private SafeCloseablePair<RowSet, RowSet> shiftedWithModifies;
        private SafeCloseablePair<RowSet, RowSet> shiftedWithoutModifies;

        private final RowSet parentRowSet;
        private final TableUpdate upstream;

        public UpdateHelper(RowSet parentRowSet, TableUpdate upstream) {
            this.parentRowSet = parentRowSet;
            this.upstream = upstream;
        }

        /**
         * Flatten the upstream update from the parent key space to the destination key space. We are guaranteed to be
         * in STATIC_SELECT mode.
         *
         * @return the flattened update
         */
        TableUpdate resultKeySpaceUpdate() {
            if (upstreamInResultSpace == null) {
                upstreamInResultSpace = new TableUpdateImpl(
                        RowSetFactory.flat(upstream.added().size()), RowSetFactory.empty(), RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            }
            return upstreamInResultSpace;
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
            if (upstreamInResultSpace != null) {
                upstreamInResultSpace.release();
                upstreamInResultSpace = null;
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
     * @param onSuccess called when the update completed successfully
     * @param onError called when the update failed
     */
    public void applyUpdate(
            final TableUpdate upstream,
            final RowSet toClear,
            final UpdateHelper helper,
            final JobScheduler jobScheduler,
            @Nullable final LivenessNode liveResultOwner,
            final Runnable onSuccess,
            final Consumer<Exception> onError) {

        Assert.assertion(remainingLayers.isEmpty(), "remainingLayers.isEmpty()");
        remainingLayers.or(requiredLayers);

        final Runnable[] runners = new Runnable[layers.length];
        final UpdateScheduler scheduler = new UpdateScheduler(runners, onSuccess, onError);

        for (int ii = 0; ii < layers.length; ++ii) {
            final Layer layer = layers[ii];
            if (layer != null) {
                // TODO (deephaven-core#4896): this error handling allows concurrent layers to fail without ensuring
                // that other tasks are finished.
                runners[ii] = layer.createUpdateHandler(
                        upstream, toClear, helper, jobScheduler, liveResultOwner,
                        () -> scheduler.onLayerComplete(layer.getLayerIndex()), onError);
            }
        }

        scheduler.tryToKickOffWork();
    }

    private class UpdateScheduler {
        private final ReentrantLock runLock = new ReentrantLock();

        private final Runnable[] runners;
        private final Runnable onSuccess;
        private final Consumer<Exception> onError;

        private volatile boolean needsRun;
        /** whether we have already invoked onSuccess */
        private boolean updateComplete;

        public UpdateScheduler(
                final Runnable[] runners,
                final Runnable onSuccess,
                final Consumer<Exception> onError) {
            this.runners = runners;
            this.onSuccess = onSuccess;
            this.onError = onError;
        }

        public void onLayerComplete(final int layerIndex) {
            synchronized (remainingLayers) {
                remainingLayers.set(layerIndex, false);
            }

            tryToKickOffWork();
        }

        private void tryToKickOffWork() {
            needsRun = true;
            while (true) {
                if (runLock.isHeldByCurrentThread() || !runLock.tryLock()) {
                    // do not permit re-entry or waiting on another thread doing exactly this work
                    return;
                }

                try {
                    if (needsRun) {
                        needsRun = false;
                        doKickOffWork();
                    }
                } catch (final Exception exception) {
                    try {
                        onError.accept(exception);
                    } catch (final Exception ignored) {
                    }
                } finally {
                    runLock.unlock();
                }

                if (!needsRun) {
                    return;
                }
            }
        }

        private void doKickOffWork() {
            if (updateComplete) {
                // we may have already completed the update, but are checking again due to the potential of a race
                return;
            }

            int nextLayer = 0;
            while (nextLayer >= 0) {
                boolean complete;
                boolean readyToFire = false;
                Runnable runner = null;
                synchronized (remainingLayers) {
                    complete = remainingLayers.isEmpty();
                    nextLayer = complete ? -1 : remainingLayers.nextSetBit(nextLayer);

                    if (nextLayer != -1) {
                        if ((runner = runners[nextLayer]) != null) {
                            readyToFire = !layers[nextLayer].getLayerDependencySet().intersects(remainingLayers);
                        }

                        if (readyToFire) {
                            runners[nextLayer] = null;
                        } else {
                            ++nextLayer;
                        }
                    }
                }

                if (readyToFire) {
                    runner.run();
                } else if (complete) {
                    updateComplete = true;
                    onSuccess.run();
                    return;
                }
            }
        }
    }

    public void startTrackingPrev() {
        for (final Layer layer : layers) {
            if (layer != null) {
                layer.startTrackingPrev();
            }
        }
    }

    /**
     * Is the result of this select/view flat?
     */
    public boolean flatResult() {
        return flatResult;
    }

    /**
     * Can all of our columns permit parallel updates?
     */
    public boolean allowCrossColumnParallelization() {
        return Arrays.stream(layers)
                .filter(Objects::nonNull)
                .allMatch(Layer::allowCrossColumnParallelization);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        logOutput = logOutput.append("SelectAndViewAnalyzer{");
        boolean first = true;
        for (final Layer layer : layers) {
            if (layer == null) {
                continue;
            }
            if (first) {
                first = false;
            } else {
                logOutput = logOutput.append(", ");
            }
            logOutput = logOutput.append(layer);

        }
        return logOutput.append("}");
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }
}
