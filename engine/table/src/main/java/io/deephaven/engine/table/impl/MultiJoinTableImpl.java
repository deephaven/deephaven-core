package io.deephaven.engine.table.impl;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.by.BitmapRandomBuilder;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.multijoin.IncrementalMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.multijoin.StaticMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.SingleValueRowRedirection;
import io.deephaven.engine.table.impl.util.WritableSingleValueRowRedirection;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker.*;

public class MultiJoinTableImpl implements MultiJoinTable {

    private static final int KEY_COLUMN_SENTINEL = -2;
    private final Table table;

    private final List<String> keyColumns;

    private static class MultiJoinInputHelper {
        Table table;

        /** The output column keys in the order provided by the MutiJoinInput */
        final String[] keyColumnNames;
        /** The input column keys in the order provided by the MutiJoinInput */
        final String[] originalKeyColumnNames;
        /** The output non-key columns in the order provided by the MutiJoinInput */
        final String[] addColumnNames;
        /** The input non-key columns in the order provided by the MutiJoinInput */
        final String[] originalAddColumnNames;

        final Map<String, ColumnSource<?>> keySourceMap;
        final Map<String, ColumnSource<?>> originalKeySourceMap;

        final JoinAddition[] columnsToAdd;

        MultiJoinInputHelper(@NotNull MultiJoinInput input) {
            table = input.inputTable().coalesce();

            final int matchCount = input.columnsToMatch().length;

            // Create the ordered list of input as well as the deterministic order from the hashmap.
            keyColumnNames = new String[matchCount];
            originalKeyColumnNames = new String[matchCount];

            keySourceMap = new HashMap<>(matchCount);
            originalKeySourceMap = new HashMap<>(matchCount);

            for (int ii = 0; ii < matchCount; ii++) {
                final JoinMatch jm = input.columnsToMatch()[ii];
                final String left = jm.left().name();
                final String right = jm.right().name();

                keyColumnNames[ii] = left;
                originalKeyColumnNames[ii] = right;

                keySourceMap.put(left, ReinterpretUtils.maybeConvertToPrimitive(table.getColumnSource(right)));
                originalKeySourceMap.put(left, table.getColumnSource(right));
            }

            // Create the lists of addition columns (or use every non-key column if unspecified).
            if (input.columnsToAdd().length == 0) {
                // Compare against the input table key column names (not output).
                final Set<String> keys = new HashSet<>(Arrays.asList(originalKeyColumnNames));
                // create them on the fly from the table
                columnsToAdd = input.inputTable().getDefinition().getColumnNames().stream()
                        .filter(cn -> !keys.contains(cn)).map(ColumnName::of)
                        .toArray(JoinAddition[]::new);
            } else {
                columnsToAdd = input.columnsToAdd();
            }

            addColumnNames = new String[columnsToAdd.length];
            originalAddColumnNames = new String[columnsToAdd.length];
            for (int ii = 0; ii < columnsToAdd.length; ii++) {
                final JoinAddition ja = columnsToAdd[ii];
                addColumnNames[ii] = ja.newColumn().name();
                originalAddColumnNames[ii] = ja.existingColumn().name();
            }
        }

        void assertCompatible(@NotNull MultiJoinTableImpl.MultiJoinInputHelper inputHelper, int tableNumber) {
            // Verify the key column names.
            if (!keySourceMap.keySet().equals(inputHelper.keySourceMap.keySet())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Key column mismatch for table %d, first table has key columns=%s, this table has %s",
                                tableNumber, inputHelper.keySourceMap.keySet(), keySourceMap.keySet()));
            }
            // Verify matching column types.
            final String[] keys = inputHelper.keyColumnNames;
            final Collection<Class<?>> currentColumnTypes =
                    Arrays.stream(keySources(keys)).map(ColumnSource::getType).collect(Collectors.toSet());
            final Collection<Class<?>> expectedColumnTypes =
                    Arrays.stream(inputHelper.keySources(keys)).map(ColumnSource::getType).collect(Collectors.toSet());
            if (!currentColumnTypes.equals(expectedColumnTypes)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Key column type mismatch for table %d, first table has key column types=%s, this table has %s",
                                tableNumber, expectedColumnTypes, currentColumnTypes));
            }
            // Verify matching column component types.
            final Collection<Class<?>> currentComponentTypes =
                    Arrays.stream(keySources(keys)).map(ColumnSource::getComponentType).collect(Collectors.toSet());
            final Collection<Class<?>> expectedComponentTypes = Arrays.stream(inputHelper.keySources(keys))
                    .map(ColumnSource::getComponentType).collect(Collectors.toSet());
            if (!currentComponentTypes.equals(expectedComponentTypes)) {
                throw new IllegalArgumentException(String.format(
                        "Key column component type mismatch for table %d, first table has key column component types=%s, this table has %s",
                        tableNumber, expectedComponentTypes, currentComponentTypes));
            }
        }

        ColumnSource<?>[] keySources(final String[] columns) {
            return Arrays.stream(columns).map(keySourceMap::get).toArray(ColumnSource<?>[]::new);
        }

        ColumnSource<?>[] keySources() {
            return keySources(keyColumnNames);
        }

        ColumnSource<?>[] originalKeySources(final String[] columns) {
            return Arrays.stream(columns).map(originalKeySourceMap::get).toArray(ColumnSource<?>[]::new);
        }

        ColumnSource<?>[] originalKeySources() {
            return originalKeySources(keyColumnNames);
        }
    }

    @VisibleForTesting
    static MultiJoinTableImpl of(@NotNull final JoinControl joinControl,
            @NotNull final MultiJoinInput... multiJoinInputs) {
        final Table[] tables = Arrays.stream(multiJoinInputs).map(MultiJoinInput::inputTable).toArray(Table[]::new);
        final UpdateGraph updateGraph = tables[0].getUpdateGraph(tables);

        final boolean refreshing = Arrays.stream(multiJoinInputs).anyMatch(ih -> ih.inputTable().isRefreshing());
        if (refreshing) {
            updateGraph.checkInitiateSerialTableOperation();
        }
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            return QueryPerformanceRecorder.withNugget("multiJoin",
                    () -> new MultiJoinTableImpl(joinControl, multiJoinInputs));
        }
    }

    static MultiJoinTableImpl of(@NotNull final MultiJoinInput... multiJoinInputs) {
        return of(new JoinControl(), multiJoinInputs);
    }

    /**
     * Get the output {@link Table table} from this multi-join table.
     *
     * @return The output {@link Table table}
     */
    public Table table() {
        return table;
    }

    @Override
    public Collection<String> keyColumns() {
        return keyColumns;
    }

    private MultiJoinTableImpl(@NotNull final JoinControl joinControl,
            @NotNull final MultiJoinInput... multiJoinInputs) {
        keyColumns = new ArrayList<>();

        // Create the join input helpers we'll use during the join creation phase.
        final MultiJoinInputHelper[] joinInputHelpers =
                Arrays.stream(multiJoinInputs).map(MultiJoinInputHelper::new).toArray(MultiJoinInputHelper[]::new);
        final TObjectIntHashMap<String> usedColumns =
                new TObjectIntHashMap<>(joinInputHelpers[0].columnsToAdd.length, 0.5f, -1);

        for (String keyColName : joinInputHelpers[0].keyColumnNames) {
            keyColumns.add(keyColName);
            usedColumns.put(keyColName, KEY_COLUMN_SENTINEL);
        }

        for (int ii = 1; ii < joinInputHelpers.length; ++ii) {
            // Verify this input table is compatible with the first table.
            joinInputHelpers[ii].assertCompatible(joinInputHelpers[0], ii);
        }

        // Verify the non-key output columns do not conflict.
        for (int ii = 0; ii < joinInputHelpers.length; ++ii) {
            MultiJoinInputHelper inputHelper = joinInputHelpers[ii];
            for (String columnName : inputHelper.addColumnNames) {
                final int previouslyUsed = usedColumns.put(columnName, ii);
                if (previouslyUsed != usedColumns.getNoEntryValue()) {
                    throw new IllegalArgumentException(String.format("Column %s defined in table %s and table %d",
                            columnName,
                            previouslyUsed == KEY_COLUMN_SENTINEL ? "key columns" : Integer.toString(previouslyUsed),
                            ii));
                }
            }
        }

        if (multiJoinInputs[0].columnsToMatch().length == 0) {
            table = doMultiJoinZeroKey(joinInputHelpers);
            return;
        }
        table = bucketedMultiJoin(joinControl, joinInputHelpers);
    }

    private Table bucketedMultiJoin(@NotNull final JoinControl joinControl,
            @NotNull final MultiJoinInputHelper[] joinInputHelpers) {

        final MultiJoinStateManager stateManager;
        final String[] firstKeyColumnNames = joinInputHelpers[0].keyColumnNames;

        // If any tables are refreshing, we must use a refreshing JoinManager.
        final boolean refreshing = Arrays.stream(joinInputHelpers).anyMatch(ih -> ih.table.isRefreshing());
        if (refreshing) {
            stateManager = TypedHasherFactory.make(IncrementalMultiJoinStateManagerTypedBase.class,
                    joinInputHelpers[0].keySources(),
                    joinInputHelpers[0].originalKeySources(),
                    joinControl.initialBuildSize(), joinControl.getMaximumLoadFactor(),
                    joinControl.getTargetLoadFactor());
        } else {
            stateManager = TypedHasherFactory.make(StaticMultiJoinStateManagerTypedBase.class,
                    joinInputHelpers[0].keySources(),
                    joinInputHelpers[0].originalKeySources(),
                    joinControl.initialBuildSize(), joinControl.getMaximumLoadFactor(),
                    joinControl.getTargetLoadFactor());
        }
        stateManager.setMaximumLoadFactor(joinControl.getMaximumLoadFactor());
        stateManager.setTargetLoadFactor(joinControl.getTargetLoadFactor());
        stateManager.ensureTableCapacity(joinInputHelpers.length);

        for (int tableNumber = 0; tableNumber < joinInputHelpers.length; ++tableNumber) {
            stateManager.build(
                    joinInputHelpers[tableNumber].table,
                    joinInputHelpers[tableNumber].keySources(firstKeyColumnNames),
                    tableNumber);
        }

        final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>();

        final ColumnSource<?>[] keyHashTableSources = stateManager.getKeyHashTableSources();
        final ColumnSource<?>[] originalColumns = joinInputHelpers[0].originalKeySources();

        // We are careful to add the output key columns in the order of the first table input.
        for (int cc = 0; cc < keyColumns.size(); ++cc) {
            if (originalColumns[cc].getType() != keyHashTableSources[cc].getType()) {
                resultSources.put(keyColumns.get(cc),
                        ReinterpretUtils.convertToOriginalType(originalColumns[cc], keyHashTableSources[cc]));
            } else {
                resultSources.put(keyColumns.get(cc), keyHashTableSources[cc]);
            }
        }

        for (int tableNumber = 0; tableNumber < joinInputHelpers.length; ++tableNumber) {
            final RowRedirection rowRedirection = stateManager.getRowRedirectionForTable(tableNumber);
            if (refreshing) {
                ((IncrementalMultiJoinStateManagerTypedBase) stateManager)
                        .startTrackingPrevRedirectionValues(tableNumber);
            }
            final MultiJoinInputHelper inputHelper = joinInputHelpers[tableNumber];
            for (final JoinAddition ja : inputHelper.columnsToAdd) {
                resultSources.put(ja.newColumn().name(), RedirectedColumnSource.alwaysRedirect(rowRedirection,
                        inputHelper.table.getColumnSource(ja.existingColumn().name())));
            }
        }

        final QueryTable result =
                new QueryTable(RowSetFactory.flat(stateManager.getResultSize()).toTracking(), resultSources);

        if (refreshing) {
            final ModifiedColumnSet[] resultModifiedColumnSet = new ModifiedColumnSet[joinInputHelpers.length];
            final List<MultiJoinListenerRecorder> listenerRecorders = new ArrayList<>();

            final MergedListener mergedListener = new MultiJoinMergedListener(
                    (IncrementalMultiJoinStateManagerTypedBase) stateManager,
                    listenerRecorders,
                    Collections.emptyList(),
                    "multiJoin(" + keyColumns + ")",
                    result,
                    resultModifiedColumnSet);

            for (int ii = 0; ii < joinInputHelpers.length; ++ii) {
                final MultiJoinInputHelper inputHelper = joinInputHelpers[ii];
                if (inputHelper.table.isRefreshing()) {
                    final QueryTable input = (QueryTable) inputHelper.table;
                    final ColumnSource<?>[] keySources = inputHelper.keySources(firstKeyColumnNames);

                    final ModifiedColumnSet sourceKeyModifiedColumnSet =
                            input.newModifiedColumnSet(inputHelper.originalKeyColumnNames);
                    final ModifiedColumnSet sourceAdditionModifiedColumnSet =
                            input.newModifiedColumnSet(inputHelper.originalAddColumnNames);

                    resultModifiedColumnSet[ii] =
                            result.newModifiedColumnSet(inputHelper.addColumnNames);
                    final MatchPair[] pairs = MatchPair.fromAddition(Arrays.asList(inputHelper.columnsToAdd));
                    final ModifiedColumnSet.Transformer transformer =
                            input.newModifiedColumnSetTransformer(result, pairs);

                    final MultiJoinListenerRecorder listenerRecorder =
                            new MultiJoinListenerRecorder("multiJoin(" + ii + ")", input, mergedListener, keySources,
                                    sourceKeyModifiedColumnSet, sourceAdditionModifiedColumnSet, transformer, ii);
                    input.addUpdateListener(listenerRecorder);
                    listenerRecorders.add(listenerRecorder);
                }
            }
            result.addParentReference(mergedListener);
        }

        return result;
    }

    private static Table doMultiJoinZeroKey(@NotNull final MultiJoinInputHelper[] joinInputHelpers) {
        final SingleValueRowRedirection[] redirections = new SingleValueRowRedirection[joinInputHelpers.length];
        final boolean refreshing = Arrays.stream(joinInputHelpers).anyMatch(ih -> ih.table.isRefreshing());

        final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>();
        boolean hasResults = false;
        for (int tableNumber = 0; tableNumber < joinInputHelpers.length; ++tableNumber) {
            final MultiJoinInputHelper inputHelper = joinInputHelpers[tableNumber];
            final Table inputTable = inputHelper.table;

            final long key;
            if (inputTable.size() == 0) {
                key = NULL_ROW_KEY;
            } else if (inputTable.size() == 1) {
                key = inputTable.getRowSet().firstRowKey();
                hasResults = true;
            } else {
                throw new IllegalStateException("Duplicate rows for table " + tableNumber + " on zero-key multiJoin.");
            }

            final SingleValueRowRedirection rowRedirection;
            if (refreshing) {
                rowRedirection = new WritableSingleValueRowRedirection(key);
                rowRedirection.writableSingleValueCast().startTrackingPrevValues();
            } else {
                rowRedirection = new SingleValueRowRedirection(key);
            }
            redirections[tableNumber] = rowRedirection;

            for (final JoinAddition ja : inputHelper.columnsToAdd) {
                resultSources.put(ja.newColumn().name(), RedirectedColumnSource.alwaysRedirect(rowRedirection,
                        inputTable.getColumnSource(ja.existingColumn().name())));
            }
        }

        final QueryTable result = new QueryTable(RowSetFactory.flat(hasResults ? 1 : 0).toTracking(), resultSources);

        if (refreshing) {
            final ModifiedColumnSet[] resultModifiedColumnSet = new ModifiedColumnSet[joinInputHelpers.length];
            final List<MultiJoinListenerRecorder> listenerRecorders = new ArrayList<>();

            final MergedListener mergedListener = new MultiJoinZeroKeyMergedListener(
                    listenerRecorders,
                    Collections.emptyList(),
                    "multiJoin()",
                    result,
                    resultModifiedColumnSet,
                    redirections);

            for (int ii = 0; ii < joinInputHelpers.length; ++ii) {
                final MultiJoinInputHelper inputHelper = joinInputHelpers[ii];
                if (inputHelper.table.isRefreshing()) {
                    final QueryTable input = (QueryTable) inputHelper.table;

                    final ModifiedColumnSet sourceAdditionModifiedColumnSet =
                            input.newModifiedColumnSet(inputHelper.originalAddColumnNames);

                    resultModifiedColumnSet[ii] =
                            result.newModifiedColumnSet(inputHelper.addColumnNames);
                    final MatchPair[] pairs = MatchPair.fromAddition(Arrays.asList(inputHelper.columnsToAdd));
                    final ModifiedColumnSet.Transformer transformer =
                            input.newModifiedColumnSetTransformer(result, pairs);

                    final MultiJoinListenerRecorder listenerRecorder = new MultiJoinListenerRecorder(
                            "multiJoin(" + ii + ")", input, mergedListener, null, null, sourceAdditionModifiedColumnSet,
                            transformer, ii);
                    input.addUpdateListener(listenerRecorder);
                    listenerRecorders.add(listenerRecorder);
                }
            }
            result.addParentReference(mergedListener);
        }

        return result;
    }

    private static class MultiJoinListenerRecorder extends ListenerRecorder {
        private final ColumnSource<?>[] keyColumns;
        private final ModifiedColumnSet sourceKeyModifiedColumnSet;
        private final ModifiedColumnSet sourceAdditionModifiedColumnSet;
        private final ModifiedColumnSet.Transformer transformer;
        private final int tableNumber;

        public MultiJoinListenerRecorder(@NotNull final String description,
                @NotNull final QueryTable parent,
                @NotNull final MergedListener dependent,
                final ColumnSource<?>[] keyColumns,
                final ModifiedColumnSet sourceKeyModifiedColumnSet,
                final ModifiedColumnSet sourceAdditionModifiedColumnSet,
                @NotNull final ModifiedColumnSet.Transformer transformer,
                final int tableNumber) {
            super(description, parent, dependent);
            this.keyColumns = keyColumns;
            this.sourceKeyModifiedColumnSet = sourceKeyModifiedColumnSet;
            this.sourceAdditionModifiedColumnSet = sourceAdditionModifiedColumnSet;
            this.transformer = transformer;
            this.tableNumber = tableNumber;

            setMergedListener(dependent);
        }

        @Override
        public Table getParent() {
            return super.getParent();
        }
    }

    private static class MultiJoinMergedListener extends MergedListener {
        private final IncrementalMultiJoinStateManagerTypedBase stateManager;
        private final List<MultiJoinListenerRecorder> recorders;
        private final ModifiedColumnSet[] modifiedColumnSets;
        private final MultiJoinModifiedSlotTracker slotTracker = new MultiJoinModifiedSlotTracker();

        protected MultiJoinMergedListener(@NotNull final IncrementalMultiJoinStateManagerTypedBase stateManager,
                @NotNull final List<MultiJoinListenerRecorder> recorders,
                @NotNull final Collection<NotificationQueue.Dependency> dependencies,
                @NotNull final String listenerDescription,
                @NotNull final QueryTable result,
                @NotNull final ModifiedColumnSet[] modifiedColumnSets) {
            super(recorders, dependencies, listenerDescription, result);
            this.stateManager = stateManager;
            this.recorders = recorders;
            this.modifiedColumnSets = modifiedColumnSets;
        }

        @Override
        protected void process() {
            final int tableCount = stateManager.getTableCount();

            slotTracker.clear();
            slotTracker.ensureTableCapacity(tableCount);

            final long originalSize = stateManager.getResultSize();

            for (MultiJoinListenerRecorder recorder : recorders) {
                if (recorder.recordedVariablesAreValid()) {
                    final boolean keysModified = recorder.getModified().isNonempty()
                            && recorder.getModifiedColumnSet().containsAny(recorder.sourceKeyModifiedColumnSet);

                    if (recorder.getRemoved().isNonempty()) {
                        stateManager.processRemoved(recorder.getRemoved(), recorder.keyColumns, recorder.tableNumber,
                                slotTracker, FLAG_REMOVE);
                    }
                    if (keysModified) {
                        stateManager.processRemoved(recorder.getModifiedPreShift(), recorder.keyColumns,
                                recorder.tableNumber, slotTracker, FLAG_MODIFY);
                    }

                    if (recorder.getShifted().nonempty()) {
                        try (final WritableRowSet previousToShift = recorder.getParent().getRowSet().copyPrev()) {
                            previousToShift.remove(recorder.getRemoved());
                            if (keysModified) {
                                previousToShift.remove(recorder.getModifiedPreShift());
                            }
                            stateManager.processShifts(previousToShift, recorder.getShifted(), recorder.keyColumns,
                                    recorder.tableNumber,
                                    slotTracker);
                        }
                    }

                    if (!keysModified && recorder.getModified().isNonempty()) {
                        // If none of our input columns changed, we have no modifications to pass downstream.
                        if (recorder.getModifiedColumnSet().containsAny(recorder.sourceAdditionModifiedColumnSet)) {
                            stateManager.processModified(recorder.getModified(), recorder.keyColumns,
                                    recorder.tableNumber,
                                    slotTracker, FLAG_MODIFY);
                        }
                    }

                    if (keysModified) {
                        stateManager.processAdded(recorder.getModified(), recorder.keyColumns, recorder.tableNumber,
                                slotTracker, FLAG_MODIFY);
                    }

                    if (recorder.getAdded().isNonempty()) {
                        stateManager.processAdded(recorder.getAdded(), recorder.keyColumns, recorder.tableNumber,
                                slotTracker, FLAG_ADD);
                    }
                }
            }

            final long newSize = stateManager.getResultSize();

            final TableUpdateImpl downstream = new TableUpdateImpl();


            if (newSize > originalSize) {
                downstream.added = RowSetFactory.fromRange(originalSize, newSize - 1);
            } else {
                downstream.added = RowSetFactory.empty();
            }

            final long[] currentRedirections = new long[tableCount];
            final boolean[] modifiedTables = new boolean[tableCount];
            final boolean[] modifiedTablesOnThisRow = new boolean[tableCount];

            final RowSetBuilderRandom modifiedBuilder = new BitmapRandomBuilder((int) newSize);
            final RowSetBuilderRandom emptiedBuilder = RowSetFactory.builderRandom();
            final RowSetBuilderRandom reincarnatedBuilder = RowSetFactory.builderRandom();

            downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            downstream.modifiedColumnSet.clear();

            final byte notShift = (FLAG_ADD | FLAG_REMOVE | FLAG_MODIFY);
            final byte addOrRemove = (FLAG_ADD | FLAG_REMOVE);

            slotTracker.forAllModifiedSlots((row, previousRedirections, flagValues) -> {
                if (row >= originalSize) {
                    return;
                }
                stateManager.getCurrentRedirections(row, currentRedirections);
                boolean allNull = true;
                boolean rowModified = false;
                int numberOfOriginalNulls = 0;
                Arrays.fill(modifiedTablesOnThisRow, false);
                for (int tableNumber = 0; tableNumber < tableCount; ++tableNumber) {
                    if (currentRedirections[tableNumber] != NULL_ROW_KEY) {
                        allNull = false;
                    }
                    if (previousRedirections[tableNumber] == NULL_ROW_KEY) {
                        numberOfOriginalNulls++;
                    }
                    if (previousRedirections[tableNumber] == MultiJoinModifiedSlotTracker.SENTINEL_UNINITIALIZED_KEY) {
                        if (currentRedirections[tableNumber] == NULL_ROW_KEY) {
                            // This slot was previously deleted and may need to be reincarnated. This redirection
                            // previously and currently points to no row and should be considered previously null
                            // for this purpose.
                            numberOfOriginalNulls++;
                        } else {
                            rowModified |= (flagValues[tableNumber] & notShift) != 0;
                        }
                    } else {
                        // If the redirection has changed and we have done anything other than a shift, we must light
                        // up all the columns for the table as a modification. Similarly, if the row was added and
                        // deleted from the original table, then we must also light up all the columns as modified.
                        if ((flagValues[tableNumber] & addOrRemove) != 0) {
                            rowModified |= (modifiedTablesOnThisRow[tableNumber] = true);
                        } else if (currentRedirections[tableNumber] != previousRedirections[tableNumber]) {
                            rowModified |=
                                    (modifiedTablesOnThisRow[tableNumber] = (flagValues[tableNumber] & notShift) != 0);
                        } else {
                            rowModified |= (flagValues[tableNumber] & notShift) != 0;
                        }
                    }
                }
                if (allNull) {
                    emptiedBuilder.addKey(row);
                } else if (numberOfOriginalNulls == currentRedirections.length) {
                    reincarnatedBuilder.addKey(row);
                } else if (rowModified) {
                    modifiedBuilder.addKey(row);

                    for (int ii = 0; ii < currentRedirections.length; ++ii) {
                        if (modifiedTablesOnThisRow[ii]) {
                            modifiedTables[ii] = true;
                        }
                    }
                }
            });

            downstream.modified = modifiedBuilder.build();
            if (downstream.modified.isEmpty()) {
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else {
                int listenerIndex = 0;
                for (int ii = 0; ii < modifiedTables.length; ++ii) {
                    MultiJoinListenerRecorder recorder = listenerIndex >= recorders.size()
                            ? null
                            : recorders.get(listenerIndex);
                    if (recorder == null || recorder.tableNumber != ii) {
                        // We have a static table, ignore it because it cannot modify anything.
                        continue;
                    }
                    if (modifiedTables[ii]) {
                        // If this table had any rows that moved slots, or any removed/added slots, _all_ its columns
                        // are modified.
                        downstream.modifiedColumnSet.setAll(modifiedColumnSets[ii]);
                    } else if (recorder.getModified().isNonempty()) {
                        // If we have "in-place" modifications (same row, in the same slot), we need only mark the
                        // _modified_ columns from this table modified in the downstream.
                        recorder.transformer.transform(recorder.getModifiedColumnSet(), downstream.modifiedColumnSet);
                    }
                    listenerIndex++;
                }
            }
            downstream.removed = emptiedBuilder.build();
            downstream.added.writableCast().insert(reincarnatedBuilder.build());

            downstream.shifted = RowSetShiftData.EMPTY;

            if (!downstream.empty()) {
                result.getRowSet().writableCast().update(downstream.added, downstream.removed);
                result.notifyListeners(downstream);
            }
        }
    }

    private static class MultiJoinZeroKeyMergedListener extends MergedListener {
        private final List<MultiJoinListenerRecorder> recorders;
        private final ModifiedColumnSet[] modifiedColumnSets;
        private final SingleValueRowRedirection[] redirections;

        protected MultiJoinZeroKeyMergedListener(@NotNull final List<MultiJoinListenerRecorder> recorders,
                @NotNull final Collection<NotificationQueue.Dependency> dependencies,
                @NotNull final String listenerDescription,
                @NotNull final QueryTable result,
                @NotNull final ModifiedColumnSet[] modifiedColumnSets,
                @NotNull final SingleValueRowRedirection[] redirections) {
            super(recorders, dependencies, listenerDescription, result);
            this.recorders = recorders;
            this.modifiedColumnSets = modifiedColumnSets;
            this.redirections = redirections;
        }

        @Override
        protected void process() {
            final TableUpdateImpl downstream = new TableUpdateImpl();
            downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            downstream.shifted = RowSetShiftData.EMPTY;

            boolean resultModified = false;

            for (MultiJoinListenerRecorder recorder : recorders) {
                if (recorder.recordedVariablesAreValid()) {
                    if (recorder.getRemoved().isNonempty() || recorder.getAdded().isNonempty()) {
                        if (recorder.getParent().size() > 1) {
                            throw new IllegalStateException(
                                    "Multiple rows in " + recorder.tableNumber + " for zero-key multiJoin.");
                        }
                        resultModified = true;
                        redirections[recorder.tableNumber].writableSingleValueCast()
                                .setValue(recorder.getParent().getRowSet().firstRowKey());
                        downstream.modifiedColumnSet.setAll(modifiedColumnSets[recorder.tableNumber]);
                    } else if (recorder.getModified().isNonempty()) {
                        // If none of our input columns changed, we have no modifications to pass downstream.
                        if (recorder.getModifiedColumnSet().containsAny(recorder.sourceAdditionModifiedColumnSet)) {
                            resultModified = true;
                            recorder.transformer.transform(recorder.getModifiedColumnSet(),
                                    downstream.modifiedColumnSet);
                        }
                        redirections[recorder.tableNumber].writableSingleValueCast()
                                .setValue(recorder.getParent().getRowSet().firstRowKey());
                    } else if (recorder.getShifted().nonempty()) {
                        redirections[recorder.tableNumber].writableSingleValueCast()
                                .setValue(recorder.getParent().getRowSet().firstRowKey());
                    }
                }
            }

            final boolean hasResults = Arrays.stream(redirections).anyMatch(rd -> rd.getValue() != NULL_ROW_KEY);
            if (hasResults && result.size() == 0) {
                result.getRowSet().writableCast().insert(0);
                downstream.added = RowSetFactory.flat(1);
                downstream.removed = RowSetFactory.empty();
                downstream.modified = RowSetFactory.empty();
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else if (!hasResults && result.size() == 1) {
                result.getRowSet().writableCast().remove(0);
                downstream.added = RowSetFactory.empty();
                downstream.removed = RowSetFactory.flat(1);
                downstream.modified = RowSetFactory.empty();
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else {
                downstream.added = RowSetFactory.empty();
                downstream.removed = RowSetFactory.empty();
                if (resultModified) {
                    downstream.modified = RowSetFactory.flat(1);
                } else {
                    // this would be a useless update
                    return;
                }
            }

            if (!downstream.empty()) {
                result.notifyListeners(downstream);
            }
        }
    }
}
