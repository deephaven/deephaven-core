package io.deephaven.engine.table.impl;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.multijoin.IncrementalMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.multijoin.StaticMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.SingleValueRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableSingleValueRowRedirection;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker.*;

/**
 * <p>
 * Join unique rows from a set of tables onto a set of common keys.
 * </p>
 *
 * <p>
 * The multiJoin operation collects the set of distinct keys from the input tables, and then joins one row from each of
 * the input tables onto the result. Input tables need not have a matching row for each key, but they may not have
 * multiple matching rows for a given key. The operation can be thought of as a merge of the key columns, followed by a
 * selectDistinct and then a series of iterative naturalJoin operations as follows:
 * </p>
 *
 * <pre>{@code
 *     private Table doIterativeMultiJoin(String [] keyColumns, List<? extends Table> inputTables) {
 *         final List<Table> keyTables = inputTables.stream().map(t -> t.view(keyColumns)).collect(Collectors.toList());
 *         final Table base = TableTools.merge(keyTables).selectDistinct(keyColumns);
 *
 *         Table result = base;
 *         for (int ii = 0; ii < inputTables.size(); ++ii) {
 *             result = result.naturalJoin(inputTables.get(ii), Arrays.asList(keyColumns));
 *         }
 *
 *         return result;
 *     }
 *     }
 * </pre>
 *
 * <p>
 * All tables must have the same number of key columns, with the same type. The key columns must all have the same names
 * in the resultant table (the left side of the {@link JoinMatch} used to create them). The columns to add from the must
 * have unique names in the result table.
 * </p>
 */
public class MultiJoinTableImpl implements MultiJoinTable {

    static final int KEY_COLUMN_SENTINEL = -2;
    private final Table table;

    @TestUseOnly
    static MultiJoinTableImpl of(@NotNull final JoinControl joinControl,
            @NotNull final MultiJoinInput... joinDescriptors) {
        return QueryPerformanceRecorder.withNugget("multiJoin",
                () -> new MultiJoinTableImpl(joinControl, joinDescriptors));
    }

    static MultiJoinTableImpl of(@NotNull final MultiJoinInput... joinDescriptors) {
        return QueryPerformanceRecorder.withNugget("multiJoin",
                () -> new MultiJoinTableImpl(new JoinControl(), joinDescriptors));
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
        return null;
    }

    private MultiJoinTableImpl(@NotNull final JoinControl joinControl,
            @NotNull final MultiJoinInput... joinDescriptors) {
        table = multiJoin(joinControl, joinDescriptors);
    }

    private Table multiJoin(@NotNull final JoinControl joinControl,
            @NotNull final MultiJoinInput... joinDescriptors) {
        final TObjectIntHashMap<String> usedColumns =
                new TObjectIntHashMap<>(joinDescriptors[0].columnsToAdd().length, 0.5f, -1);
        final List<String> expectedLeftMatches =
                ColumnName.names(JoinMatch.lefts(Arrays.asList(joinDescriptors[0].columnsToMatch())));
        if (expectedLeftMatches.size() == 0) {
            return doMultiJoinZeroKey(joinDescriptors);
        }

        final Set<String> keyColumnNames = new LinkedHashSet<>(expectedLeftMatches);
        for (final String keyColumn : expectedLeftMatches) {
            usedColumns.put(keyColumn, KEY_COLUMN_SENTINEL);
        }
        final ChunkType[] expectedChunkTypes = Arrays.stream(getKeySources(joinDescriptors[0]))
                .map(ColumnSource::getChunkType).toArray(ChunkType[]::new);

        final MultiJoinInput[] useDescriptors = Arrays.copyOf(joinDescriptors, joinDescriptors.length);

        for (int jj = 0; jj < joinDescriptors.length; ++jj) {
            MultiJoinInput joinDescriptor = useDescriptors[jj];

            final List<String> currentLeftMatches =
                    ColumnName.names(JoinMatch.lefts(Arrays.asList(joinDescriptor.columnsToMatch())));
            if (!currentLeftMatches.equals(expectedLeftMatches)) {
                throw new IllegalArgumentException("Key column mismatch for table " + jj + ", first key columns="
                        + expectedLeftMatches + " table has " + currentLeftMatches);
            }
            final ChunkType[] currentChunkType = Arrays.stream(getKeySources(joinDescriptor))
                    .map(ColumnSource::getChunkType).toArray(ChunkType[]::new);
            if (!Arrays.equals(currentChunkType, expectedChunkTypes)) {
                throw new IllegalArgumentException("Key column type mismatch for table " + jj
                        + ", first key columns types=" + Arrays.toString(expectedChunkTypes) + " table has "
                        + Arrays.toString(currentChunkType));
            }

            joinDescriptor = maybePopulateColumnsToAdd(keyColumnNames, useDescriptors, jj);
            for (int cc = 0; cc < joinDescriptor.columnsToAdd().length; ++cc) {
                final String columnName = joinDescriptor.columnsToAdd()[cc].newColumn().name();
                final int previouslyUsed = usedColumns.put(columnName, jj);
                if (previouslyUsed != usedColumns.getNoEntryValue()) {
                    throw new IllegalArgumentException("Column " + columnName + " defined in table "
                            + (previouslyUsed == KEY_COLUMN_SENTINEL ? "key columns" : Integer.toString(previouslyUsed))
                            + " and table " + jj);
                }
            }
        }

        final ColumnSource<?>[] firstKeySources = getKeySources(useDescriptors[0]);
        final ColumnSource<?>[] originalColumns = getOriginalKeyColumns(useDescriptors[0]);
        final MultiJoinStateManager stateManager;

        // If any tables are refreshing, we must use a refreshing JoinManager.
        final boolean refreshing = Arrays.stream(joinDescriptors).anyMatch(jd -> jd.inputTable().isRefreshing());
        if (refreshing) {
            ExecutionContext.getContext().getUpdateGraph().checkInitiateSerialTableOperation();
            stateManager = TypedHasherFactory.make(IncrementalMultiJoinStateManagerTypedBase.class,
                    firstKeySources, originalColumns,
                    joinControl.initialBuildSize(), joinControl.getMaximumLoadFactor(),
                    joinControl.getTargetLoadFactor());

        } else {
            stateManager = TypedHasherFactory.make(StaticMultiJoinStateManagerTypedBase.class,
                    firstKeySources, originalColumns,
                    joinControl.initialBuildSize(), joinControl.getMaximumLoadFactor(),
                    joinControl.getTargetLoadFactor());
        }
        stateManager.setMaximumLoadFactor(joinControl.getMaximumLoadFactor());
        stateManager.setTargetLoadFactor(joinControl.getTargetLoadFactor());
        stateManager.ensureTableCapacity(useDescriptors.length);

        for (int tableNumber = 0; tableNumber < useDescriptors.length; ++tableNumber) {
            final MultiJoinInput joinDescriptor = useDescriptors[tableNumber];
            final ColumnSource<?>[] keySources = getKeySources(joinDescriptor);
            stateManager.build(joinDescriptor.inputTable(), keySources, tableNumber);
        }


        final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>();
        final ColumnSource[] keyHashTableSources = stateManager.getKeyHashTableSources();
        for (int cc = 0; cc < expectedLeftMatches.size(); ++cc) {
            if (originalColumns[cc].getType() != keyHashTableSources[cc].getType()) {
                resultSources.put(expectedLeftMatches.get(cc),
                        ReinterpretUtils.convertToOriginalType(originalColumns[cc], keyHashTableSources[cc]));
            } else {
                resultSources.put(expectedLeftMatches.get(cc), keyHashTableSources[cc]);
            }
        }

        for (int tableNumber = 0; tableNumber < useDescriptors.length; ++tableNumber) {
            final MultiJoinInput joinDescriptor = useDescriptors[tableNumber];

            final WritableRowRedirection rowRedirection = stateManager.getRowRedirectionForTable(tableNumber);
            if (refreshing) {
                rowRedirection.startTrackingPrevValues();
            }
            final Table inputTable = joinDescriptor.inputTable();

            for (final JoinAddition ja : joinDescriptor.columnsToAdd()) {
                resultSources.put(ja.newColumn().name(), RedirectedColumnSource.alwaysRedirect(rowRedirection,
                        inputTable.getColumnSource(ja.existingColumn().name())));
            }
        }

        final QueryTable result =
                new QueryTable(RowSetFactory.flat(stateManager.getResultSize()).toTracking(), resultSources);

        if (refreshing) {
            final ModifiedColumnSet[] resultModifiedColumnSet = new ModifiedColumnSet[useDescriptors.length];
            final List<MultiJoinListenerRecorder> listenerRecorders = Collections.synchronizedList(new ArrayList<>());

            final Logger log = LoggerFactory.getLogger(MultiJoinTableImpl.class);

            final MergedListener mergedListener = new MultiJoinMergedListener(
                    (IncrementalMultiJoinStateManagerTypedBase) stateManager,
                    listenerRecorders,
                    Collections.emptyList(),
                    "multiJoin(" + keyColumnNames + ")",
                    result,
                    resultModifiedColumnSet);

            for (int ii = 0; ii < useDescriptors.length; ++ii) {
                final MultiJoinInput jd = useDescriptors[ii];
                if (jd.inputTable().isRefreshing()) {
                    final QueryTable input = (QueryTable) jd.inputTable();
                    final ColumnSource<?>[] keySources = getKeySources(jd);


                    final String[] matchNames =
                            Arrays.stream(jd.columnsToMatch()).map(v -> v.right().name()).toArray(String[]::new);
                    final ModifiedColumnSet sourceKeyModifiedColumnSet =
                            input.newModifiedColumnSet(matchNames);

                    final String[] addNames =
                            Arrays.stream(jd.columnsToAdd()).map(v -> v.newColumn().name()).toArray(String[]::new);
                    resultModifiedColumnSet[ii] =
                            result.newModifiedColumnSet(addNames);
                    final ModifiedColumnSet.Transformer transformer =
                            input.newModifiedColumnSetTransformer(result, addNames);

                    final MultiJoinListenerRecorder listenerRecorder =
                            new MultiJoinListenerRecorder("multiJoin(" + ii + ")", input, result, keySources,
                                    sourceKeyModifiedColumnSet, transformer, ii);
                    listenerRecorder.setMergedListener(mergedListener);
                    input.addUpdateListener(listenerRecorder);
                    listenerRecorders.add(listenerRecorder);
                }
            }

            result.addParentReference(mergedListener);
        }

        return result;
    }

    @NotNull
    private static MultiJoinInput maybePopulateColumnsToAdd(
            @NotNull final Set<String> keyColumnNames,
            @NotNull final MultiJoinInput[] useDescriptors, final int tableNumber) {
        MultiJoinInput joinDescriptor = useDescriptors[tableNumber];
        JoinAddition[] columnsToAdd = joinDescriptor.columnsToAdd();
        if (columnsToAdd.length == 0) {
            // create them on the fly from the table
            final JoinAddition[] newColumnsToAdd = joinDescriptor.inputTable().getDefinition().getColumnNames().stream()
                    .filter(cn -> !keyColumnNames.contains(cn)).map(MatchPairFactory::getExpression)
                    .toArray(MatchPair[]::new);
            joinDescriptor = useDescriptors[tableNumber] =
                    MultiJoinInput.of(joinDescriptor.inputTable(),
                            joinDescriptor.columnsToMatch(),
                            newColumnsToAdd);
        }
        return joinDescriptor;
    }

    private static Table doMultiJoinZeroKey(@NotNull final MultiJoinInput... joinDescriptors) {
        final MultiJoinInput[] useDescriptors = Arrays.copyOf(joinDescriptors, joinDescriptors.length);
        final TObjectIntHashMap<String> usedColumns =
                new TObjectIntHashMap<>(joinDescriptors[0].columnsToAdd().length, 0.5f, -1);

        for (int jj = 0; jj < joinDescriptors.length; ++jj) {
            MultiJoinInput joinDescriptor = useDescriptors[jj];

            if (joinDescriptor.columnsToMatch().length != 0) {
                Collection<String> matches = Arrays.stream(joinDescriptor.columnsToMatch())
                        .map(v -> v.left().name() + "=" + v.right().name()).collect(Collectors.toList());
                throw new IllegalArgumentException(
                        "Key column mismatch for table " + jj + ", first table had no key columns this table has "
                                + matches);
            }

            joinDescriptor = maybePopulateColumnsToAdd(Collections.emptySet(), useDescriptors, jj);
            for (int cc = 0; cc < joinDescriptor.columnsToAdd().length; ++cc) {
                final String columnName = joinDescriptor.columnsToAdd()[cc].newColumn().name();
                final int previouslyUsed = usedColumns.put(columnName, jj);
                if (previouslyUsed != usedColumns.getNoEntryValue()) {
                    throw new IllegalArgumentException(
                            "Column " + columnName + " defined in table " + previouslyUsed + " and table " + jj);
                }
            }
        }

        final SingleValueRowRedirection[] redirections = new SingleValueRowRedirection[joinDescriptors.length];
        final boolean refreshing = Arrays.stream(joinDescriptors).anyMatch(jd -> jd.inputTable().isRefreshing());

        final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>();
        boolean hasResults = false;
        for (int tableNumber = 0; tableNumber < useDescriptors.length; ++tableNumber) {
            final MultiJoinInput joinDescriptor = useDescriptors[tableNumber];
            final Table inputTable = joinDescriptor.inputTable();

            final long key;
            if (inputTable.size() == 0) {
                key = NULL_ROW_KEY;
            } else if (inputTable.size() == 1) {
                key = inputTable.getRowSet().firstRowKey();
                hasResults = true;
            } else {
                throw new IllegalStateException("Duplicate rows for table " + tableNumber + " on zero-key multiJoin.");
            }

            final SingleValueRowRedirection rowRedirection =
                    refreshing ? new WritableSingleValueRowRedirection(key) : new SingleValueRowRedirection(key);
            if (refreshing) {
                rowRedirection.writableSingleValueCast().startTrackingPrevValues();
            }
            redirections[tableNumber] = rowRedirection;

            for (final JoinAddition ja : joinDescriptor.columnsToAdd()) {
                resultSources.put(ja.newColumn().name(), RedirectedColumnSource.alwaysRedirect(rowRedirection,
                        inputTable.getColumnSource(ja.existingColumn().name())));
            }
        }

        final QueryTable result = new QueryTable(RowSetFactory.flat(hasResults ? 1 : 0).toTracking(), resultSources);

        if (refreshing) {
            final ModifiedColumnSet[] resultModifiedColumnSet = new ModifiedColumnSet[useDescriptors.length];
            final List<MultiJoinListenerRecorder> listenerRecorders = Collections.synchronizedList(new ArrayList<>());

            final Logger log = LoggerFactory.getLogger(MultiJoinTableImpl.class);

            final MergedListener mergedListener = new MultiJoinZeroKeyMergedListener(
                    listenerRecorders,
                    Collections.emptyList(),
                    "multiJoin()",
                    result,
                    resultModifiedColumnSet,
                    redirections);

            for (int ii = 0; ii < useDescriptors.length; ++ii) {
                final MultiJoinInput jd = useDescriptors[ii];
                if (jd.inputTable().isRefreshing()) {
                    final QueryTable input = (QueryTable) jd.inputTable();

                    final String[] addNames =
                            Arrays.stream(jd.columnsToAdd()).map(v -> v.newColumn().name()).toArray(String[]::new);
                    resultModifiedColumnSet[ii] =
                            result.newModifiedColumnSet(addNames);
                    final ModifiedColumnSet.Transformer transformer =
                            input.newModifiedColumnSetTransformer(result, addNames);

                    final MultiJoinListenerRecorder listenerRecorder = new MultiJoinListenerRecorder(
                            "multiJoin(" + ii + ")", input, result, null, null, transformer, ii);
                    listenerRecorder.setMergedListener(mergedListener);
                    input.addUpdateListener(listenerRecorder);
                    listenerRecorders.add(listenerRecorder);
                }
            }

            result.addParentReference(mergedListener);

        }

        return result;
    }

    @NotNull
    private static ColumnSource[] getKeySources(@NotNull final MultiJoinInput useDescriptor) {
        return Arrays.stream(useDescriptor.columnsToMatch())
                .map(jm -> ReinterpretUtils
                        .maybeConvertToPrimitive(useDescriptor.inputTable().getColumnSource(jm.right().name())))
                .toArray(ColumnSource[]::new);
    }

    @NotNull

    private static ColumnSource<?>[] getOriginalKeyColumns(
            @NotNull final MultiJoinInput useDescriptor) {
        return Arrays.stream(useDescriptor.columnsToMatch())
                .map(jm -> useDescriptor.inputTable().getColumnSource(jm.right().name()))
                .toArray(ColumnSource<?>[]::new);
    }

    private static class MultiJoinListenerRecorder extends ListenerRecorder {
        private final ColumnSource<?>[] keyColumns;
        private final ModifiedColumnSet sourceKeyModifiedColumnSet;
        private final ModifiedColumnSet.Transformer transformer;
        private final int tableNumber;

        public MultiJoinListenerRecorder(@NotNull final String description,
                @NotNull final QueryTable parent,
                @NotNull final QueryTable dependent,
                final ColumnSource<?>[] keyColumns,
                final ModifiedColumnSet sourceKeyModifiedColumnSet,
                @NotNull final ModifiedColumnSet.Transformer transformer,
                final int tableNumber) {
            super(description, parent, dependent);
            this.keyColumns = keyColumns;
            this.sourceKeyModifiedColumnSet = sourceKeyModifiedColumnSet;
            this.transformer = transformer;
            this.tableNumber = tableNumber;
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
            slotTracker.clear();
            slotTracker.ensureTableCapacity(stateManager.getTableCount());

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
                        stateManager.processModified(recorder.getModified(), recorder.keyColumns, recorder.tableNumber,
                                slotTracker, FLAG_MODIFY);
                    }

                    if (recorder.getAdded().isNonempty()) {
                        stateManager.processAdded(recorder.getAdded(), recorder.keyColumns, recorder.tableNumber,
                                slotTracker, FLAG_ADD);
                    }
                    if (keysModified) {
                        stateManager.processAdded(recorder.getModified(), recorder.keyColumns, recorder.tableNumber,
                                slotTracker, FLAG_MODIFY);
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

            final long[] currentRedirections = new long[stateManager.getTableCount()];
            final boolean[] modifiedTables = new boolean[stateManager.getTableCount()];
            final boolean[] modifiedTablesOnThisRow = new boolean[stateManager.getTableCount()];

            final RowSetBuilderRandom modifiedBuilder = RowSetFactory.builderRandom();
            final RowSetBuilderRandom removedBuilder = RowSetFactory.builderRandom();
            final RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();

            downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            downstream.modifiedColumnSet.clear();

            final byte notShift = (FLAG_ADD | FLAG_REMOVE | FLAG_MODIFY);
            final byte addOrRemove = (FLAG_ADD | FLAG_REMOVE);

            slotTracker.forAllModifiedSlots((slot, originalValues, flagValues) -> {
                if (slot >= originalSize) {
                    return;
                }
                stateManager.getCurrentRedirections(slot, currentRedirections);
                boolean allNull = true;
                boolean rowModified = false;
                int numberOfOriginalNulls = 0;
                Arrays.fill(modifiedTablesOnThisRow, false);
                for (int ii = 0; ii < currentRedirections.length; ++ii) {
                    if (currentRedirections[ii] != NULL_ROW_KEY) {
                        allNull = false;
                    }
                    if (originalValues[ii] == NULL_ROW_KEY) {
                        numberOfOriginalNulls++;
                    }
                    if (originalValues[ii] == MultiJoinModifiedSlotTracker.SENTINEL_UNINITIALIZED_KEY) {
                        if (currentRedirections[ii] == NULL_ROW_KEY) {
                            numberOfOriginalNulls++;
                        }
                        rowModified |= (flagValues[ii] & notShift) != 0;
                    } else {
                        // If the redirection has changed and we have done anything other than a shift, we must light
                        // up all the columns for the table as a modification. Similarly, if the row was added and
                        // deleted from the original table, then we must also light up all the columns as modified.
                        if ((flagValues[ii] & addOrRemove) != 0 || currentRedirections[ii] != originalValues[ii]) {
                            rowModified |= (modifiedTablesOnThisRow[ii] = (flagValues[ii] & notShift) != 0);
                        } else {
                            rowModified |= (flagValues[ii] & notShift) != 0;
                        }
                    }
                }
                if (allNull) {
                    removedBuilder.addKey(slot);
                } else if (numberOfOriginalNulls == currentRedirections.length) {
                    addedBuilder.addKey(slot);
                } else if (rowModified) {
                    modifiedBuilder.addKey(slot);

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
                    MultiJoinListenerRecorder recorder =
                            listenerIndex >= recorders.size() ? null : recorders.get(listenerIndex);
                    if (modifiedTables[ii]) {
                        downstream.modifiedColumnSet.setAll(modifiedColumnSets[ii]);
                        if (recorder != null && recorder.tableNumber == ii) {
                            listenerIndex++;
                        }
                    } else {
                        // If we have not had any cells which are remapped, but we did have any modified rows then we
                        // need to light up all the result columns, because we know that the modified row in the input
                        // table must map to a row in the output table.
                        if (recorder != null && recorder.tableNumber == ii) {
                            if (recorder.getModified().isNonempty()) {
                                recorder.transformer.transform(recorder.getModifiedColumnSet(),
                                        downstream.modifiedColumnSet);
                            }
                            listenerIndex++;
                        }
                    }
                }
            }
            downstream.removed = removedBuilder.build();
            downstream.added.writableCast().insert(addedBuilder.build());

            downstream.shifted = RowSetShiftData.EMPTY;

            result.getRowSet().writableCast().remove(downstream.removed);
            result.getRowSet().writableCast().insert(downstream.added);

            result.notifyListeners(downstream);
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
                        resultModified = true;
                        recorder.transformer.transform(recorder.getModifiedColumnSet(), downstream.modifiedColumnSet);
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
                downstream.added = RowSetFactory.fromKeys(0);
                downstream.removed = RowSetFactory.empty();
                downstream.modified = RowSetFactory.empty();
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else if (!hasResults && result.size() == 1) {
                result.getRowSet().writableCast().remove(0);
                downstream.added = RowSetFactory.empty();
                downstream.removed = RowSetFactory.fromKeys(0);
                downstream.modified = RowSetFactory.empty();
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else {
                downstream.added = RowSetFactory.empty();
                downstream.removed = RowSetFactory.empty();
                if (resultModified) {
                    downstream.modified = RowSetFactory.fromKeys(0);
                } else {
                    // this would be a useless update
                    return;
                }
            }
            result.notifyListeners(downstream);
        }
    }
}
