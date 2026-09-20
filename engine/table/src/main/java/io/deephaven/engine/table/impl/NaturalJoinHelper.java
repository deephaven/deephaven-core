//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.join.ChangedKeyRows;
import io.deephaven.engine.table.impl.join.JoinListenerRecorder;
import io.deephaven.engine.table.impl.naturaljoin.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.table.impl.JoinControl.BuildParameters.From.*;

class NaturalJoinHelper {

    private NaturalJoinHelper() {} // static use only

    static Table naturalJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, NaturalJoinType joinType) {
        return naturalJoin(leftTable, rightTable, columnsToMatch, columnsToAdd, joinType, new JoinControl());
    }

    @VisibleForTesting
    static Table naturalJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, NaturalJoinType joinType, JoinControl control) {
        final QueryTable result =
                naturalJoinInternal(leftTable, rightTable, columnsToMatch, columnsToAdd, joinType, control);
        leftTable.maybeCopyColumnDescriptions(result, rightTable, columnsToMatch, columnsToAdd);
        leftTable.copyAttributes(result, BaseTable.CopyAttributeOperation.Join);
        // note in exact match we require that the right table can match as soon as a row is added to the left
        boolean rightDoesNotGenerateModifies =
                !rightTable.isRefreshing() || (joinType == NaturalJoinType.EXACTLY_ONE_MATCH && rightTable.isAddOnly());
        if (leftTable.isAddOnly() && rightDoesNotGenerateModifies) {
            result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
        }
        if (leftTable.isAppendOnly() && rightDoesNotGenerateModifies) {
            result.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
        }
        return result;
    }

    private static QueryTable naturalJoinInternal(QueryTable leftTable, QueryTable rightTable,
            MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, NaturalJoinType joinType, JoinControl control) {
        QueryTable.checkInitiateBinaryOperation(leftTable, rightTable);

        try (final BucketingContext bc = new BucketingContext("naturalJoin",
                leftTable, rightTable, columnsToMatch, columnsToAdd, control, true, true)) {
            final JoinControl.BuildParameters.From firstBuildFrom = bc.buildParameters.firstBuildFrom();
            final int initialHashTableSize = bc.buildParameters.hashTableSize();
            final boolean rightAddOnly = rightTable.isAddOnly();

            // if we have a single column of unique values, and the range is small, we can use a simplified table
            // TODO: SimpleUniqueStaticNaturalJoinManager, but not static!
            if (!rightTable.isRefreshing()
                    && control.useUniqueTable(bc.uniqueValues, bc.maximumUniqueValue, bc.minimumUniqueValue)) {
                Assert.neqNull(bc.uniqueChunkType, "uniqueChunkType");
                final SimpleUniqueStaticNaturalJoinStateManager jsm = new SimpleUniqueStaticNaturalJoinStateManager(
                        bc.originalLeftSources, bc.uniqueValuesRange(), bc.uniqueChunkType, bc.uniqueOffset, joinType,
                        rightTable.isAddOnly());
                jsm.setRightSide(rightTable.getRowSet(), bc.rightSources[0]);
                final LongArraySource leftRedirections = new LongArraySource();
                leftRedirections.ensureCapacity(leftTable.getRowSet().size());
                jsm.decorateLeftSide(leftTable.getRowSet(), bc.leftSources, leftRedirections);

                final WritableRowRedirection rowRedirection =
                        jsm.buildRowRedirection(leftTable, leftRedirections, control.getRedirectionType(leftTable));

                // the right side is static, so the redirection only changes when left rows are added or re-keyed
                final QueryTable result =
                        makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, leftTable.isRefreshing());
                if (leftTable.isRefreshing()) {
                    leftTable.addUpdateListener(new LeftTickingListener(bc.listenerDescription, columnsToMatch,
                            columnsToAdd, leftTable, result, rowRedirection, jsm, bc.leftSources));
                }
                return result;
            }

            if (bc.leftSources.length == 0) {
                return zeroKeyColumnsJoin(leftTable, rightTable, columnsToAdd, joinType, bc.listenerDescription);
            }

            final WritableRowRedirection rowRedirection;

            if (leftTable.isRefreshing() && rightTable.isRefreshing()) {
                // We always build right first, regardless of the build parameters. This is probably irrelevant.

                // The build parameters size the table for the right row count, treating the right keys as unique. A
                // join that errors on duplicates needs a state per right row, so that size is exact; a first- or
                // last-match join collapses duplicates into one state, so its right row count may far overstate the
                // states, and it starts from the left data index size (or the default) and grows by rehashing.
                final boolean rightKeysUnique =
                        joinType == NaturalJoinType.ERROR_ON_DUPLICATE || joinType == NaturalJoinType.EXACTLY_ONE_MATCH;
                final int bothIncrementalTableSize = rightKeysUnique
                        ? initialHashTableSize
                        : bc.leftDataIndexTable != null
                                ? control.tableSize(bc.leftDataIndexTable.size())
                                : control.initialBuildSize();

                final BothIncrementalNaturalJoinStateManager jsm = TypedHasherFactory.makeNaturalJoin(
                        IncrementalNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        bothIncrementalTableSize, control.getMaximumLoadFactor(),
                        control.getTargetLoadFactor(), joinType, rightAddOnly);
                jsm.buildFromRightSide(rightTable, bc.rightSources);

                try (final BothIncrementalNaturalJoinStateManager.InitialBuildContext ibc =
                        jsm.makeInitialBuildContext()) {

                    if (bc.leftDataIndexTable != null) {
                        jsm.decorateLeftSide(bc.leftDataIndexTable.getRowSet(), bc.leftDataIndexSources, ibc);
                        rowRedirection = jsm.buildIndexedRowRedirection(leftTable, ibc,
                                bc.leftDataIndexRowSetSource, control.getRedirectionType(leftTable));
                    } else {
                        jsm.decorateLeftSide(leftTable.getRowSet(), bc.leftSources, ibc);
                        jsm.compactAll();
                        rowRedirection = jsm.buildRowRedirectionFromRedirections(leftTable, ibc,
                                control.getRedirectionType(leftTable));
                    }
                }

                final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, true);

                final JoinListenerRecorder leftRecorder =
                        new JoinListenerRecorder(true, bc.listenerDescription, leftTable, result);
                final JoinListenerRecorder rightRecorder =
                        new JoinListenerRecorder(false, bc.listenerDescription, rightTable, result);

                final ChunkedMergedJoinListener mergedJoinListener = new ChunkedMergedJoinListener(
                        leftTable, rightTable, bc.leftSources, bc.rightSources, columnsToMatch, columnsToAdd,
                        leftRecorder, rightRecorder, result, rowRedirection, jsm, joinType, rightAddOnly,
                        bc.listenerDescription);
                leftRecorder.setMergedListener(mergedJoinListener);
                rightRecorder.setMergedListener(mergedJoinListener);

                leftTable.addUpdateListener(leftRecorder);
                rightTable.addUpdateListener(rightRecorder);

                result.addParentReference(mergedJoinListener);

                return result;
            }

            if (leftTable.isRefreshing()) {
                Assert.eq(firstBuildFrom, "firstBuildFrom", RightInput);

                final LongArraySource leftRedirections = new LongArraySource();
                final StaticHashedNaturalJoinStateManager jsm = TypedHasherFactory.makeNaturalJoin(
                        StaticNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        initialHashTableSize, control.getMaximumLoadFactor(),
                        control.getTargetLoadFactor(), joinType, rightAddOnly);

                jsm.buildFromRightSide(rightTable, bc.rightSources);
                if (bc.leftDataIndexTable != null) {
                    jsm.decorateLeftSideIndexed(bc.leftDataIndexTable.getRowSet(), bc.leftDataIndexSources,
                            bc.leftDataIndexRowSetSource, leftRedirections);
                    rowRedirection = jsm.buildIndexedRowRedirectionFromRedirections(leftTable,
                            bc.leftDataIndexTable.getRowSet(), leftRedirections, bc.leftDataIndexRowSetSource,
                            control.getRedirectionType(leftTable));
                } else {
                    jsm.decorateLeftSide(leftTable.getRowSet(), bc.leftSources, leftRedirections);
                    rowRedirection = jsm.buildRowRedirectionFromRedirections(leftTable, leftRedirections,
                            control.getRedirectionType(leftTable));
                }

                final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, true);

                leftTable.addUpdateListener(
                        new LeftTickingListener(
                                bc.listenerDescription,
                                columnsToMatch,
                                columnsToAdd,
                                leftTable,
                                result,
                                rowRedirection,
                                jsm,
                                bc.leftSources));
                return result;
            }

            if (rightTable.isRefreshing()) {
                Assert.assertion(firstBuildFrom == LeftInput || firstBuildFrom == LeftDataIndex,
                        "firstBuildFrom == LeftInput || firstBuildFrom == LeftDataIndex");

                final RightIncrementalNaturalJoinStateManager jsm = TypedHasherFactory.makeNaturalJoin(
                        RightIncrementalNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        initialHashTableSize, control.getMaximumLoadFactor(),
                        control.getTargetLoadFactor(), joinType, rightAddOnly);
                final RightIncrementalNaturalJoinStateManager.InitialBuildContext ibc =
                        jsm.makeInitialBuildContext(leftTable);

                if (firstBuildFrom == LeftDataIndex) {
                    Assert.neqNull(bc.leftDataIndexTable, "leftDataIndexTable");
                    jsm.buildFromLeftSide(bc.leftDataIndexTable, bc.leftDataIndexSources, ibc);
                    jsm.convertLeftDataIndex(bc.leftDataIndexTable.intSize(), ibc, bc.leftDataIndexRowSetSource);
                } else {
                    jsm.buildFromLeftSide(leftTable, bc.leftSources, ibc);
                }

                jsm.addRightSide(rightTable.getRowSet(), bc.rightSources);

                if (firstBuildFrom == LeftDataIndex) {
                    rowRedirection = jsm.buildRowRedirectionFromHashSlotIndexed(leftTable, bc.leftDataIndexRowSetSource,
                            bc.leftDataIndexTable.intSize(), ibc, control.getRedirectionType(leftTable));
                } else {
                    rowRedirection = jsm.buildRowRedirectionFromHashSlot(leftTable, ibc,
                            control.getRedirectionType(leftTable));
                }

                final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, true);

                rightTable.addUpdateListener(
                        new RightTickingListener(
                                bc.listenerDescription,
                                rightTable,
                                columnsToMatch,
                                columnsToAdd,
                                result,
                                rowRedirection,
                                jsm,
                                bc.rightSources,
                                joinType,
                                rightAddOnly));
                return result;
            }

            if (firstBuildFrom == LeftDataIndex) {
                Assert.neqNull(bc.leftDataIndexTable, "leftDataIndexTable");
                final StaticHashedNaturalJoinStateManager jsm = TypedHasherFactory.makeNaturalJoin(
                        StaticNaturalJoinStateManagerTypedBase.class, bc.leftDataIndexSources,
                        bc.originalLeftDataIndexSources, initialHashTableSize,
                        control.getMaximumLoadFactor(), control.getTargetLoadFactor(), joinType, rightAddOnly);

                final IntegerArraySource leftHashSlots = new IntegerArraySource();
                jsm.buildFromLeftSide(bc.leftDataIndexTable, bc.leftDataIndexSources,
                        leftHashSlots);
                try {
                    jsm.decorateWithRightSide(rightTable, bc.rightSources);
                } catch (DuplicateRightRowDecorationException e) {
                    jsm.errorOnDuplicatesIndexed(leftHashSlots, bc.leftDataIndexTable.getRowSet());
                }
                rowRedirection = jsm.buildIndexedRowRedirectionFromHashSlots(leftTable,
                        bc.leftDataIndexTable.getRowSet(), leftHashSlots,
                        bc.leftDataIndexRowSetSource, control.getRedirectionType(leftTable));
            } else if (firstBuildFrom == LeftInput) {
                final StaticHashedNaturalJoinStateManager jsm = TypedHasherFactory.makeNaturalJoin(
                        StaticNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        // The static state manager doesn't allow rehashing, so we must allocate a big enough hash
                        // table for the possibility that all left rows will have unique keys.
                        control.tableSize(leftTable.size()),
                        control.getMaximumLoadFactor(), control.getTargetLoadFactor(), joinType, rightAddOnly);

                final IntegerArraySource leftHashSlots = new IntegerArraySource();
                jsm.buildFromLeftSide(leftTable, bc.leftSources, leftHashSlots);
                try {
                    jsm.decorateWithRightSide(rightTable, bc.rightSources);
                } catch (DuplicateRightRowDecorationException e) {
                    jsm.errorOnDuplicatesSingle(leftHashSlots, leftTable.size(), leftTable.getRowSet());
                }
                rowRedirection = jsm.buildRowRedirectionFromHashSlot(leftTable, leftHashSlots,
                        control.getRedirectionType(leftTable));
            } else {
                final LongArraySource leftRedirections = new LongArraySource();
                final StaticHashedNaturalJoinStateManager jsm = TypedHasherFactory.makeNaturalJoin(
                        StaticNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        initialHashTableSize, control.getMaximumLoadFactor(),
                        control.getTargetLoadFactor(), joinType, rightAddOnly);

                jsm.buildFromRightSide(rightTable, bc.rightSources);
                jsm.decorateLeftSide(leftTable.getRowSet(), bc.leftSources, leftRedirections);
                rowRedirection = jsm.buildRowRedirectionFromRedirections(leftTable, leftRedirections,
                        control.getRedirectionType(leftTable));
            }
            return makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, false);
        }
    }

    @NotNull
    private static QueryTable zeroKeyColumnsJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToAdd,
            NaturalJoinType joinType, String listenerDescription) {
        // we are a single value join, we do not need to do any work
        final SingleValueRowRedirection rowRedirection;

        final boolean rightRefreshing = rightTable.isRefreshing();

        if (rightTable.size() > 1) {
            if ((joinType == NaturalJoinType.ERROR_ON_DUPLICATE || joinType == NaturalJoinType.EXACTLY_ONE_MATCH)) {
                if (!leftTable.isEmpty()) {
                    throw new IllegalStateException(
                            "naturalJoin with zero key columns may not have more than one row in the right hand side table!");
                }
                // we don't care where it goes
                rowRedirection = getSingleValueRowRedirection(rightRefreshing, RowSequence.NULL_ROW_KEY);
            } else {
                // The selected right row is known here, and only a refreshing right table gets a writable
                // redirection, so the row must be seeded rather than re-pointed afterwards.
                rowRedirection = getSingleValueRowRedirection(rightRefreshing,
                        joinType == NaturalJoinType.FIRST_MATCH
                                ? rightTable.getRowSet().firstRowKey()
                                : rightTable.getRowSet().lastRowKey());
            }
        } else if (rightTable.size() == 1) {
            rowRedirection = getSingleValueRowRedirection(rightRefreshing, rightTable.getRowSet().firstRowKey());
        } else {
            if (joinType == NaturalJoinType.EXACTLY_ONE_MATCH && !leftTable.isEmpty()) {
                throw new RuntimeException(
                        "exactJoin with zero key columns must have exactly one row in the right hand side table!");
            }
            rowRedirection = getSingleValueRowRedirection(rightRefreshing, RowSequence.NULL_ROW_KEY);
        }

        final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, rightRefreshing);
        final ModifiedColumnSet.Transformer leftTransformer =
                leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());
        final ModifiedColumnSet.Transformer rightTransformer =
                rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);
        final ModifiedColumnSet allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));

        if (leftTable.isRefreshing()) {
            if (rightTable.isRefreshing()) {
                final JoinListenerRecorder leftRecorder =
                        new JoinListenerRecorder(true, listenerDescription, leftTable, result);
                final JoinListenerRecorder rightRecorder =
                        new JoinListenerRecorder(false, listenerDescription, rightTable, result);

                final MergedListener mergedListener = new MergedListener(Arrays.asList(leftRecorder, rightRecorder),
                        Collections.emptyList(), listenerDescription, result) {
                    @Override
                    protected void process() {
                        final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                        modifiedColumnSet.clear();

                        final boolean rightChanged = rightRecorder.recordedVariablesAreValid();
                        final boolean leftChanged = leftRecorder.recordedVariablesAreValid();

                        checkRightTableSizeZeroKeys(leftTable, rightTable, joinType);

                        final boolean rightValuesChanged = rightChanged && applyZeroKeyRightUpdate(rightTable,
                                rowRedirection, joinType, rightRecorder.getUpdate(), rightTransformer,
                                allRightColumns, modifiedColumnSet);

                        if (leftChanged) {
                            final RowSet modified;
                            if (rightValuesChanged) {
                                modified = result.getRowSet().minus(leftRecorder.getAdded());
                            } else {
                                modified = leftRecorder.getModified().copy();
                            }
                            leftTransformer.transform(leftRecorder.getModifiedColumnSet(), modifiedColumnSet);
                            result.notifyListeners(new TableUpdateImpl(
                                    leftRecorder.getAdded().copy(), leftRecorder.getRemoved().copy(), modified,
                                    leftRecorder.getShifted(), modifiedColumnSet));
                        } else if (rightValuesChanged) {
                            result.notifyListeners(new TableUpdateImpl(
                                    RowSetFactory.empty(), RowSetFactory.empty(),
                                    result.getRowSet().copy(), RowSetShiftData.EMPTY, modifiedColumnSet));
                        }
                    }

                };

                leftRecorder.setMergedListener(mergedListener);
                rightRecorder.setMergedListener(mergedListener);
                leftTable.addUpdateListener(leftRecorder);
                rightTable.addUpdateListener(rightRecorder);
                result.addParentReference(mergedListener);

            } else {
                leftTable
                        .addUpdateListener(new BaseTable.ListenerImpl(listenerDescription, leftTable, result) {
                            @Override
                            public void onUpdate(final TableUpdate upstream) {
                                checkRightTableSizeZeroKeys(leftTable, rightTable, joinType);
                                final TableUpdateImpl downstream =
                                        TableUpdateImpl.copy(upstream, result.getModifiedColumnSetForUpdates());
                                leftTransformer.clearAndTransform(upstream.modifiedColumnSet(),
                                        downstream.modifiedColumnSet);
                                result.notifyListeners(downstream);
                            }
                        });
            }
        } else if (rightTable.isRefreshing()) {
            if (!leftTable.isEmpty()) {
                rightTable.addUpdateListener(
                        new BaseTable.ListenerImpl(listenerDescription, rightTable, result) {
                            @Override
                            public void onUpdate(final TableUpdate upstream) {
                                checkRightTableSizeZeroKeys(leftTable, rightTable, joinType);
                                final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                                modifiedColumnSet.clear();
                                if (!applyZeroKeyRightUpdate(rightTable, rowRedirection, joinType, upstream,
                                        rightTransformer, allRightColumns, modifiedColumnSet)) {
                                    return;
                                }
                                result.notifyListeners(
                                        new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(),
                                                result.getRowSet().copy(), RowSetShiftData.EMPTY, modifiedColumnSet));
                            }
                        });
            }
        }
        return result;
    }

    @NotNull
    private static SingleValueRowRedirection getSingleValueRowRedirection(boolean refreshing, long value) {
        return refreshing ? new WritableSingleValueRowRedirection(value)
                : new SingleValueRowRedirection(value);
    }

    private static boolean updateRightRedirection(
            final QueryTable rightTable,
            final SingleValueRowRedirection rowRedirection,
            final NaturalJoinType joinType) {
        final boolean changed;
        if (rightTable.isEmpty()) {
            changed = rowRedirection.getValue() != RowSequence.NULL_ROW_KEY;
            if (changed) {
                rowRedirection.writableSingleValueCast().setValue(RowSequence.NULL_ROW_KEY);
            }
        } else {
            final long value;
            if (joinType == NaturalJoinType.FIRST_MATCH) {
                value = rightTable.getRowSet().firstRowKey();
            } else {
                value = rightTable.getRowSet().lastRowKey();
            }
            changed = rowRedirection.getValue() != value;
            if (changed) {
                rowRedirection.writableSingleValueCast().setValue(value);
            }
        }
        return changed;
    }

    /**
     * Apply a right update to a zero-key join's redirection and record which right columns may have changed for every
     * result row. The redirection selects one right row, so only a change to that row (or a change of which row is
     * selected) affects the result.
     *
     * @param modifiedColumnSet the result's modified column set, cleared by the caller; receives the affected right
     *        columns
     * @return whether every result row's right values may have changed
     */
    private static boolean applyZeroKeyRightUpdate(
            final QueryTable rightTable,
            final SingleValueRowRedirection rowRedirection,
            final NaturalJoinType joinType,
            final TableUpdate upstream,
            final ModifiedColumnSet.Transformer rightTransformer,
            final ModifiedColumnSet allRightColumns,
            final ModifiedColumnSet modifiedColumnSet) {
        // NULL_ROW_KEY when no right row was selected; it is never a member of the update's row sets below
        final long previousRightRow = rowRedirection.getValue();
        if (updateRightRedirection(rightTable, rowRedirection, joinType)) {
            // the selected key changed; if the previously selected row simply shifted to the new key, it is still the
            // selected row and only its own modifications matter
            final long selectedRightRow = rowRedirection.getValue();
            final boolean previousRowShiftedToSelectedKey = previousRightRow != RowSequence.NULL_ROW_KEY
                    && upstream.removed().find(previousRightRow) < 0
                    && upstream.shifted().apply(previousRightRow) == selectedRightRow;
            if (!previousRowShiftedToSelectedKey) {
                modifiedColumnSet.setAll(allRightColumns);
                return true;
            }
        } else if (upstream.removed().find(previousRightRow) >= 0 || upstream.added().find(previousRightRow) >= 0) {
            // the selected key is unchanged but holds a different row: the previous row was removed (and another row
            // re-added or shifted into its key), or it shifted away and a new row was added at its key. Shifts preserve
            // order, so an existing row cannot otherwise take the first or last key without that key changing.
            modifiedColumnSet.setAll(allRightColumns);
            return true;
        }
        // the same row remains selected, at its current (post-shift) key
        if (upstream.modified().find(rowRedirection.getValue()) >= 0) {
            rightTransformer.transform(upstream.modifiedColumnSet(), modifiedColumnSet);
            return modifiedColumnSet.nonempty();
        }
        return false;
    }

    /**
     * Check the right table's size against the join type when there are left rows to match. The exceptions match the
     * keyed paths: a duplicate right key is an {@link IllegalStateException}, a missing exact match a
     * {@link RuntimeException}.
     */
    private static void checkRightTableSizeZeroKeys(
            final Table leftTable,
            final Table rightTable,
            final NaturalJoinType joinType) {
        if (leftTable.isEmpty()) {
            return;
        }
        if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
            if (rightTable.size() > 1) {
                throw new IllegalStateException(
                        "naturalJoin with zero key columns may not have more than one row in the right hand side table!");
            }
        }
        if (joinType == NaturalJoinType.EXACTLY_ONE_MATCH && rightTable.isEmpty()) {
            throw new RuntimeException(
                    "exactJoin with zero key columns must have exactly one row in the right hand side table!");
        }
    }

    @NotNull
    private static QueryTable makeResult(@NotNull final QueryTable leftTable,
            @NotNull final Table rightTable,
            @NotNull final MatchPair[] columnsToAdd,
            @NotNull final RowRedirection rowRedirection,
            final boolean rightRefreshingColumns) {
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>(leftTable.getColumnSourceMap());
        for (MatchPair mp : columnsToAdd) {
            // note that we must always redirect the right-hand side, because unmatched rows will be redirected to null
            final ColumnSource<?> redirectedColumnSource =
                    RedirectedColumnSource.alwaysRedirect(rowRedirection, rightTable.getColumnSource(mp.rightColumn()));
            if (rightRefreshingColumns) {
                redirectedColumnSource.startTrackingPrevValues();
            }
            columnSourceMap.put(mp.leftColumn(), redirectedColumnSource);
        }
        if (rightRefreshingColumns) {
            if (rowRedirection.isWritable()) {
                rowRedirection.writableCast().startTrackingPrevValues();
            } else {
                ((WritableSingleValueRowRedirection) rowRedirection).startTrackingPrevValues();
            }
        }
        return new QueryTable(leftTable.getRowSet(), columnSourceMap);
    }

    private static class LeftTickingListener extends BaseTable.ListenerImpl {
        final LongArraySource newLeftRedirections;
        private final QueryTable result;
        private final QueryTable leftTable;
        private final WritableRowRedirection rowRedirection;
        private final StaticNaturalJoinStateManager jsm;
        private final ColumnSource<?>[] leftSources;
        private final ModifiedColumnSet leftKeyColumns;
        private final ModifiedColumnSet rightModifiedColumns;
        private final ModifiedColumnSet.Transformer leftTransformer;
        // detects which modified left rows actually changed key value
        private final ChangedKeyRows changedKeyRows;

        LeftTickingListener(String description, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
                QueryTable leftTable, QueryTable result, WritableRowRedirection rowRedirection,
                StaticNaturalJoinStateManager jsm, ColumnSource<?>[] leftSources) {
            super(description, leftTable, result);
            this.result = result;
            this.leftTable = leftTable;
            this.rowRedirection = rowRedirection;
            this.jsm = jsm;
            this.leftSources = leftSources;
            newLeftRedirections = new LongArraySource();
            leftKeyColumns = leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));
            rightModifiedColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));

            leftTransformer =
                    leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());
            changedKeyRows = new ChangedKeyRows(
                    Arrays.stream(leftSources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new));
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            rowRedirection.removeAll(upstream.removed());

            try (final RowSet prevRowSet = leftTable.getRowSet().copyPrev()) {
                rowRedirection.applyShift(prevRowSet, upstream.shifted());
            }

            final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream, result.getModifiedColumnSetForUpdates());
            leftTransformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);

            if (upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(leftKeyColumns)) {
                // only the rows whose key value actually changed need a new redirection; the others were shifted
                // above, so only the post-shift keys of the changed rows are needed
                final RowSetBuilderSequential changedPostShiftBuilder = RowSetFactory.builderSequential();
                changedKeyRows.findChanged(leftSources, upstream.getModifiedPreShift(), upstream.modified(), null,
                        changedPostShiftBuilder, null);
                try (final RowSet changedKeys = changedPostShiftBuilder.build()) {
                    if (changedKeys.isNonempty()) {
                        redirectChangedKeys(changedKeys, downstream.modifiedColumnSet());
                    }
                }
            }

            newLeftRedirections.ensureCapacity(downstream.added().size());
            jsm.decorateLeftSide(downstream.added(), leftSources, newLeftRedirections);
            final MutableInt position = new MutableInt(0);
            downstream.added().forAllRowKeys((long ll) -> {
                final long newRedirection = newLeftRedirections.getLong(position.get());
                jsm.checkExactMatch(ll, newRedirection);
                if (newRedirection != RowSequence.NULL_ROW_KEY) {
                    rowRedirection.putVoid(ll, newRedirection);
                }
                position.increment();
            });

            result.notifyListeners(downstream);
        }

        /**
         * Probe the static right side for the left rows whose key value changed and store their new redirections,
         * marking every added column modified if any redirection differs from before.
         */
        private void redirectChangedKeys(final RowSet changedKeys, final ModifiedColumnSet downstreamColumns) {
            newLeftRedirections.ensureCapacity(changedKeys.size());
            jsm.decorateLeftSide(changedKeys, leftSources, newLeftRedirections);
            final MutableBoolean updatedRightRow = new MutableBoolean(false);
            final MutableInt position = new MutableInt(0);
            changedKeys.forAllRowKeys((long modifiedKey) -> {
                final long newRedirection = newLeftRedirections.getLong(position.get());
                jsm.checkExactMatch(modifiedKey, newRedirection);
                final long old;
                if (newRedirection == RowSequence.NULL_ROW_KEY) {
                    old = rowRedirection.remove(modifiedKey);
                } else {
                    old = rowRedirection.put(modifiedKey, newRedirection);
                }
                if (newRedirection != old) {
                    updatedRightRow.setValue(true);
                }
                position.increment();
            });

            if (updatedRightRow.booleanValue()) {
                downstreamColumns.setAll(rightModifiedColumns);
            }
        }
    }

    private static class RightTickingListener extends BaseTable.ListenerImpl {
        private final QueryTable result;
        private final WritableRowRedirection rowRedirection;
        private final RightIncrementalNaturalJoinStateManager jsm;
        private final ColumnSource<?>[] rightSources;
        private final NaturalJoinType joinType;
        private final ModifiedColumnSet allRightColumns;
        private final ModifiedColumnSet rightKeyColumns;
        private final ModifiedColumnSet.Transformer rightTransformer;
        private final NaturalJoinModifiedSlotTracker modifiedSlotTracker = new NaturalJoinModifiedSlotTracker();

        RightTickingListener(String description, QueryTable rightTable, MatchPair[] columnsToMatch,
                MatchPair[] columnsToAdd, QueryTable result, WritableRowRedirection rowRedirection,
                RightIncrementalNaturalJoinStateManager jsm, ColumnSource<?>[] rightSources,
                NaturalJoinType joinType, boolean rightAddOnly) {
            super(description, rightTable, result);
            this.result = result;
            this.rowRedirection = rowRedirection;
            this.jsm = jsm;
            this.rightSources = rightSources;
            this.joinType = joinType;

            rightKeyColumns = rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
            allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
            rightTransformer = rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            modifiedSlotTracker.clear();

            final boolean addedRightColumnsChanged;

            final int maxSize = UpdateSizeCalculator.chunkSize(upstream, JoinControl.CHUNK_SIZE);
            if (maxSize == 0) {
                Assert.assertion(upstream.empty(), "upstream.empty()");
                return;
            }

            try (final Context pc = jsm.makeProbeContext(rightSources, maxSize)) {
                // the modified rows whose key value actually changed (null when the key columns were not modified);
                // the other modified rows keep their hash slot
                final WritableRowSet changedKeysPreShift;
                final WritableRowSet changedKeysPostShift;

                // We must do all removes before shifting or there will be collisions in the RHS duplicate sets
                if (upstream.removed().isNonempty()) {
                    jsm.removeRight(pc, upstream.removed(), rightSources, modifiedSlotTracker);
                }
                if (upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(rightKeyColumns)) {
                    final RowSetBuilderSequential preShiftBuilder = RowSetFactory.builderSequential();
                    final RowSetBuilderSequential postShiftBuilder = RowSetFactory.builderSequential();
                    jsm.removeRightModifications(rightSources, upstream.getModifiedPreShift(), upstream.modified(),
                            preShiftBuilder, postShiftBuilder, modifiedSlotTracker);
                    changedKeysPreShift = preShiftBuilder.build();
                    changedKeysPostShift = postShiftBuilder.build();
                } else {
                    changedKeysPreShift = null;
                    changedKeysPostShift = null;
                }

                try {
                    if (upstream.shifted().nonempty()) {
                        try (final WritableRowSet previousToShift =
                                getParent().getRowSet().prev().minus(upstream.removed())) {
                            if (changedKeysPreShift != null) {
                                previousToShift.remove(changedKeysPreShift);
                            }
                            upstream.shifted().apply((long beginRange, long endRange, long shiftDelta) -> {
                                try (final WritableRowSet shiftedRowSet =
                                        previousToShift.subSetByKeyRange(beginRange, endRange)) {
                                    shiftedRowSet.shiftInPlace(shiftDelta);
                                    jsm.applyRightShift(pc, rightSources, shiftedRowSet, shiftDelta,
                                            modifiedSlotTracker);
                                }
                            });
                        }
                    }

                    final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                    rightTransformer.clearAndTransform(upstream.modifiedColumnSet(), modifiedColumnSet);
                    addedRightColumnsChanged = modifiedColumnSet.size() != 0;

                    if (changedKeysPostShift != null) {
                        jsm.addRightSide(pc, changedKeysPostShift, rightSources, modifiedSlotTracker);
                        if (addedRightColumnsChanged) {
                            try (final WritableRowSet unchangedKeys =
                                    upstream.modified().minus(changedKeysPostShift)) {
                                jsm.modifyByRight(pc, unchangedKeys, rightSources, modifiedSlotTracker);
                            }
                        }
                    } else if (upstream.modified().isNonempty() && addedRightColumnsChanged) {
                        jsm.modifyByRight(pc, upstream.modified(), rightSources, modifiedSlotTracker);
                    }

                    jsm.addRightSide(pc, upstream.added(), rightSources, modifiedSlotTracker);
                } finally {
                    if (changedKeysPostShift != null) {
                        changedKeysPostShift.close();
                        changedKeysPreShift.close();
                    }
                }
            }

            final RowSetBuilderRandom modifiedLeftBuilder = RowSetFactory.builderRandom();
            final ModifiedSlotUpdater slotUpdater = new ModifiedSlotUpdater(jsm, modifiedLeftBuilder, rowRedirection,
                    joinType, addedRightColumnsChanged);
            modifiedSlotTracker.forAllModifiedSlots(slotUpdater);
            final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            if (slotUpdater.selectedRightRowChanged) {
                modifiedColumnSet.setAll(allRightColumns);
            }

            // left is static, so the only thing that can happen is modifications
            final RowSet modifiedLeft = modifiedLeftBuilder.build();

            result.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(),
                    modifiedLeft, RowSetShiftData.EMPTY,
                    modifiedLeft.isNonempty() ? modifiedColumnSet : ModifiedColumnSet.EMPTY));
        }
    }

    private static class ModifiedSlotUpdater implements NaturalJoinModifiedSlotTracker.ModifiedSlotConsumer {

        private final IncrementalNaturalJoinStateManager jsm;
        private final RowSetBuilderRandom modifiedLeftBuilder;
        private final WritableRowRedirection rowRedirection;
        private final NaturalJoinType joinType;
        private final boolean rightAddedColumnsChanged;
        /**
         * Whether some slot's left rows are now redirected to a different right row than before (as opposed to the same
         * right row at a shifted key), in which case every added column may have a new value.
         */
        boolean selectedRightRowChanged = false;

        private ModifiedSlotUpdater(IncrementalNaturalJoinStateManager jsm, RowSetBuilderRandom modifiedLeftBuilder,
                WritableRowRedirection rowRedirection, NaturalJoinType joinType, boolean rightAddedColumnsChanged) {
            this.jsm = jsm;
            this.modifiedLeftBuilder = modifiedLeftBuilder;
            this.rowRedirection = rowRedirection;
            this.joinType = joinType;
            this.rightAddedColumnsChanged = rightAddedColumnsChanged;
        }

        @Override
        public void accept(int updatedSlot, long originalRightValue, byte flag) {
            final RowSet leftIndices = jsm.getLeftRowSet(updatedSlot);
            if (leftIndices == null || leftIndices.isEmpty()) {
                return;
            }

            long rowKey = jsm.getRightRowKey(updatedSlot);
            if (rowKey == StaticNaturalJoinStateManager.DUPLICATE_RIGHT_VALUE) {
                if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE
                        || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
                    throw new IllegalStateException(
                            "Natural Join found duplicate right key for " + jsm.keyString(updatedSlot));
                }
                // Get the correct row key from the duplicates on the RHS
                final RowSet rightRowSet = jsm.getRightRowSet(updatedSlot);
                rowKey = joinType == NaturalJoinType.FIRST_MATCH
                        ? rightRowSet.firstRowKey()
                        : rightRowSet.lastRowKey();
            }
            final long rightRowKey = rowKey;

            // Whether the redirection stored for the left rows must be rewritten. A shift rewrites it to the same
            // right row at its new key; only a right change (or a right add to a previously unmatched key) selects a
            // different right row.
            final boolean unchangedRedirection;
            if (IncrementalNaturalJoinStateManager.isDuplicateRightState(originalRightValue)) {
                // The slot held several right rows when it was first recorded, and the token does not identify which
                // of them the left rows were redirected to. The flags decide instead: a change or shift of the deciding
                // right row is recorded as such, while a modify probe leaves the redirection alone.
                unchangedRedirection = (flag & (NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE
                        | NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT)) == 0;
            } else {
                unchangedRedirection = rightRowKey == originalRightValue;
            }
            final boolean rightAdded = (flag & NaturalJoinModifiedSlotTracker.FLAG_RIGHT_ADD) != 0;

            // if we have no right columns that have changed, and our redirection is identical we can quit here
            if (unchangedRedirection && !rightAddedColumnsChanged && !rightAdded) {
                return;
            }

            final byte notShift =
                    (~NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT) & NaturalJoinModifiedSlotTracker.FLAG_MASK;
            if ((flag & notShift) != 0) {
                // we do not want to mark the state as modified if the only thing that changed was a shift
                // otherwise we know the left side is modified
                modifiedLeftBuilder.addRowSet(leftIndices);
            }

            // but we might not need to update the row redirection
            if (unchangedRedirection && !rightAdded) {
                return;
            }

            if (rightAdded || (flag & NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE) != 0) {
                selectedRightRowChanged = true;
            }

            if (rightRowKey == RowSequence.NULL_ROW_KEY) {
                jsm.checkExactMatch(leftIndices.firstRowKey(), rightRowKey);
                rowRedirection.removeAll(leftIndices);
            } else {
                leftIndices.forAllRowKeys((long key) -> rowRedirection.putVoid(key, rightRowKey));
            }
        }
    }

    private static class ChunkedMergedJoinListener extends MergedListener {
        private final ColumnSource<?>[] leftSources;
        private final ColumnSource<?>[] rightSources;
        private final JoinListenerRecorder leftRecorder;
        private final JoinListenerRecorder rightRecorder;
        private final WritableRowRedirection rowRedirection;
        private final BothIncrementalNaturalJoinStateManager jsm;
        private final NaturalJoinType joinType;
        private final ModifiedColumnSet rightKeyColumns;
        private final ModifiedColumnSet leftKeyColumns;
        private final ModifiedColumnSet allRightColumns;
        private final ModifiedColumnSet.Transformer rightTransformer;
        private final ModifiedColumnSet.Transformer leftTransformer;
        private final NaturalJoinModifiedSlotTracker modifiedSlotTracker;


        private ChunkedMergedJoinListener(
                QueryTable leftTable,
                QueryTable rightTable,
                ColumnSource<?>[] leftSources,
                ColumnSource<?>[] rightSources,
                MatchPair[] columnsToMatch,
                MatchPair[] columnsToAdd,
                JoinListenerRecorder leftRecorder,
                JoinListenerRecorder rightRecorder,
                QueryTable result,
                WritableRowRedirection rowRedirection,
                BothIncrementalNaturalJoinStateManager jsm,
                NaturalJoinType joinType,
                boolean rightAddOnly,
                String listenerDescription) {
            super(Arrays.asList(leftRecorder, rightRecorder), Collections.emptyList(), listenerDescription, result);
            this.leftSources = leftSources;
            this.rightSources = rightSources;
            this.leftRecorder = leftRecorder;
            this.rightRecorder = rightRecorder;
            this.rowRedirection = rowRedirection;
            this.jsm = jsm;
            this.joinType = joinType;

            rightKeyColumns = rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
            leftKeyColumns = leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));
            allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));

            leftTransformer = leftTable.newModifiedColumnSetTransformer(result,
                    leftTable.getColumnSourceMap().keySet().toArray(String[]::new));
            rightTransformer = rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);
            modifiedSlotTracker = new NaturalJoinModifiedSlotTracker();
        }

        @Override
        protected void process() {
            final RowSetBuilderRandom modifiedLeftBuilder = RowSetFactory.builderRandom();
            final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            modifiedColumnSet.clear();
            modifiedSlotTracker.clear();

            final boolean addedRightColumnsChanged;

            if (rightRecorder.recordedVariablesAreValid()) {
                final RowSet rightAdded = rightRecorder.getAdded();
                final RowSet rightRemoved = rightRecorder.getRemoved();
                final RowSet rightModified = rightRecorder.getModified();
                final ModifiedColumnSet rightModifiedColumns = rightRecorder.getModifiedColumnSet();
                final boolean rightKeysModified =
                        rightModified.isNonempty() && rightModifiedColumns.containsAny(rightKeyColumns);

                final long probeSize =
                        UpdateSizeCalculator.chunkSize(Math.max(rightRemoved.size(), rightModified.size()),
                                rightRecorder.getShifted(), JoinControl.CHUNK_SIZE);
                final long buildSize = Math.max(rightAdded.size(), rightKeysModified ? rightModified.size() : 0);

                // process right updates
                try (final Context pc =
                        probeSize == 0 ? null : jsm.makeProbeContext(rightSources, probeSize);
                        final Context bc =
                                buildSize == 0 ? null : jsm.makeBuildContext(rightSources, buildSize)) {
                    final RowSetShiftData rightShifted = rightRecorder.getShifted();

                    if (rightRemoved.isNonempty()) {
                        jsm.removeRight(pc, rightRemoved, rightSources, modifiedSlotTracker);
                    }

                    rightTransformer.transform(rightModifiedColumns, modifiedColumnSet);
                    addedRightColumnsChanged = modifiedColumnSet.size() > 0;

                    // the modified rows whose key value actually changed (null when the key columns were not
                    // modified); the other modified rows keep their hash slot
                    final WritableRowSet changedKeysPreShift;
                    final WritableRowSet changedKeysPostShift;
                    if (rightKeysModified) {
                        final RowSetBuilderSequential preShiftBuilder = RowSetFactory.builderSequential();
                        final RowSetBuilderSequential postShiftBuilder = RowSetFactory.builderSequential();
                        jsm.removeRightModifications(rightSources, rightRecorder.getModifiedPreShift(),
                                rightModified, preShiftBuilder, postShiftBuilder, modifiedSlotTracker);
                        changedKeysPreShift = preShiftBuilder.build();
                        changedKeysPostShift = postShiftBuilder.build();
                    } else {
                        changedKeysPreShift = null;
                        changedKeysPostShift = null;
                    }

                    try {
                        if (rightShifted.nonempty()) {
                            try (final WritableRowSet previousToShift =
                                    rightRecorder.getParent().getRowSet().prev().minus(rightRemoved)) {
                                if (changedKeysPreShift != null) {
                                    previousToShift.remove(changedKeysPreShift);
                                }
                                rightShifted.apply((long beginRange, long endRange, long shiftDelta) -> {
                                    try (final WritableRowSet shiftedRowSet =
                                            previousToShift.subSetByKeyRange(beginRange, endRange)) {
                                        shiftedRowSet.shiftInPlace(shiftDelta);
                                        jsm.applyRightShift(pc, rightSources, shiftedRowSet, shiftDelta,
                                                modifiedSlotTracker);
                                    }
                                });
                            }
                        }

                        if (changedKeysPostShift != null) {
                            jsm.addRightSide(bc, changedKeysPostShift, rightSources, modifiedSlotTracker);
                            if (addedRightColumnsChanged) {
                                try (final WritableRowSet unchangedKeys =
                                        rightModified.minus(changedKeysPostShift)) {
                                    jsm.modifyByRight(pc, unchangedKeys, rightSources, modifiedSlotTracker);
                                }
                            }
                        } else if (rightModified.isNonempty() && addedRightColumnsChanged) {
                            jsm.modifyByRight(pc, rightModified, rightSources, modifiedSlotTracker);
                        }

                        if (rightAdded.isNonempty()) {
                            jsm.addRightSide(bc, rightAdded, rightSources, modifiedSlotTracker);
                        }
                    } finally {
                        if (changedKeysPostShift != null) {
                            changedKeysPostShift.close();
                            changedKeysPreShift.close();
                        }
                    }
                }
            } else {
                addedRightColumnsChanged = false;
            }

            final RowSet leftAdded = leftRecorder.getAdded();
            final RowSet leftRemoved = leftRecorder.getRemoved();
            final RowSetShiftData leftShifted = leftRecorder.getShifted();

            if (leftRecorder.recordedVariablesAreValid()) {
                final RowSet leftModified = leftRecorder.getModified();
                final ModifiedColumnSet leftModifiedColumns = leftRecorder.getModifiedColumnSet();
                final boolean leftAdditions = leftAdded.isNonempty();
                final boolean leftKeyModifications =
                        leftModified.isNonempty() && leftModifiedColumns.containsAny(leftKeyColumns);

                final WritableRowSet changedKeysPreShift;
                final WritableRowSet changedKeysPostShift;
                if (leftKeyModifications) {
                    final RowSet leftModifiedPreShift =
                            leftShifted.nonempty() ? leftShifted.unapply(leftModified.copy()) : leftModified;
                    final RowSetBuilderSequential preShiftBuilder = RowSetFactory.builderSequential();
                    final RowSetBuilderSequential postShiftBuilder = RowSetFactory.builderSequential();
                    jsm.removeLeftModifications(leftSources, leftModifiedPreShift, leftModified, preShiftBuilder,
                            postShiftBuilder,
                            modifiedSlotTracker);
                    changedKeysPreShift = preShiftBuilder.build();
                    changedKeysPostShift = postShiftBuilder.build();
                    if (leftModifiedPreShift != leftModified) {
                        leftModifiedPreShift.close();
                    }
                } else {
                    changedKeysPreShift = null;
                    changedKeysPostShift = null;
                }
                final boolean leftKeyChanges = changedKeysPostShift != null && changedKeysPostShift.isNonempty();

                final boolean newLeftRedirections = leftAdditions || leftKeyChanges;
                final long buildSize = Math.max(leftAdded.size(), leftKeyChanges ? changedKeysPostShift.size() : 0);
                final long probeSize = UpdateSizeCalculator.chunkSize(leftRemoved.size(), leftShifted,
                        JoinControl.CHUNK_SIZE);

                final LongArraySource leftRedirections = newLeftRedirections ? new LongArraySource() : null;
                if (leftRedirections != null) {
                    leftRedirections.ensureCapacity(buildSize);
                }

                try (final Context pc =
                        probeSize == 0 ? null : jsm.makeProbeContext(leftSources, probeSize);
                        final Context bc =
                                buildSize == 0 ? null : jsm.makeBuildContext(leftSources, buildSize)) {
                    rowRedirection.removeAll(leftRemoved);
                    jsm.removeLeft(pc, leftRemoved, leftSources, modifiedSlotTracker);

                    if (leftKeyChanges) {
                        // the changed rows were already removed from the hash slots by removeLeftModifications above;
                        // here we only need to drop their redirections
                        rowRedirection.removeAll(changedKeysPreShift);
                    }

                    if (leftShifted.nonempty()) {
                        try (final WritableRowSet prevRowSet = leftRecorder.getParent().getRowSet().copyPrev()) {
                            prevRowSet.remove(leftRemoved);

                            if (leftKeyChanges) {
                                prevRowSet.remove(changedKeysPreShift);
                            }

                            final RowSetShiftData.Iterator sit = leftShifted.applyIterator();
                            while (sit.hasNext()) {
                                sit.next();
                                try (final RowSet shiftedRowSet = prevRowSet
                                        .subSetByKeyRange(sit.beginRange(), sit.endRange()).shift(sit.shiftDelta())) {
                                    jsm.applyLeftShift(pc, leftSources, shiftedRowSet, sit.shiftDelta());
                                }
                            }

                            rowRedirection.applyShift(prevRowSet, leftShifted);
                        }
                    }

                    if (leftKeyChanges) {
                        // add the post-shift rows whose key value actually changed
                        jsm.addLeftSide(bc, changedKeysPostShift, leftSources, leftRedirections, modifiedSlotTracker);
                        copyRedirections(changedKeysPostShift, leftRedirections);

                        // TODO: This column mask could be made better if we were to keep more careful track of the
                        // original left hash slots during removal.
                        // We are almost able to fix this, because we know the hash slot and the result redirection for
                        // the left modified row; which is the new value.
                        // We could get the hash slot from the removal, and compare them, but the hash slot outside of a
                        // modified slot tracker is unstable [and we don't want two of them].
                        // On removal, we could ask our modified slot tracker if, (i) our cookie is valid, and if so
                        // (ii) what the original right value was what the right value was
                        // [presuming we add that for right side point 1]. This would let us report our original
                        // row redirection as part of the jsm.removeLeft. We could then compare
                        // the old redirections to the new redirections, only lighting up allRightColumns if there was
                        // indeed a change.
                        modifiedColumnSet.setAll(allRightColumns);
                    }

                    if (leftAdditions) {
                        jsm.addLeftSide(bc, leftAdded, leftSources, leftRedirections, modifiedSlotTracker);
                        copyRedirections(leftAdded, leftRedirections);
                    }
                } finally {
                    if (changedKeysPostShift != null) {
                        changedKeysPostShift.close();
                    }
                    if (changedKeysPreShift != null) {
                        changedKeysPreShift.close();
                    }
                }

                // process left updates
                leftTransformer.transform(leftModifiedColumns, modifiedColumnSet);

                modifiedLeftBuilder.addRowSet(leftModified);
            }

            final ModifiedSlotUpdater slotUpdater = new ModifiedSlotUpdater(jsm, modifiedLeftBuilder, rowRedirection,
                    joinType, addedRightColumnsChanged);
            modifiedSlotTracker.forAllModifiedSlots(slotUpdater);
            if (slotUpdater.selectedRightRowChanged) {
                modifiedColumnSet.setAll(allRightColumns);
            }

            final WritableRowSet modifiedLeft = modifiedLeftBuilder.build();
            modifiedLeft.retain(result.getRowSet());
            modifiedLeft.remove(leftRecorder.getAdded());

            result.notifyListeners(new TableUpdateImpl(leftAdded.copy(), leftRemoved.copy(), modifiedLeft,
                    leftShifted, modifiedColumnSet));
        }

        private void copyRedirections(final RowSet leftRows, @NotNull final LongArraySource leftRedirections) {
            final MutableInt position = new MutableInt(0);
            leftRows.forAllRowKeys((long ll) -> {
                final long rightKey = leftRedirections.getLong(position.get());
                jsm.checkExactMatch(ll, rightKey);
                if (rightKey == RowSequence.NULL_ROW_KEY) {
                    rowRedirection.removeVoid(ll);
                } else {
                    rowRedirection.putVoid(ll, rightKey);
                }
                position.increment();
            });
        }
    }

}
