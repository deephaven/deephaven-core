//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
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
            MatchPair[] columnsToAdd, boolean exactMatch) {
        return naturalJoin(leftTable, rightTable, columnsToMatch, columnsToAdd, exactMatch, new JoinControl());
    }

    @VisibleForTesting
    static Table naturalJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, boolean exactMatch, JoinControl control) {
        final QueryTable result =
                naturalJoinInternal(leftTable, rightTable, columnsToMatch, columnsToAdd, exactMatch, control);
        leftTable.maybeCopyColumnDescriptions(result, rightTable, columnsToMatch, columnsToAdd);
        leftTable.copyAttributes(result, BaseTable.CopyAttributeOperation.Join);
        // note in exact match we require that the right table can match as soon as a row is added to the left
        boolean rightDoesNotGenerateModifies = !rightTable.isRefreshing() || (exactMatch && rightTable.isAddOnly());
        if (leftTable.isAddOnly() && rightDoesNotGenerateModifies) {
            result.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
        }
        if (leftTable.isAppendOnly() && rightDoesNotGenerateModifies) {
            result.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
        }
        return result;
    }

    private static QueryTable naturalJoinInternal(QueryTable leftTable, QueryTable rightTable,
            MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, boolean exactMatch, JoinControl control) {
        QueryTable.checkInitiateBinaryOperation(leftTable, rightTable);

        try (final BucketingContext bc = new BucketingContext("naturalJoin",
                leftTable, rightTable, columnsToMatch, columnsToAdd, control, true, true)) {
            final JoinControl.BuildParameters.From firstBuildFrom = bc.buildParameters.firstBuildFrom();
            final int initialHashTableSize = bc.buildParameters.hashTableSize();

            // if we have a single column of unique values, and the range is small, we can use a simplified table
            // TODO: SimpleUniqueStaticNaturalJoinManager, but not static!
            if (!rightTable.isRefreshing()
                    && control.useUniqueTable(bc.uniqueValues, bc.maximumUniqueValue, bc.minimumUniqueValue)) {
                Assert.neqNull(bc.uniqueFunctor, "uniqueFunctor");
                final SimpleUniqueStaticNaturalJoinStateManager jsm = new SimpleUniqueStaticNaturalJoinStateManager(
                        bc.originalLeftSources, bc.uniqueValuesRange(), bc.uniqueFunctor);
                jsm.setRightSide(rightTable.getRowSet(), bc.rightSources[0]);
                final LongArraySource leftRedirections = new LongArraySource();
                leftRedirections.ensureCapacity(leftTable.getRowSet().size());
                jsm.decorateLeftSide(leftTable.getRowSet(), bc.leftSources, leftRedirections);

                final WritableRowRedirection rowRedirection = jsm.buildRowRedirection(leftTable, exactMatch,
                        leftRedirections, control.getRedirectionType(leftTable));

                final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, true);
                if (leftTable.isRefreshing()) {
                    leftTable.addUpdateListener(new LeftTickingListener(bc.listenerDescription, columnsToMatch,
                            columnsToAdd, leftTable, result, rowRedirection, jsm, bc.leftSources));
                }
                return result;
            }

            if (bc.leftSources.length == 0) {
                return zeroKeyColumnsJoin(leftTable, rightTable, columnsToAdd, exactMatch, bc.listenerDescription);
            }

            final WritableRowRedirection rowRedirection;

            if (leftTable.isRefreshing() && rightTable.isRefreshing()) {
                // We always build right first, regardless of the build parameters. This is probably irrelevant.

                final BothIncrementalNaturalJoinStateManager jsm = TypedHasherFactory.make(
                        IncrementalNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        initialHashTableSize, control.getMaximumLoadFactor(),
                        control.getTargetLoadFactor());
                jsm.buildFromRightSide(rightTable, bc.rightSources);

                try (final BothIncrementalNaturalJoinStateManager.InitialBuildContext ibc =
                        jsm.makeInitialBuildContext()) {

                    if (bc.leftDataIndexTable != null) {
                        jsm.decorateLeftSide(bc.leftDataIndexTable.getRowSet(), bc.leftDataIndexSources, ibc);
                        rowRedirection = jsm.buildIndexedRowRedirection(leftTable, exactMatch, ibc,
                                bc.leftDataIndexRowSetSource, control.getRedirectionType(leftTable));
                    } else {
                        jsm.decorateLeftSide(leftTable.getRowSet(), bc.leftSources, ibc);
                        jsm.compactAll();
                        rowRedirection = jsm.buildRowRedirectionFromRedirections(leftTable, exactMatch, ibc,
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
                        leftRecorder, rightRecorder, result, rowRedirection, jsm, exactMatch, bc.listenerDescription);
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
                final StaticHashedNaturalJoinStateManager jsm = TypedHasherFactory.make(
                        StaticNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        initialHashTableSize, control.getMaximumLoadFactor(),
                        control.getTargetLoadFactor());

                jsm.buildFromRightSide(rightTable, bc.rightSources);
                if (bc.leftDataIndexTable != null) {
                    jsm.decorateLeftSide(bc.leftDataIndexTable.getRowSet(), bc.leftDataIndexSources, leftRedirections);
                    rowRedirection = jsm.buildIndexedRowRedirectionFromRedirections(leftTable, exactMatch,
                            bc.leftDataIndexTable.getRowSet(), leftRedirections, bc.leftDataIndexRowSetSource,
                            control.getRedirectionType(leftTable));
                } else {
                    jsm.decorateLeftSide(leftTable.getRowSet(), bc.leftSources, leftRedirections);
                    rowRedirection = jsm.buildRowRedirectionFromRedirections(leftTable, exactMatch, leftRedirections,
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

                final RightIncrementalNaturalJoinStateManager jsm = TypedHasherFactory.make(
                        RightIncrementalNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        initialHashTableSize, control.getMaximumLoadFactor(),
                        control.getTargetLoadFactor());
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
                    rowRedirection = jsm.buildRowRedirectionFromHashSlotIndexed(leftTable,
                            bc.leftDataIndexRowSetSource, bc.leftDataIndexTable.intSize(),
                            exactMatch, ibc, control.getRedirectionType(leftTable));
                } else {
                    rowRedirection = jsm.buildRowRedirectionFromHashSlot(leftTable, exactMatch, ibc,
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
                                exactMatch));
                return result;
            }

            if (firstBuildFrom == LeftDataIndex) {
                Assert.neqNull(bc.leftDataIndexTable, "leftDataIndexTable");
                final StaticHashedNaturalJoinStateManager jsm = TypedHasherFactory.make(
                        StaticNaturalJoinStateManagerTypedBase.class, bc.leftDataIndexSources,
                        bc.originalLeftDataIndexSources, initialHashTableSize,
                        control.getMaximumLoadFactor(), control.getTargetLoadFactor());

                final IntegerArraySource leftHashSlots = new IntegerArraySource();
                jsm.buildFromLeftSide(bc.leftDataIndexTable, bc.leftDataIndexSources,
                        leftHashSlots);
                jsm.decorateWithRightSide(rightTable, bc.rightSources);
                rowRedirection = jsm.buildIndexedRowRedirectionFromHashSlots(leftTable, exactMatch,
                        bc.leftDataIndexTable.getRowSet(), leftHashSlots,
                        bc.leftDataIndexRowSetSource, control.getRedirectionType(leftTable));
            } else if (firstBuildFrom == LeftInput) {
                final StaticHashedNaturalJoinStateManager jsm = TypedHasherFactory.make(
                        StaticNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        // The static state manager doesn't allow rehashing, so we must allocate a big enough hash
                        // table for the possibility that all left rows will have unique keys.
                        control.tableSize(leftTable.size()),
                        control.getMaximumLoadFactor(), control.getTargetLoadFactor());
                final IntegerArraySource leftHashSlots = new IntegerArraySource();
                jsm.buildFromLeftSide(leftTable, bc.leftSources, leftHashSlots);
                try {
                    jsm.decorateWithRightSide(rightTable, bc.rightSources);
                } catch (DuplicateRightRowDecorationException e) {
                    jsm.errorOnDuplicatesSingle(leftHashSlots, leftTable.size(), leftTable.getRowSet());
                }
                rowRedirection = jsm.buildRowRedirectionFromHashSlot(leftTable, exactMatch, leftHashSlots,
                        control.getRedirectionType(leftTable));
            } else {
                final LongArraySource leftRedirections = new LongArraySource();
                final StaticHashedNaturalJoinStateManager jsm = TypedHasherFactory.make(
                        StaticNaturalJoinStateManagerTypedBase.class, bc.leftSources, bc.originalLeftSources,
                        initialHashTableSize, control.getMaximumLoadFactor(),
                        control.getTargetLoadFactor());

                jsm.buildFromRightSide(rightTable, bc.rightSources);
                jsm.decorateLeftSide(leftTable.getRowSet(), bc.leftSources, leftRedirections);
                rowRedirection = jsm.buildRowRedirectionFromRedirections(leftTable, exactMatch, leftRedirections,
                        control.getRedirectionType(leftTable));
            }
            return makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, false);
        }
    }

    @NotNull
    private static QueryTable zeroKeyColumnsJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToAdd,
            boolean exactMatch, String listenerDescription) {
        // we are a single value join, we do not need to do any work
        final SingleValueRowRedirection rowRedirection;

        final boolean rightRefreshing = rightTable.isRefreshing();

        if (rightTable.size() > 1) {
            if (!leftTable.isEmpty()) {
                throw new RuntimeException(
                        "naturalJoin with zero key columns may not have more than one row in the right hand side table!");
            }
            // we don't care where it goes
            rowRedirection = getSingleValueRowRedirection(rightRefreshing, RowSequence.NULL_ROW_KEY);
        } else if (rightTable.size() == 1) {
            rowRedirection = getSingleValueRowRedirection(rightRefreshing, rightTable.getRowSet().firstRowKey());
        } else {
            if (exactMatch && !leftTable.isEmpty()) {
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

                        checkRightTableSizeZeroKeys(leftTable, rightTable, exactMatch);

                        if (rightChanged) {
                            final boolean rightUpdated = updateRightRedirection(rightTable, rowRedirection);
                            if (rightUpdated) {
                                modifiedColumnSet.setAll(allRightColumns);
                            } else {
                                rightTransformer.transform(rightRecorder.getModifiedColumnSet(), modifiedColumnSet);
                            }
                        }

                        if (leftChanged) {
                            final RowSet modified;
                            if (rightChanged) {
                                modified = result.getRowSet().minus(leftRecorder.getAdded());
                            } else {
                                modified = leftRecorder.getModified().copy();
                            }
                            leftTransformer.transform(leftRecorder.getModifiedColumnSet(), modifiedColumnSet);
                            result.notifyListeners(new TableUpdateImpl(
                                    leftRecorder.getAdded().copy(), leftRecorder.getRemoved().copy(), modified,
                                    leftRecorder.getShifted(), modifiedColumnSet));
                        } else if (rightChanged) {
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
                                checkRightTableSizeZeroKeys(leftTable, rightTable, exactMatch);
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
                                checkRightTableSizeZeroKeys(leftTable, rightTable, exactMatch);
                                final boolean changed = updateRightRedirection(rightTable, rowRedirection);
                                final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                                if (!changed) {
                                    rightTransformer.clearAndTransform(upstream.modifiedColumnSet(), modifiedColumnSet);
                                }
                                result.notifyListeners(
                                        new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(),
                                                result.getRowSet().copy(), RowSetShiftData.EMPTY,
                                                changed ? allRightColumns : modifiedColumnSet));
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

    private static boolean updateRightRedirection(QueryTable rightTable, SingleValueRowRedirection rowRedirection) {
        final boolean changed;
        if (rightTable.isEmpty()) {
            changed = rowRedirection.getValue() != RowSequence.NULL_ROW_KEY;
            if (changed) {
                rowRedirection.writableSingleValueCast().setValue(RowSequence.NULL_ROW_KEY);
            }
        } else {
            final long value = rightTable.getRowSet().firstRowKey();
            changed = rowRedirection.getValue() != value;
            if (changed) {
                rowRedirection.writableSingleValueCast().setValue(value);
            }
        }
        return changed;
    }

    private static void checkRightTableSizeZeroKeys(final Table leftTable, final Table rightTable, boolean exactMatch) {
        if (!leftTable.isEmpty()) {
            if (rightTable.size() > 1) {
                throw new RuntimeException(
                        "naturalJoin with zero key columns may not have more than one row in the right hand side table!");
            } else if (rightTable.isEmpty() && exactMatch) {
                throw new RuntimeException(
                        "exactJoin with zero key columns must have exactly one row in the right hand side table!");
            }
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
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            rowRedirection.removeAll(upstream.removed());

            try (final RowSet prevRowSet = leftTable.getRowSet().copyPrev()) {
                rowRedirection.applyShift(prevRowSet, upstream.shifted());
            }

            final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream, result.getModifiedColumnSetForUpdates());
            leftTransformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);

            if (upstream.modifiedColumnSet().containsAny(leftKeyColumns)) {
                newLeftRedirections.ensureCapacity(downstream.modified().size());
                // compute our new values
                jsm.decorateLeftSide(downstream.modified(), leftSources, newLeftRedirections);
                final MutableBoolean updatedRightRow = new MutableBoolean(false);
                final MutableInt position = new MutableInt(0);
                downstream.modified().forAllRowKeys((long modifiedKey) -> {
                    final long newRedirection = newLeftRedirections.getLong(position.get());
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
                    downstream.modifiedColumnSet().setAll(rightModifiedColumns);
                }
            }

            newLeftRedirections.ensureCapacity(downstream.added().size());
            jsm.decorateLeftSide(downstream.added(), leftSources, newLeftRedirections);
            final MutableInt position = new MutableInt(0);
            downstream.added().forAllRowKeys((long ll) -> {
                final long newRedirection = newLeftRedirections.getLong(position.get());
                if (newRedirection != RowSequence.NULL_ROW_KEY) {
                    rowRedirection.putVoid(ll, newRedirection);
                }
                position.increment();
            });

            result.notifyListeners(downstream);
        }
    }

    private static class RightTickingListener extends BaseTable.ListenerImpl {
        private final QueryTable result;
        private final WritableRowRedirection rowRedirection;
        private final RightIncrementalNaturalJoinStateManager jsm;
        private final ColumnSource<?>[] rightSources;
        private final boolean exactMatch;
        private final ModifiedColumnSet allRightColumns;
        private final ModifiedColumnSet rightKeyColumns;
        private final ModifiedColumnSet.Transformer rightTransformer;
        private final NaturalJoinModifiedSlotTracker modifiedSlotTracker = new NaturalJoinModifiedSlotTracker();

        RightTickingListener(String description, QueryTable rightTable, MatchPair[] columnsToMatch,
                MatchPair[] columnsToAdd, QueryTable result, WritableRowRedirection rowRedirection,
                RightIncrementalNaturalJoinStateManager jsm, ColumnSource<?>[] rightSources,
                boolean exactMatch) {
            super(description, rightTable, result);
            this.result = result;
            this.rowRedirection = rowRedirection;
            this.jsm = jsm;
            this.rightSources = rightSources;
            this.exactMatch = exactMatch;

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
                final RowSet modifiedPreShift;

                final boolean rightKeysChanged = upstream.modifiedColumnSet().containsAny(rightKeyColumns);

                if (rightKeysChanged) {
                    modifiedPreShift = upstream.getModifiedPreShift();
                } else {
                    modifiedPreShift = null;
                }

                if (upstream.shifted().nonempty()) {
                    try (final WritableRowSet previousToShift =
                            getParent().getRowSet().prev().minus(upstream.removed())) {
                        if (rightKeysChanged) {
                            previousToShift.remove(modifiedPreShift);
                        }
                        upstream.shifted().apply((long beginRange, long endRange, long shiftDelta) -> {
                            try (final WritableRowSet shiftedRowSet =
                                    previousToShift.subSetByKeyRange(beginRange, endRange)) {
                                shiftedRowSet.shiftInPlace(shiftDelta);
                                jsm.applyRightShift(pc, rightSources, shiftedRowSet, shiftDelta, modifiedSlotTracker);
                            }
                        });
                    }
                }

                jsm.removeRight(pc, upstream.removed(), rightSources, modifiedSlotTracker);

                final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                rightTransformer.clearAndTransform(upstream.modifiedColumnSet(), modifiedColumnSet);
                addedRightColumnsChanged = modifiedColumnSet.size() != 0;

                if (rightKeysChanged) {
                    // It should make us somewhat sad that we have to add/remove, because we are doing two hash lookups
                    // for keys that have not actually changed.
                    // The alternative would be to do an initial pass that would filter out key columns that have not
                    // actually changed.
                    jsm.removeRight(pc, modifiedPreShift, rightSources, modifiedSlotTracker);
                    jsm.addRightSide(pc, upstream.modified(), rightSources, modifiedSlotTracker);
                } else {
                    if (upstream.modified().isNonempty() && addedRightColumnsChanged) {
                        jsm.modifyByRight(pc, upstream.modified(), rightSources, modifiedSlotTracker);
                    }
                }

                jsm.addRightSide(pc, upstream.added(), rightSources, modifiedSlotTracker);
            }

            final RowSetBuilderRandom modifiedLeftBuilder = RowSetFactory.builderRandom();
            final ModifiedSlotUpdater slotUpdater = new ModifiedSlotUpdater(jsm, modifiedLeftBuilder, rowRedirection,
                    exactMatch, addedRightColumnsChanged);
            modifiedSlotTracker.forAllModifiedSlots(slotUpdater);
            final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            if (slotUpdater.changedRedirection) {
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
        private final boolean exactMatch;
        private final boolean rightAddedColumnsChanged;
        boolean changedRedirection = false;

        private ModifiedSlotUpdater(IncrementalNaturalJoinStateManager jsm, RowSetBuilderRandom modifiedLeftBuilder,
                WritableRowRedirection rowRedirection, boolean exactMatch, boolean rightAddedColumnsChanged) {
            this.jsm = jsm;
            this.modifiedLeftBuilder = modifiedLeftBuilder;
            this.rowRedirection = rowRedirection;
            this.exactMatch = exactMatch;
            this.rightAddedColumnsChanged = rightAddedColumnsChanged;
        }

        @Override
        public void accept(int updatedSlot, long originalRightValue, byte flag) {
            final RowSet leftIndices = jsm.getLeftIndex(updatedSlot);
            if (leftIndices == null || leftIndices.isEmpty()) {
                return;
            }

            final long rightIndex = jsm.getRightIndex(updatedSlot);

            if (rightIndex == StaticNaturalJoinStateManager.DUPLICATE_RIGHT_VALUE) {
                throw new IllegalStateException(
                        "Natural Join found duplicate right key for " + jsm.keyString(updatedSlot));
            }

            final boolean unchangedRedirection = rightIndex == originalRightValue;

            // if we have no right columns that have changed, and our redirection is identical we can quit here
            if (unchangedRedirection && !rightAddedColumnsChanged
                    && (flag & NaturalJoinModifiedSlotTracker.FLAG_RIGHT_ADD) == 0) {
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
            if (unchangedRedirection && (flag & NaturalJoinModifiedSlotTracker.FLAG_RIGHT_ADD) == 0) {
                return;
            }

            changedRedirection = true;

            if (rightIndex == RowSequence.NULL_ROW_KEY) {
                jsm.checkExactMatch(exactMatch, leftIndices.firstRowKey(), rightIndex);
                rowRedirection.removeAll(leftIndices);
            } else {
                leftIndices.forAllRowKeys((long key) -> rowRedirection.putVoid(key, rightIndex));
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
        private final boolean exactMatch;
        private final ModifiedColumnSet rightKeyColumns;
        private final ModifiedColumnSet leftKeyColumns;
        private final ModifiedColumnSet allRightColumns;
        private final ModifiedColumnSet.Transformer rightTransformer;
        private final ModifiedColumnSet.Transformer leftTransformer;
        private final NaturalJoinModifiedSlotTracker modifiedSlotTracker;


        private ChunkedMergedJoinListener(QueryTable leftTable,
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
                boolean exactMatch,
                String listenerDescription) {
            super(Arrays.asList(leftRecorder, rightRecorder), Collections.emptyList(), listenerDescription, result);
            this.leftSources = leftSources;
            this.rightSources = rightSources;
            this.leftRecorder = leftRecorder;
            this.rightRecorder = rightRecorder;
            this.rowRedirection = rowRedirection;
            this.jsm = jsm;
            this.exactMatch = exactMatch;

            rightKeyColumns = rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
            leftKeyColumns = leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));
            allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));

            leftTransformer = leftTable.newModifiedColumnSetTransformer(result,
                    leftTable.getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
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
                    final RowSet modifiedPreShift;

                    final RowSetShiftData rightShifted = rightRecorder.getShifted();

                    if (rightKeysModified) {
                        modifiedPreShift = rightRecorder.getModifiedPreShift();
                    } else {
                        modifiedPreShift = null;
                    }

                    if (rightRemoved.isNonempty()) {
                        jsm.removeRight(pc, rightRemoved, rightSources, modifiedSlotTracker);
                    }

                    rightTransformer.transform(rightModifiedColumns, modifiedColumnSet);
                    addedRightColumnsChanged = modifiedColumnSet.size() > 0;

                    if (rightKeysModified) {
                        // It should make us somewhat sad that we have to add/remove, because we are doing two hash
                        // lookups for keys that have not actually changed.
                        // The alternative would be to do an initial pass that would filter out key columns that have
                        // not actually changed.
                        jsm.removeRight(pc, modifiedPreShift, rightSources, modifiedSlotTracker);
                    }

                    if (rightShifted.nonempty()) {
                        try (final WritableRowSet previousToShift =
                                rightRecorder.getParent().getRowSet().prev().minus(rightRemoved)) {
                            if (rightKeysModified) {
                                previousToShift.remove(modifiedPreShift);
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

                    if (rightKeysModified) {
                        jsm.addRightSide(bc, rightModified, rightSources, modifiedSlotTracker);
                    } else if (rightModified.isNonempty() && addedRightColumnsChanged) {
                        jsm.modifyByRight(pc, rightModified, rightSources, modifiedSlotTracker);
                    }

                    if (rightAdded.isNonempty()) {
                        jsm.addRightSide(bc, rightAdded, rightSources, modifiedSlotTracker);
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
                final boolean newLeftRedirections = leftAdditions || leftKeyModifications;
                final long buildSize = Math.max(leftAdded.size(), leftKeyModifications ? leftModified.size() : 0);
                final long probeSize = UpdateSizeCalculator.chunkSize(
                        Math.max(leftRemoved.size(), leftKeyModifications ? leftModified.size() : 0), leftShifted,
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
                    jsm.removeLeft(pc, leftRemoved, leftSources);

                    final RowSet leftModifiedPreShift;
                    if (leftKeyModifications) {
                        if (leftShifted.nonempty()) {
                            leftModifiedPreShift = leftShifted.unapply(leftModified.copy());
                        } else {
                            leftModifiedPreShift = leftModified;
                        }

                        // remove pre-shift modified
                        jsm.removeLeft(pc, leftModifiedPreShift, leftSources);
                        rowRedirection.removeAll(leftModifiedPreShift);
                    } else {
                        leftModifiedPreShift = null;
                    }

                    if (leftShifted.nonempty()) {
                        try (final WritableRowSet prevRowSet = leftRecorder.getParent().getRowSet().copyPrev()) {
                            prevRowSet.remove(leftRemoved);

                            if (leftKeyModifications) {
                                prevRowSet.remove(leftModifiedPreShift);
                                leftModifiedPreShift.close();
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

                    if (leftKeyModifications) {
                        // add post-shift modified
                        jsm.addLeftSide(bc, leftModified, leftSources, leftRedirections, modifiedSlotTracker);
                        copyRedirections(leftModified, leftRedirections);

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
                }

                // process left updates
                leftTransformer.transform(leftModifiedColumns, modifiedColumnSet);

                modifiedLeftBuilder.addRowSet(leftModified);
            }

            final ModifiedSlotUpdater slotUpdater = new ModifiedSlotUpdater(jsm, modifiedLeftBuilder, rowRedirection,
                    exactMatch, addedRightColumnsChanged);
            modifiedSlotTracker.forAllModifiedSlots(slotUpdater);
            if (slotUpdater.changedRedirection) {
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
                jsm.checkExactMatch(exactMatch, ll, rightKey);
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
