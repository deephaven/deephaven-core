/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.join.JoinListenerRecorder;
import io.deephaven.engine.table.impl.naturaljoin.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.*;

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

        try (final BucketingContext bucketingContext =
                new BucketingContext("naturalJoin", leftTable, rightTable, columnsToMatch, columnsToAdd, control)) {

            // if we have a single column of unique values, and the range is small, we can use a simplified table
            // TODO: SimpleUniqueStaticNaturalJoinManager, but not static!
            if (!rightTable.isRefreshing() && control.useUniqueTable(bucketingContext.uniqueValues,
                    bucketingContext.maximumUniqueValue, bucketingContext.minimumUniqueValue)) {
                Assert.neqNull(bucketingContext.uniqueFunctor, "uniqueFunctor");
                final SimpleUniqueStaticNaturalJoinStateManager jsm = new SimpleUniqueStaticNaturalJoinStateManager(
                        bucketingContext.originalLeftSources, bucketingContext.uniqueValuesRange(),
                        bucketingContext.uniqueFunctor);
                jsm.setRightSide(rightTable.getRowSet(), bucketingContext.rightSources[0]);
                final LongArraySource leftRedirections = new LongArraySource();
                leftRedirections.ensureCapacity(leftTable.getRowSet().size());
                jsm.decorateLeftSide(leftTable.getRowSet(), bucketingContext.leftSources, leftRedirections);

                final WritableRowRedirection rowRedirection = jsm.buildRowRedirection(leftTable, exactMatch,
                        leftRedirections, control.getRedirectionType(leftTable));

                final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, true);

                leftTable
                        .addUpdateListener(new LeftTickingListener(bucketingContext.listenerDescription, columnsToMatch,
                                columnsToAdd, leftTable, result, rowRedirection, jsm, bucketingContext.leftSources));

                return result;
            }

            if (bucketingContext.leftSources.length == 0) {
                return zeroKeyColumnsJoin(leftTable, rightTable, columnsToAdd, exactMatch,
                        bucketingContext.listenerDescription);
            }

            final WritableRowRedirection rowRedirection;
            if (rightTable.isRefreshing()) {
                if (leftTable.isRefreshing()) {
                    if (bucketingContext.useLeftGrouping) {
                        throw new UnsupportedOperationException(
                                "Grouping is not supported with ticking chunked naturalJoin!");
                    }

                    // the right side is unique, so we should have a state for it; the left side can have many
                    // duplicates
                    // so we would prefer to have a smaller table
                    final int tableSize = control.tableSizeForRightBuild(rightTable);

                    final BothIncrementalNaturalJoinStateManager jsm =
                            TypedHasherFactory.make(IncrementalNaturalJoinStateManagerTypedBase.class,
                                    bucketingContext.leftSources, bucketingContext.originalLeftSources,
                                    tableSize, control.getMaximumLoadFactor(),
                                    control.getTargetLoadFactor());
                    jsm.buildFromRightSide(rightTable, bucketingContext.rightSources);

                    try (final BothIncrementalNaturalJoinStateManager.InitialBuildContext ibc =
                            jsm.makeInitialBuildContext()) {
                        jsm.decorateLeftSide(leftTable.getRowSet(), bucketingContext.leftSources, ibc);

                        jsm.compactAll();

                        rowRedirection = jsm.buildRowRedirectionFromRedirections(leftTable, exactMatch, ibc,
                                control.getRedirectionType(leftTable));
                    }

                    final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, true);

                    final JoinListenerRecorder leftRecorder =
                            new JoinListenerRecorder(true, bucketingContext.listenerDescription, leftTable, result);
                    final JoinListenerRecorder rightRecorder =
                            new JoinListenerRecorder(false, bucketingContext.listenerDescription, rightTable, result);

                    final ChunkedMergedJoinListener mergedJoinListener = new ChunkedMergedJoinListener(
                            leftTable, rightTable, bucketingContext.leftSources, bucketingContext.rightSources,
                            columnsToMatch,
                            columnsToAdd, leftRecorder, rightRecorder, result, rowRedirection, jsm, exactMatch,
                            bucketingContext.listenerDescription);
                    leftRecorder.setMergedListener(mergedJoinListener);
                    rightRecorder.setMergedListener(mergedJoinListener);

                    leftTable.addUpdateListener(leftRecorder);
                    rightTable.addUpdateListener(rightRecorder);

                    result.addParentReference(mergedJoinListener);

                    return result;
                } else {
                    // right is live, left is static
                    final RightIncrementalNaturalJoinStateManager jsm =
                            TypedHasherFactory.make(RightIncrementalNaturalJoinStateManagerTypedBase.class,
                                    bucketingContext.leftSources, bucketingContext.originalLeftSources,
                                    control.tableSizeForLeftBuild(leftTable),
                                    control.getMaximumLoadFactor(), control.getTargetLoadFactor());
                    RightIncrementalNaturalJoinStateManager.InitialBuildContext initialBuildContext =
                            jsm.makeInitialBuildContext(leftTable);

                    final ObjectArraySource<WritableRowSet> rowSetSource;
                    final MutableInt groupingSize = new MutableInt();
                    if (bucketingContext.useLeftGrouping) {
                        final Map<?, RowSet> grouping =
                                bucketingContext.leftSources[0].getGroupToRange(leftTable.getRowSet());

                        // noinspection unchecked,rawtypes
                        final Pair<ArrayBackedColumnSource<?>, ObjectArraySource<WritableRowSet>> flatResultColumnSources =
                                GroupingUtils.groupingToFlatSources(
                                        (ColumnSource) bucketingContext.leftSources[0], grouping, leftTable.getRowSet(),
                                        groupingSize);
                        final ArrayBackedColumnSource<?> groupSource = flatResultColumnSources.getFirst();
                        rowSetSource = flatResultColumnSources.getSecond();

                        final Table leftTableGrouped = new QueryTable(
                                RowSetFactory.flat(groupingSize.intValue()).toTracking(),
                                Collections.singletonMap(columnsToMatch[0].leftColumn(), groupSource));

                        final ColumnSource<?>[] groupedSourceArray = {groupSource};
                        jsm.buildFromLeftSide(leftTableGrouped, groupedSourceArray, initialBuildContext);
                        jsm.convertLeftGroups(groupingSize.intValue(), initialBuildContext, rowSetSource);
                    } else {
                        jsm.buildFromLeftSide(leftTable, bucketingContext.leftSources, initialBuildContext);
                        rowSetSource = null;
                    }

                    jsm.addRightSide(rightTable.getRowSet(), bucketingContext.rightSources);

                    if (bucketingContext.useLeftGrouping) {
                        rowRedirection = jsm.buildRowRedirectionFromHashSlotGrouped(leftTable, rowSetSource,
                                groupingSize.intValue(), exactMatch, initialBuildContext,
                                control.getRedirectionType(leftTable));
                    } else {
                        rowRedirection = jsm.buildRowRedirectionFromHashSlot(leftTable, exactMatch, initialBuildContext,
                                control.getRedirectionType(leftTable));
                    }

                    final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, true);

                    rightTable.addUpdateListener(
                            new RightTickingListener(
                                    bucketingContext.listenerDescription,
                                    rightTable,
                                    columnsToMatch,
                                    columnsToAdd,
                                    result,
                                    rowRedirection,
                                    jsm,
                                    bucketingContext.rightSources,
                                    exactMatch));
                    return result;
                }
            } else {
                if (bucketingContext.useLeftGrouping) {
                    if (leftTable.isRefreshing()) {
                        throw new UnsupportedOperationException(
                                "Grouping information is not supported when tables are refreshing!");
                    }

                    final Map<?, RowSet> grouping =
                            bucketingContext.leftSources[0].getGroupToRange(leftTable.getRowSet());

                    final MutableInt groupingSize = new MutableInt();
                    // noinspection unchecked,rawtypes
                    final Pair<ArrayBackedColumnSource<?>, ObjectArraySource<RowSet>> flatResultColumnSources =
                            GroupingUtils.groupingToFlatSources((ColumnSource) bucketingContext.leftSources[0],
                                    grouping, leftTable.getRowSet(), groupingSize);
                    final ArrayBackedColumnSource<?> groupSource = flatResultColumnSources.getFirst();
                    final ObjectArraySource<RowSet> rowSetSource = flatResultColumnSources.getSecond();

                    final Table leftTableGrouped = new QueryTable(
                            RowSetFactory.flat(groupingSize.intValue()).toTracking(),
                            Collections.singletonMap(columnsToMatch[0].leftColumn(), groupSource));

                    final ColumnSource<?>[] groupedSourceArray = {groupSource};
                    final StaticHashedNaturalJoinStateManager jsm =
                            TypedHasherFactory.make(StaticNaturalJoinStateManagerTypedBase.class, groupedSourceArray,
                                    groupedSourceArray,
                                    control.tableSize(groupingSize.intValue()),
                                    control.getMaximumLoadFactor(), control.getTargetLoadFactor());
                    final IntegerArraySource leftHashSlots = new IntegerArraySource();
                    jsm.buildFromLeftSide(leftTableGrouped, groupedSourceArray, leftHashSlots);
                    try {
                        jsm.decorateWithRightSide(rightTable, bucketingContext.rightSources);
                    } catch (DuplicateRightRowDecorationException e) {
                        jsm.errorOnDuplicatesGrouped(leftHashSlots, leftTableGrouped.size(), rowSetSource);
                    }
                    rowRedirection = jsm.buildGroupedRowRedirection(leftTable, exactMatch, leftTableGrouped.size(),
                            leftHashSlots, rowSetSource, control.getRedirectionType(leftTable));
                } else if (control.buildLeft(leftTable, rightTable)) {
                    final StaticHashedNaturalJoinStateManager jsm =
                            TypedHasherFactory.make(StaticNaturalJoinStateManagerTypedBase.class,
                                    bucketingContext.leftSources, bucketingContext.originalLeftSources,
                                    control.tableSizeForLeftBuild(leftTable),
                                    control.getMaximumLoadFactor(), control.getTargetLoadFactor());
                    final IntegerArraySource leftHashSlots = new IntegerArraySource();
                    jsm.buildFromLeftSide(leftTable, bucketingContext.leftSources, leftHashSlots);
                    try {
                        jsm.decorateWithRightSide(rightTable, bucketingContext.rightSources);
                    } catch (DuplicateRightRowDecorationException e) {
                        jsm.errorOnDuplicatesSingle(leftHashSlots, leftTable.size(), leftTable.getRowSet());
                    }
                    rowRedirection = jsm.buildRowRedirectionFromHashSlot(leftTable, exactMatch, leftHashSlots,
                            control.getRedirectionType(leftTable));
                } else {
                    final LongArraySource leftRedirections = new LongArraySource();
                    final StaticHashedNaturalJoinStateManager jsm =
                            TypedHasherFactory.make(StaticNaturalJoinStateManagerTypedBase.class,
                                    bucketingContext.leftSources, bucketingContext.originalLeftSources,
                                    control.tableSizeForRightBuild(rightTable),
                                    control.getMaximumLoadFactor(), control.getTargetLoadFactor());
                    jsm.buildFromRightSide(rightTable, bucketingContext.rightSources);
                    jsm.decorateLeftSide(leftTable.getRowSet(), bucketingContext.leftSources, leftRedirections);
                    rowRedirection = jsm.buildRowRedirectionFromRedirections(leftTable, exactMatch, leftRedirections,
                            control.getRedirectionType(leftTable));

                    final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, rowRedirection, true);

                    leftTable.addUpdateListener(
                            new LeftTickingListener(
                                    bucketingContext.listenerDescription,
                                    columnsToMatch,
                                    columnsToAdd,
                                    leftTable,
                                    result,
                                    rowRedirection,
                                    jsm,
                                    bucketingContext.leftSources));
                    return result;
                }
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
            if (leftTable.size() > 0) {
                throw new RuntimeException(
                        "naturalJoin with zero key columns may not have more than one row in the right hand side table!");
            }
            // we don't care where it goes
            rowRedirection = getSingleValueRowRedirection(rightRefreshing, RowSequence.NULL_ROW_KEY);
        } else if (rightTable.size() == 1) {
            rowRedirection = getSingleValueRowRedirection(rightRefreshing, rightTable.getRowSet().firstRowKey());
        } else {
            if (exactMatch && leftTable.size() > 0) {
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
                                final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);
                                downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                                leftTransformer.clearAndTransform(upstream.modifiedColumnSet(),
                                        downstream.modifiedColumnSet);
                                result.notifyListeners(downstream);
                            }
                        });
            }
        } else if (rightTable.isRefreshing()) {
            if (leftTable.size() > 0) {
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
        if (rightTable.size() == 0) {
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
        if (leftTable.size() != 0) {
            if (rightTable.size() > 1) {
                throw new RuntimeException(
                        "naturalJoin with zero key columns may not have more than one row in the right hand side table!");
            } else if (rightTable.size() == 0 && exactMatch) {
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

    /**
     * This column source is used as a wrapper for the original table's symbol sources.
     *
     * The symbol sources are reinterpreted to longs, and then the SymbolCombiner produces an IntegerSparseArraySource
     * for each side. To convert from the symbol table value, we simply look it up in the symbolLookup source and use
     * that as our chunked result.
     */
    static class SymbolTableToUniqueIdSource extends AbstractColumnSource<Integer>
            implements ImmutableColumnSourceGetDefaults.ForInt {
        private final ColumnSource<Long> symbolSource;
        private final IntegerSparseArraySource symbolLookup;

        SymbolTableToUniqueIdSource(ColumnSource<Long> symbolSource, IntegerSparseArraySource symbolLookup) {
            super(int.class);
            this.symbolSource = symbolSource;
            this.symbolLookup = symbolLookup;
        }

        @Override
        public int getInt(long rowKey) {
            final long symbolId = symbolSource.getLong(rowKey);
            return symbolLookup.getInt(symbolId);
        }

        private class LongToIntFillContext implements ColumnSource.FillContext {
            final WritableLongChunk<Values> longChunk;
            final FillContext innerFillContext;

            LongToIntFillContext(final int chunkCapacity, final SharedContext sharedState) {
                longChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
                innerFillContext = symbolSource.makeFillContext(chunkCapacity, sharedState);
            }

            @Override
            public void close() {
                longChunk.close();
                innerFillContext.close();
            }
        }

        @Override
        public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            return new LongToIntFillContext(chunkCapacity, sharedContext);
        }

        @Override
        public void fillChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
            final WritableIntChunk<? super Values> destAsInt = destination.asWritableIntChunk();
            final LongToIntFillContext longToIntContext = (LongToIntFillContext) context;
            final WritableLongChunk<Values> longChunk = longToIntContext.longChunk;
            symbolSource.fillChunk(longToIntContext.innerFillContext, longChunk, rowSequence);
            for (int ii = 0; ii < longChunk.size(); ++ii) {
                destAsInt.set(ii, symbolLookup.getInt(longChunk.get(ii)));
            }
            destination.setSize(longChunk.size());
        }

        @Override
        public boolean isStateless() {
            return symbolSource.isStateless();
        }
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
            final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);
            rowRedirection.removeAll(upstream.removed());

            try (final RowSet prevRowSet = leftTable.getRowSet().copyPrev()) {
                rowRedirection.applyShift(prevRowSet, upstream.shifted());
            }

            downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            leftTransformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);

            if (upstream.modifiedColumnSet().containsAny(leftKeyColumns)) {
                newLeftRedirections.ensureCapacity(downstream.modified().size());
                // compute our new values
                jsm.decorateLeftSide(downstream.modified(), leftSources, newLeftRedirections);
                final MutableBoolean updatedRightRow = new MutableBoolean(false);
                final MutableInt position = new MutableInt(0);
                downstream.modified().forAllRowKeys((long modifiedKey) -> {
                    final long newRedirection = newLeftRedirections.getLong(position.intValue());
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
                final long newRedirection = newLeftRedirections.getLong(position.intValue());
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

            final int maxSize =
                    UpdateSizeCalculator.chunkSize(upstream, JoinControl.CHUNK_SIZE);
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
                    final RowSet previousToShift;

                    if (rightKeysChanged) {
                        previousToShift =
                                getParent().getRowSet().copyPrev().minus(modifiedPreShift)
                                        .minus(upstream.removed());
                    } else {
                        previousToShift = getParent().getRowSet().copyPrev().minus(upstream.removed());
                    }

                    final RowSetShiftData.Iterator sit = upstream.shifted().applyIterator();
                    while (sit.hasNext()) {
                        sit.next();
                        final RowSet shiftedRowSet =
                                previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange())
                                        .shift(sit.shiftDelta());
                        jsm.applyRightShift(pc, rightSources, shiftedRowSet, sit.shiftDelta(), modifiedSlotTracker);
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
                        final WritableRowSet previousToShift = rightRecorder.getParent().getRowSet().copyPrev();
                        previousToShift.remove(rightRemoved);

                        if (rightKeysModified) {
                            previousToShift.remove(modifiedPreShift);
                        }

                        final RowSetShiftData.Iterator sit = rightShifted.applyIterator();
                        while (sit.hasNext()) {
                            sit.next();
                            try (final WritableRowSet shiftedRowSet =
                                    previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange())) {
                                shiftedRowSet.shiftInPlace(sit.shiftDelta());
                                jsm.applyRightShift(pc, rightSources, shiftedRowSet, sit.shiftDelta(),
                                        modifiedSlotTracker);
                            }
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
                final long rightKey = leftRedirections.getLong(position.intValue());
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
