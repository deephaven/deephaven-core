package io.deephaven.db.v2;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.join.JoinListenerRecorder;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.*;
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
        final Table result =
                naturalJoinInternal(leftTable, rightTable, columnsToMatch, columnsToAdd, exactMatch, control);
        leftTable.maybeCopyColumnDescriptions(result, rightTable, columnsToMatch, columnsToAdd);
        leftTable.copyAttributes(result, BaseTable.CopyAttributeOperation.Join);
        return result;
    }

    private static Table naturalJoinInternal(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, boolean exactMatch, JoinControl control) {
        try (final BucketingContext bucketingContext =
                new BucketingContext("naturalJoin", leftTable, rightTable, columnsToMatch, columnsToAdd, control)) {

            // if we have a single column of unique values, and the range is small, we can use a simplified table
            // TODO: SimpleUniqueStaticNaturalJoinManager, but not static!
            if (!rightTable.isLive() && control.useUniqueTable(bucketingContext.uniqueValues,
                    bucketingContext.maximumUniqueValue, bucketingContext.minimumUniqueValue)) {
                Assert.neqNull(bucketingContext.uniqueFunctor, "uniqueFunctor");
                final SimpleUniqueStaticNaturalJoinStateManager jsm = new SimpleUniqueStaticNaturalJoinStateManager(
                        bucketingContext.originalLeftSources, bucketingContext.uniqueValuesRange(),
                        bucketingContext.uniqueFunctor);
                jsm.setRightSide(rightTable.getIndex(), bucketingContext.rightSources[0]);
                final LongArraySource leftRedirections = new LongArraySource();
                leftRedirections.ensureCapacity(leftTable.getIndex().size());
                jsm.decorateLeftSide(leftTable.getIndex(), bucketingContext.leftSources, leftRedirections);

                final RedirectionIndex redirectionIndex = jsm.buildRedirectionIndex(leftTable, exactMatch,
                        leftRedirections, control.getRedirectionType(leftTable));

                final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, redirectionIndex, true);

                leftTable.listenForUpdates(new LeftTickingListener(bucketingContext.listenerDescription, columnsToMatch,
                        columnsToAdd, leftTable, result, redirectionIndex, jsm, bucketingContext.leftSources));

                return result;
            }

            if (bucketingContext.leftSources.length == 0) {
                return zeroKeyColumnsJoin(leftTable, rightTable, columnsToAdd, exactMatch,
                        bucketingContext.listenerDescription);
            }

            final LongArraySource leftHashSlots = new LongArraySource();

            final RedirectionIndex redirectionIndex;

            if (rightTable.isLive()) {
                if (leftTable.isLive()) {
                    if (bucketingContext.useLeftGrouping) {
                        throw new UnsupportedOperationException(
                                "Grouping is not supported with ticking chunked naturalJoin!");
                    }

                    final int tableSize = Math.max(control.tableSizeForLeftBuild(leftTable),
                            control.tableSizeForRightBuild(rightTable));

                    final IncrementalChunkedNaturalJoinStateManager jsm = new IncrementalChunkedNaturalJoinStateManager(
                            bucketingContext.leftSources, tableSize, bucketingContext.originalLeftSources);
                    jsm.buildFromRightSide(rightTable, bucketingContext.rightSources);
                    jsm.decorateLeftSide(leftTable.getIndex(), bucketingContext.leftSources, leftHashSlots);

                    jsm.compactAll();

                    redirectionIndex = jsm.buildRedirectionIndexFromRedirections(leftTable, exactMatch, leftHashSlots,
                            control.getRedirectionType(leftTable));

                    final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, redirectionIndex, true);

                    final JoinListenerRecorder leftRecorder =
                            new JoinListenerRecorder(true, bucketingContext.listenerDescription, leftTable, result);
                    final JoinListenerRecorder rightRecorder =
                            new JoinListenerRecorder(false, bucketingContext.listenerDescription, rightTable, result);

                    jsm.setMaximumLoadFactor(control.getMaximumLoadFactor());
                    jsm.setTargetLoadFactor(control.getTargetLoadFactor());

                    final ChunkedMergedJoinListener mergedJoinListener = new ChunkedMergedJoinListener(
                            leftTable, rightTable, bucketingContext.leftSources, bucketingContext.rightSources,
                            columnsToMatch,
                            columnsToAdd, leftRecorder, rightRecorder, result, redirectionIndex, jsm, exactMatch,
                            bucketingContext.listenerDescription);
                    leftRecorder.setMergedListener(mergedJoinListener);
                    rightRecorder.setMergedListener(mergedJoinListener);

                    leftTable.listenForUpdates(leftRecorder);
                    rightTable.listenForUpdates(rightRecorder);

                    result.addParentReference(mergedJoinListener);

                    return result;
                } else {
                    // right is live, left is static
                    final RightIncrementalChunkedNaturalJoinStateManager jsm =
                            new RightIncrementalChunkedNaturalJoinStateManager(
                                    bucketingContext.leftSources, control.tableSizeForLeftBuild(leftTable),
                                    bucketingContext.originalLeftSources);

                    final ObjectArraySource<Index> indexSource;
                    final MutableInt groupingSize = new MutableInt();
                    if (bucketingContext.useLeftGrouping) {
                        final Map<?, Index> grouping =
                                bucketingContext.leftSources[0].getGroupToRange(leftTable.getIndex());

                        // noinspection unchecked,rawtypes
                        final Pair<ArrayBackedColumnSource<?>, ObjectArraySource<Index>> flatResultColumnSources =
                                AbstractColumnSource.groupingToFlatSources(
                                        (ColumnSource) bucketingContext.leftSources[0], grouping, leftTable.getIndex(),
                                        groupingSize);
                        final ArrayBackedColumnSource<?> groupSource = flatResultColumnSources.getFirst();
                        indexSource = flatResultColumnSources.getSecond();

                        final Table leftTableGrouped =
                                new QueryTable(Index.FACTORY.getFlatIndex(groupingSize.intValue()),
                                        Collections.singletonMap(columnsToMatch[0].left(), groupSource));

                        final ColumnSource<?>[] groupedSourceArray = {groupSource};
                        jsm.buildFromLeftSide(leftTableGrouped, groupedSourceArray, leftHashSlots);
                        jsm.convertLeftGroups(groupingSize.intValue(), leftHashSlots, indexSource);
                    } else {
                        jsm.buildFromLeftSide(leftTable, bucketingContext.leftSources, leftHashSlots);
                        indexSource = null;
                    }

                    jsm.addRightSide(rightTable.getIndex(), bucketingContext.rightSources);

                    if (bucketingContext.useLeftGrouping) {
                        redirectionIndex = jsm.buildRedirectionIndexFromHashSlotGrouped(leftTable, indexSource,
                                groupingSize.intValue(), exactMatch, leftHashSlots,
                                control.getRedirectionType(leftTable));
                    } else {
                        redirectionIndex = jsm.buildRedirectionIndexFromHashSlot(leftTable, exactMatch, leftHashSlots,
                                control.getRedirectionType(leftTable));
                    }

                    final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, redirectionIndex, true);

                    rightTable
                            .listenForUpdates(new RightTickingListener(bucketingContext.listenerDescription, rightTable,
                                    columnsToMatch, columnsToAdd, result, redirectionIndex, jsm,
                                    bucketingContext.rightSources, exactMatch));
                    return result;
                }
            } else {
                if (bucketingContext.useLeftGrouping) {
                    if (leftTable.isRefreshing()) {
                        throw new UnsupportedOperationException(
                                "Grouping information is not supported when tables are refreshing!");
                    }

                    final Map<?, Index> grouping =
                            bucketingContext.leftSources[0].getGroupToRange(leftTable.getIndex());

                    final MutableInt groupingSize = new MutableInt();
                    // noinspection unchecked,rawtypes
                    final Pair<ArrayBackedColumnSource<?>, ObjectArraySource<Index>> flatResultColumnSources =
                            AbstractColumnSource.groupingToFlatSources((ColumnSource) bucketingContext.leftSources[0],
                                    grouping, leftTable.getIndex(), groupingSize);
                    final ArrayBackedColumnSource<?> groupSource = flatResultColumnSources.getFirst();
                    final ObjectArraySource<Index> indexSource = flatResultColumnSources.getSecond();

                    final Table leftTableGrouped = new QueryTable(Index.FACTORY.getFlatIndex(groupingSize.intValue()),
                            Collections.singletonMap(columnsToMatch[0].left(), groupSource));

                    final ColumnSource<?>[] groupedSourceArray = {groupSource};
                    final StaticChunkedNaturalJoinStateManager jsm =
                            new StaticChunkedNaturalJoinStateManager(groupedSourceArray,
                                    StaticChunkedNaturalJoinStateManager.hashTableSize(groupingSize.intValue()),
                                    groupedSourceArray);
                    jsm.buildFromLeftSide(leftTableGrouped, groupedSourceArray, leftHashSlots);
                    jsm.decorateWithRightSide(rightTable, bucketingContext.rightSources);
                    redirectionIndex = jsm.buildGroupedRedirectionIndex(leftTable, exactMatch, leftTableGrouped.size(),
                            leftHashSlots, indexSource, control.getRedirectionType(leftTable));
                } else if (control.buildLeft(leftTable, rightTable)) {
                    final StaticChunkedNaturalJoinStateManager jsm =
                            new StaticChunkedNaturalJoinStateManager(bucketingContext.leftSources,
                                    control.tableSizeForLeftBuild(leftTable), bucketingContext.originalLeftSources);
                    jsm.buildFromLeftSide(leftTable, bucketingContext.leftSources, leftHashSlots);
                    jsm.decorateWithRightSide(rightTable, bucketingContext.rightSources);
                    redirectionIndex = jsm.buildRedirectionIndexFromHashSlot(leftTable, exactMatch, leftHashSlots,
                            control.getRedirectionType(leftTable));
                } else {
                    final StaticChunkedNaturalJoinStateManager jsm =
                            new StaticChunkedNaturalJoinStateManager(bucketingContext.leftSources,
                                    control.tableSizeForRightBuild(rightTable), bucketingContext.originalLeftSources);
                    jsm.buildFromRightSide(rightTable, bucketingContext.rightSources);
                    jsm.decorateLeftSide(leftTable, bucketingContext.leftSources, leftHashSlots);
                    redirectionIndex = jsm.buildRedirectionIndexFromRedirections(leftTable, exactMatch, leftHashSlots,
                            control.getRedirectionType(leftTable));

                    final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, redirectionIndex, true);

                    leftTable.listenForUpdates(
                            new LeftTickingListener(bucketingContext.listenerDescription, columnsToMatch, columnsToAdd,
                                    leftTable, result, redirectionIndex, jsm, bucketingContext.leftSources));
                    return result;
                }
            }

            return makeResult(leftTable, rightTable, columnsToAdd, redirectionIndex, false);
        }
    }

    @NotNull
    private static Table zeroKeyColumnsJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToAdd,
            boolean exactMatch, String listenerDescription) {
        // we are a single value join, we do not need to do any work
        final SingleValueRedirectionIndex redirectionIndex;

        final boolean rightRefreshing = rightTable.isRefreshing();

        if (rightTable.size() > 1) {
            if (leftTable.size() > 0) {
                throw new RuntimeException(
                        "naturalJoin with zero key columns may not have more than one row in the right hand side table!");
            }
            // we don't care where it goes
            redirectionIndex = getSingleValueRedirectionIndex(rightRefreshing, Index.NULL_KEY);
        } else if (rightTable.size() == 1) {
            redirectionIndex = getSingleValueRedirectionIndex(rightRefreshing, rightTable.getIndex().firstKey());
        } else {
            if (exactMatch && leftTable.size() > 0) {
                throw new RuntimeException(
                        "exactJoin with zero key columns must have exactly one row in the right hand side table!");
            }
            redirectionIndex = getSingleValueRedirectionIndex(rightRefreshing, Index.NULL_KEY);
        }

        final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, redirectionIndex, rightRefreshing);
        final ModifiedColumnSet.Transformer leftTransformer =
                leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());
        final ModifiedColumnSet.Transformer rightTransformer =
                rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);
        final ModifiedColumnSet allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));

        if (leftTable.isLive()) {
            if (rightTable.isLive()) {
                final JoinListenerRecorder leftRecorder =
                        new JoinListenerRecorder(true, listenerDescription, leftTable, result);
                final JoinListenerRecorder rightRecorder =
                        new JoinListenerRecorder(false, listenerDescription, rightTable, result);

                final MergedListener mergedListener = new MergedListener(Arrays.asList(leftRecorder, rightRecorder),
                        Collections.emptyList(), listenerDescription, result) {
                    @Override
                    protected void process() {
                        result.modifiedColumnSet.clear();

                        final boolean rightChanged = rightRecorder.recordedVariablesAreValid();
                        final boolean leftChanged = leftRecorder.recordedVariablesAreValid();

                        checkRightTableSizeZeroKeys(leftTable, rightTable, exactMatch);

                        if (rightChanged) {
                            final boolean rightUpdated = updateRightRedirection(rightTable, redirectionIndex);
                            if (rightUpdated) {
                                result.modifiedColumnSet.setAll(allRightColumns);
                            } else {
                                rightTransformer.transform(rightRecorder.getModifiedColumnSet(),
                                        result.modifiedColumnSet);
                            }
                        }

                        if (leftChanged) {
                            final Index modified;
                            if (rightChanged) {
                                modified = result.getIndex().minus(leftRecorder.getAdded());
                            } else {
                                modified = leftRecorder.getModified().clone();
                            }
                            leftTransformer.transform(leftRecorder.getModifiedColumnSet(), result.modifiedColumnSet);
                            result.notifyListeners(new ShiftAwareListener.Update(
                                    leftRecorder.getAdded().clone(), leftRecorder.getRemoved().clone(), modified,
                                    leftRecorder.getShifted(), result.modifiedColumnSet));
                        } else if (rightChanged) {
                            result.notifyListeners(new ShiftAwareListener.Update(
                                    Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex(),
                                    result.getIndex().clone(), IndexShiftData.EMPTY, result.modifiedColumnSet));
                        }
                    }

                };

                leftRecorder.setMergedListener(mergedListener);
                rightRecorder.setMergedListener(mergedListener);
                leftTable.listenForUpdates(leftRecorder);
                rightTable.listenForUpdates(rightRecorder);
                result.addParentReference(mergedListener);

            } else {
                leftTable
                        .listenForUpdates(new BaseTable.ShiftAwareListenerImpl(listenerDescription, leftTable, result) {
                            @Override
                            public void onUpdate(final Update upstream) {
                                checkRightTableSizeZeroKeys(leftTable, rightTable, exactMatch);
                                leftTransformer.clearAndTransform(upstream.modifiedColumnSet, result.modifiedColumnSet);
                                final Update downstream = upstream.copy();
                                downstream.modifiedColumnSet = result.modifiedColumnSet;
                                result.notifyListeners(downstream);
                            }
                        });
            }
        } else if (rightTable.isLive()) {
            if (leftTable.size() > 0) {
                rightTable.listenForUpdates(
                        new BaseTable.ShiftAwareListenerImpl(listenerDescription, rightTable, result) {
                            @Override
                            public void onUpdate(final Update upstream) {
                                checkRightTableSizeZeroKeys(leftTable, rightTable, exactMatch);
                                final boolean changed = updateRightRedirection(rightTable, redirectionIndex);
                                if (!changed) {
                                    rightTransformer.clearAndTransform(upstream.modifiedColumnSet,
                                            result.modifiedColumnSet);
                                }
                                result.notifyListeners(
                                        new Update(Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex(),
                                                result.getIndex().clone(), IndexShiftData.EMPTY,
                                                changed ? allRightColumns : result.modifiedColumnSet));
                            }
                        });
            }
        }
        return result;
    }

    @NotNull
    private static SingleValueRedirectionIndex getSingleValueRedirectionIndex(boolean refreshing, long value) {
        return refreshing ? new TickingSingleValueRedirectionIndexImpl(value)
                : new StaticSingleValueRedirectionIndexImpl(value);
    }

    private static boolean updateRightRedirection(QueryTable rightTable, SingleValueRedirectionIndex redirectionIndex) {
        final boolean changed;
        if (rightTable.size() == 0) {
            changed = redirectionIndex.getValue() != Index.NULL_KEY;
            if (changed) {
                redirectionIndex.setValue(Index.NULL_KEY);
            }
        } else {
            final long value = rightTable.getIndex().firstKey();
            changed = redirectionIndex.getValue() != value;
            if (changed) {
                redirectionIndex.setValue(value);
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
            @NotNull final RedirectionIndex redirectionIndex,
            final boolean rightRefreshingColumns) {
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>(leftTable.getColumnSourceMap());
        for (MatchPair mp : columnsToAdd) {
            final ReadOnlyRedirectedColumnSource<?> redirectedColumnSource =
                    new ReadOnlyRedirectedColumnSource<>(redirectionIndex, rightTable.getColumnSource(mp.right()));
            if (rightRefreshingColumns) {
                redirectedColumnSource.startTrackingPrevValues();
            }
            columnSourceMap.put(mp.left(), redirectedColumnSource);
        }
        if (rightRefreshingColumns) {
            redirectionIndex.startTrackingPrevValues();
        }
        return new QueryTable(leftTable.getIndex(), columnSourceMap);
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
        public int getInt(long index) {
            final long symbolId = symbolSource.getLong(index);
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
                @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
            final WritableIntChunk<? super Values> destAsInt = destination.asWritableIntChunk();
            final LongToIntFillContext longToIntContext = (LongToIntFillContext) context;
            final WritableLongChunk<Values> longChunk = longToIntContext.longChunk;
            symbolSource.fillChunk(longToIntContext.innerFillContext, longChunk, orderedKeys);
            for (int ii = 0; ii < longChunk.size(); ++ii) {
                destAsInt.set(ii, symbolLookup.getInt(longChunk.get(ii)));
            }
            destination.setSize(longChunk.size());
        }
    }

    private static class LeftTickingListener extends BaseTable.ShiftAwareListenerImpl {
        final LongArraySource newLeftRedirections;
        private final QueryTable result;
        private final QueryTable leftTable;
        private final RedirectionIndex redirectionIndex;
        private final StaticNaturalJoinStateManager jsm;
        private final ColumnSource<?>[] leftSources;
        private final ModifiedColumnSet leftKeyColumns;
        private final ModifiedColumnSet rightModifiedColumns;
        private final ModifiedColumnSet.Transformer leftTransformer;

        LeftTickingListener(String description, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
                QueryTable leftTable, QueryTable result, RedirectionIndex redirectionIndex,
                StaticNaturalJoinStateManager jsm, ColumnSource<?>[] leftSources) {
            super(description, leftTable, result);
            this.result = result;
            this.leftTable = leftTable;
            this.redirectionIndex = redirectionIndex;
            this.jsm = jsm;
            this.leftSources = leftSources;
            newLeftRedirections = new LongArraySource();
            leftKeyColumns = leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));
            rightModifiedColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));

            leftTransformer =
                    leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());
        }

        @Override
        public void onUpdate(final Update upstream) {
            final Update downstream = upstream.copy();
            upstream.removed.forAllLongs(redirectionIndex::removeVoid);

            try (final Index prevIndex = leftTable.getIndex().getPrevIndex()) {
                redirectionIndex.applyShift(prevIndex, upstream.shifted);
            }

            downstream.modifiedColumnSet = result.modifiedColumnSet;
            leftTransformer.clearAndTransform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);

            if (upstream.modifiedColumnSet.containsAny(leftKeyColumns)) {
                newLeftRedirections.ensureCapacity(downstream.modified.size());
                // compute our new values
                jsm.decorateLeftSide(downstream.modified, leftSources, newLeftRedirections);
                final MutableBoolean updatedRightRow = new MutableBoolean(false);
                final MutableInt position = new MutableInt(0);
                downstream.modified.forAllLongs((long modifiedKey) -> {
                    final long newRedirection = newLeftRedirections.getLong(position.intValue());
                    final long old;
                    if (newRedirection == Index.NULL_KEY) {
                        old = redirectionIndex.remove(modifiedKey);
                    } else {
                        old = redirectionIndex.put(modifiedKey, newRedirection);
                    }
                    if (newRedirection != old) {
                        updatedRightRow.setValue(true);
                    }
                    position.increment();
                });

                if (updatedRightRow.booleanValue()) {
                    downstream.modifiedColumnSet.setAll(rightModifiedColumns);
                }
            }

            newLeftRedirections.ensureCapacity(downstream.added.size());
            jsm.decorateLeftSide(downstream.added, leftSources, newLeftRedirections);
            final MutableInt position = new MutableInt(0);
            downstream.added.forAllLongs((long ll) -> {
                final long newRedirection = newLeftRedirections.getLong(position.intValue());
                if (newRedirection != Index.NULL_KEY) {
                    redirectionIndex.putVoid(ll, newRedirection);
                }
                position.increment();
            });

            result.notifyListeners(downstream);
        }
    }

    private static class RightTickingListener extends BaseTable.ShiftAwareListenerImpl {
        private final QueryTable result;
        private final RedirectionIndex redirectionIndex;
        private final RightIncrementalChunkedNaturalJoinStateManager jsm;
        private final ColumnSource<?>[] rightSources;
        private final boolean exactMatch;
        private final ModifiedColumnSet allRightColumns;
        private final ModifiedColumnSet rightKeyColumns;
        private final ModifiedColumnSet.Transformer rightTransformer;
        private final NaturalJoinModifiedSlotTracker modifiedSlotTracker = new NaturalJoinModifiedSlotTracker();

        RightTickingListener(String description, QueryTable rightTable, MatchPair[] columnsToMatch,
                MatchPair[] columnsToAdd, QueryTable result, RedirectionIndex redirectionIndex,
                RightIncrementalChunkedNaturalJoinStateManager jsm, ColumnSource<?>[] rightSources,
                boolean exactMatch) {
            super(description, rightTable, result);
            this.result = result;
            this.redirectionIndex = redirectionIndex;
            this.jsm = jsm;
            this.rightSources = rightSources;
            this.exactMatch = exactMatch;

            rightKeyColumns = rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
            allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
            rightTransformer = rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);
        }

        @Override
        public void onUpdate(final Update upstream) {

            modifiedSlotTracker.clear();

            final boolean addedRightColumnsChanged;

            final int maxSize =
                    UpdateSizeCalculator.chunkSize(upstream, IncrementalChunkedNaturalJoinStateManager.CHUNK_SIZE);
            if (maxSize == 0) {
                Assert.assertion(upstream.empty(), "upstream.empty()");
                return;
            }

            try (final RightIncrementalChunkedNaturalJoinStateManager.ProbeContext pc =
                    jsm.makeProbeContext(rightSources, maxSize)) {
                final Index modifiedPreShift;

                final boolean rightKeysChanged = upstream.modifiedColumnSet.containsAny(rightKeyColumns);

                if (rightKeysChanged) {
                    modifiedPreShift = upstream.getModifiedPreShift();
                } else {
                    modifiedPreShift = null;
                }

                if (upstream.shifted.nonempty()) {
                    final Index previousToShift;

                    if (rightKeysChanged) {
                        previousToShift =
                                getParent().getIndex().getPrevIndex().minus(modifiedPreShift).minus(upstream.removed);
                    } else {
                        previousToShift = getParent().getIndex().getPrevIndex().minus(upstream.removed);
                    }

                    final IndexShiftData.Iterator sit = upstream.shifted.applyIterator();
                    while (sit.hasNext()) {
                        sit.next();
                        final Index shiftedIndex =
                                previousToShift.subindexByKey(sit.beginRange(), sit.endRange()).shift(sit.shiftDelta());
                        jsm.applyRightShift(pc, rightSources, shiftedIndex, sit.shiftDelta(), modifiedSlotTracker);
                    }
                }

                jsm.removeRight(pc, upstream.removed, rightSources, modifiedSlotTracker);

                rightTransformer.clearAndTransform(upstream.modifiedColumnSet, result.modifiedColumnSet);
                addedRightColumnsChanged = result.modifiedColumnSet.size() != 0;

                if (rightKeysChanged) {
                    // It should make us somewhat sad that we have to add/remove, because we are doing two hash lookups
                    // for keys that have not actually changed.
                    // The alternative would be to do an initial pass that would filter out key columns that have not
                    // actually changed.
                    jsm.removeRight(pc, modifiedPreShift, rightSources, modifiedSlotTracker);
                    jsm.addRightSide(pc, upstream.modified, rightSources, modifiedSlotTracker);
                } else {
                    if (upstream.modified.nonempty() && addedRightColumnsChanged) {
                        jsm.modifyByRight(pc, upstream.modified, rightSources, modifiedSlotTracker);
                    }
                }

                jsm.addRightSide(pc, upstream.added, rightSources, modifiedSlotTracker);
            }

            final Index.RandomBuilder modifiedLeftBuilder = Index.FACTORY.getRandomBuilder();
            final ModifiedSlotUpdater slotUpdater = new ModifiedSlotUpdater(jsm, modifiedLeftBuilder, redirectionIndex,
                    exactMatch, addedRightColumnsChanged);
            modifiedSlotTracker.forAllModifiedSlots(slotUpdater);
            if (slotUpdater.changedRedirection) {
                result.modifiedColumnSet.setAll(allRightColumns);
            }

            // left is static, so the only thing that can happen is modifications
            final Index modifiedLeft = modifiedLeftBuilder.getIndex();

            result.notifyListeners(new Update(Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex(),
                    modifiedLeft, IndexShiftData.EMPTY,
                    modifiedLeft.nonempty() ? result.modifiedColumnSet : ModifiedColumnSet.EMPTY));
        }
    }

    private static class ModifiedSlotUpdater implements NaturalJoinModifiedSlotTracker.ModifiedSlotConsumer {

        private final IncrementalNaturalJoinStateManager jsm;
        private final Index.RandomBuilder modifiedLeftBuilder;
        private final RedirectionIndex redirectionIndex;
        private final boolean exactMatch;
        private final boolean rightAddedColumnsChanged;
        boolean changedRedirection = false;

        private ModifiedSlotUpdater(IncrementalNaturalJoinStateManager jsm, Index.RandomBuilder modifiedLeftBuilder,
                RedirectionIndex redirectionIndex, boolean exactMatch, boolean rightAddedColumnsChanged) {
            this.jsm = jsm;
            this.modifiedLeftBuilder = modifiedLeftBuilder;
            this.redirectionIndex = redirectionIndex;
            this.exactMatch = exactMatch;
            this.rightAddedColumnsChanged = rightAddedColumnsChanged;
        }

        @Override
        public void accept(long updatedSlot, long originalRightValue, byte flag) {
            final Index leftIndices = jsm.getLeftIndex(updatedSlot);
            if (leftIndices == null || leftIndices.empty()) {
                return;
            }

            final long rightIndex = jsm.getRightIndex(updatedSlot);

            if (rightIndex == StaticNaturalJoinStateManager.DUPLICATE_RIGHT_VALUE) {
                throw new IllegalStateException("Duplicate right key for " + jsm.keyString(updatedSlot));
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
                modifiedLeftBuilder.addIndex(leftIndices);
            }


            // but we might not need to update the redirection index
            if (unchangedRedirection && (flag & NaturalJoinModifiedSlotTracker.FLAG_RIGHT_ADD) == 0) {
                return;
            }

            changedRedirection = true;

            if (rightIndex == Index.NULL_KEY) {
                jsm.checkExactMatch(exactMatch, leftIndices.firstKey(), rightIndex);
                leftIndices.forAllLongs(redirectionIndex::removeVoid);
            } else {
                leftIndices.forAllLongs((long key) -> redirectionIndex.putVoid(key, rightIndex));
            }
        }
    }

    private static class ChunkedMergedJoinListener extends MergedListener {
        private final ColumnSource<?>[] leftSources;
        private final ColumnSource<?>[] rightSources;
        private final JoinListenerRecorder leftRecorder;
        private final JoinListenerRecorder rightRecorder;
        private final RedirectionIndex redirectionIndex;
        private final IncrementalChunkedNaturalJoinStateManager jsm;
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
                RedirectionIndex redirectionIndex,
                IncrementalChunkedNaturalJoinStateManager jsm,
                boolean exactMatch,
                String listenerDescription) {
            super(Arrays.asList(leftRecorder, rightRecorder), Collections.emptyList(), listenerDescription, result);
            this.leftSources = leftSources;
            this.rightSources = rightSources;
            this.leftRecorder = leftRecorder;
            this.rightRecorder = rightRecorder;
            this.redirectionIndex = redirectionIndex;
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
            final Index.RandomBuilder modifiedLeftBuilder = Index.FACTORY.getRandomBuilder();
            result.modifiedColumnSet.clear();
            modifiedSlotTracker.clear();

            final boolean addedRightColumnsChanged;

            if (rightRecorder.recordedVariablesAreValid()) {
                final Index rightAdded = rightRecorder.getAdded();
                final Index rightRemoved = rightRecorder.getRemoved();
                final Index rightModified = rightRecorder.getModified();
                final ModifiedColumnSet rightModifiedColumns = rightRecorder.getModifiedColumnSet();
                final boolean rightKeysModified =
                        rightModified.nonempty() && rightModifiedColumns.containsAny(rightKeyColumns);

                final long probeSize =
                        UpdateSizeCalculator.chunkSize(Math.max(rightRemoved.size(), rightModified.size()),
                                rightRecorder.getShifted(), IncrementalChunkedNaturalJoinStateManager.CHUNK_SIZE);
                final long buildSize = Math.max(rightAdded.size(), rightKeysModified ? rightModified.size() : 0);

                // process right updates
                try (final IncrementalChunkedNaturalJoinStateManager.ProbeContext pc =
                        probeSize == 0 ? null : jsm.makeProbeContext(rightSources, probeSize);
                        final IncrementalChunkedNaturalJoinStateManager.BuildContext bc =
                                buildSize == 0 ? null : jsm.makeBuildContext(rightSources, buildSize)) {
                    final Index modifiedPreShift;

                    final IndexShiftData rightShifted = rightRecorder.getShifted();

                    if (rightKeysModified) {
                        modifiedPreShift = rightRecorder.getModifiedPreShift();
                    } else {
                        modifiedPreShift = null;
                    }

                    if (rightRemoved.nonempty()) {
                        jsm.removeRight(pc, rightRemoved, rightSources, modifiedSlotTracker);
                    }

                    rightTransformer.transform(rightModifiedColumns, result.modifiedColumnSet);
                    addedRightColumnsChanged = result.modifiedColumnSet.size() > 0;

                    if (rightKeysModified) {
                        // It should make us somewhat sad that we have to add/remove, because we are doing two hash
                        // lookups for keys that have not actually changed.
                        // The alternative would be to do an initial pass that would filter out key columns that have
                        // not actually changed.
                        jsm.removeRight(pc, modifiedPreShift, rightSources, modifiedSlotTracker);
                    }

                    if (rightShifted.nonempty()) {
                        final Index previousToShift =
                                rightRecorder.getParent().getIndex().getPrevIndex().minus(rightRemoved);

                        if (rightKeysModified) {
                            previousToShift.remove(modifiedPreShift);
                        }

                        final IndexShiftData.Iterator sit = rightShifted.applyIterator();
                        while (sit.hasNext()) {
                            sit.next();
                            final Index shiftedIndex = previousToShift.subindexByKey(sit.beginRange(), sit.endRange())
                                    .shift(sit.shiftDelta());
                            jsm.applyRightShift(pc, rightSources, shiftedIndex, sit.shiftDelta(), modifiedSlotTracker);
                        }
                    }

                    if (rightKeysModified) {
                        jsm.addRightSide(bc, rightModified, rightSources, modifiedSlotTracker);
                    } else if (rightModified.nonempty() && addedRightColumnsChanged) {
                        jsm.modifyByRight(pc, rightModified, rightSources, modifiedSlotTracker);
                    }

                    if (rightAdded.nonempty()) {
                        jsm.addRightSide(bc, rightAdded, rightSources, modifiedSlotTracker);
                    }
                }
            } else {
                addedRightColumnsChanged = false;
            }

            final Index leftAdded = leftRecorder.getAdded();
            final Index leftRemoved = leftRecorder.getRemoved();
            final IndexShiftData leftShifted = leftRecorder.getShifted();

            if (leftRecorder.recordedVariablesAreValid()) {
                final Index leftModified = leftRecorder.getModified();
                final ModifiedColumnSet leftModifiedColumns = leftRecorder.getModifiedColumnSet();
                final boolean leftAdditions = leftAdded.nonempty();
                final boolean leftKeyModifications =
                        leftModified.nonempty() && leftModifiedColumns.containsAny(leftKeyColumns);
                final boolean newLeftRedirections = leftAdditions || leftKeyModifications;
                final long buildSize = Math.max(leftAdded.size(), leftKeyModifications ? leftModified.size() : 0);
                final long probeSize = UpdateSizeCalculator.chunkSize(
                        Math.max(leftRemoved.size(), leftKeyModifications ? leftModified.size() : 0), leftShifted,
                        IncrementalChunkedNaturalJoinStateManager.CHUNK_SIZE);

                final LongArraySource leftRedirections = newLeftRedirections ? new LongArraySource() : null;
                if (leftRedirections != null) {
                    leftRedirections.ensureCapacity(buildSize);
                }

                try (final IncrementalChunkedNaturalJoinStateManager.ProbeContext pc =
                        probeSize == 0 ? null : jsm.makeProbeContext(leftSources, probeSize);
                        final IncrementalChunkedNaturalJoinStateManager.BuildContext bc =
                                buildSize == 0 ? null : jsm.makeBuildContext(leftSources, buildSize)) {
                    leftRemoved.forAllLongs(redirectionIndex::removeVoid);
                    jsm.removeLeft(pc, leftRemoved, leftSources);

                    final Index leftModifiedPreShift;
                    if (leftKeyModifications) {
                        if (leftShifted.nonempty()) {
                            leftModifiedPreShift = leftModified.clone();
                            leftShifted.unapply(leftModifiedPreShift);
                        } else {
                            leftModifiedPreShift = leftModified;
                        }

                        // remove pre-shift modified
                        jsm.removeLeft(pc, leftModifiedPreShift, leftSources);
                        leftModifiedPreShift.forAllLongs(redirectionIndex::removeVoid);
                    } else {
                        leftModifiedPreShift = null;
                    }

                    if (leftShifted.nonempty()) {
                        try (final Index prevIndex = leftRecorder.getParent().getIndex().getPrevIndex()) {
                            prevIndex.remove(leftRemoved);

                            if (leftKeyModifications) {
                                prevIndex.remove(leftModifiedPreShift);
                                leftModifiedPreShift.close();
                            }

                            final IndexShiftData.Iterator sit = leftShifted.applyIterator();
                            while (sit.hasNext()) {
                                sit.next();
                                try (final Index shiftedIndex = prevIndex
                                        .subindexByKey(sit.beginRange(), sit.endRange()).shift(sit.shiftDelta())) {
                                    jsm.applyLeftShift(pc, leftSources, shiftedIndex, sit.shiftDelta());
                                }
                            }

                            redirectionIndex.applyShift(prevIndex, leftShifted);
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
                        // modified slot tracker is unstable [and we don’t want two of them].
                        // On removal, we could ask our modified slot tracker if, (i) our cookie is valid, and if so
                        // (ii) what the original right value was what the right value was
                        // [presuming we add that for right side point 1]. This would let us report our original
                        // redirection index as part of the jsm.removeLeft. We could then compare
                        // the old redirections to the new redirections, only lighting up allRightColumns if there was
                        // indeed a change.
                        result.modifiedColumnSet.setAll(allRightColumns);
                    }

                    if (leftAdditions) {
                        jsm.addLeftSide(bc, leftAdded, leftSources, leftRedirections, modifiedSlotTracker);
                        copyRedirections(leftAdded, leftRedirections);
                    }
                }

                // process left updates
                leftTransformer.transform(leftModifiedColumns, result.modifiedColumnSet);

                modifiedLeftBuilder.addIndex(leftModified);
            }

            final ModifiedSlotUpdater slotUpdater = new ModifiedSlotUpdater(jsm, modifiedLeftBuilder, redirectionIndex,
                    exactMatch, addedRightColumnsChanged);
            modifiedSlotTracker.forAllModifiedSlots(slotUpdater);
            if (slotUpdater.changedRedirection) {
                result.modifiedColumnSet.setAll(allRightColumns);
            }

            final Index modifiedLeft = modifiedLeftBuilder.getIndex();
            modifiedLeft.retain(result.getIndex());
            modifiedLeft.remove(leftRecorder.getAdded());

            result.notifyListeners(new ShiftAwareListener.Update(leftAdded.clone(), leftRemoved.clone(), modifiedLeft,
                    leftShifted, result.modifiedColumnSet));
        }

        private void copyRedirections(final Index leftRows, @NotNull final LongArraySource leftRedirections) {
            final MutableInt position = new MutableInt(0);
            leftRows.forAllLongs((long ll) -> {
                final long rightKey = leftRedirections.getLong(position.intValue());
                jsm.checkExactMatch(exactMatch, ll, rightKey);
                if (rightKey == Index.NULL_KEY) {
                    redirectionIndex.removeVoid(ll);
                } else {
                    redirectionIndex.putVoid(ll, rightKey);
                }
                position.increment();
            });
        }
    }

}
