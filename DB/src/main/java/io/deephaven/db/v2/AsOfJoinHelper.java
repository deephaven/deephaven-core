package io.deephaven.db.v2;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.hashing.ChunkEquals;
import io.deephaven.db.v2.join.BucketedChunkedAjMergedListener;
import io.deephaven.db.v2.join.JoinListenerRecorder;
import io.deephaven.db.v2.join.ZeroKeyChunkedAjMergedListener;
import io.deephaven.db.v2.sort.LongSortKernel;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.sized.SizedBooleanChunk;
import io.deephaven.db.v2.sources.chunk.sized.SizedChunk;
import io.deephaven.db.v2.sources.chunk.sized.SizedLongChunk;
import io.deephaven.db.v2.ssa.ChunkSsaStamp;
import io.deephaven.db.v2.ssa.SegmentedSortedArray;
import io.deephaven.db.v2.ssa.SsaSsaStamp;
import io.deephaven.db.v2.utils.*;
import io.deephaven.db.v2.utils.compact.CompactKernel;
import io.deephaven.db.v2.utils.compact.LongCompactKernel;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.deephaven.db.v2.sources.chunk.Attributes.*;

public class AsOfJoinHelper {

    private AsOfJoinHelper() {} // static use only

    static Table asOfJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, SortingOrder order, boolean disallowExactMatch) {
        final JoinControl joinControl = new JoinControl();
        return asOfJoin(joinControl, leftTable, rightTable, columnsToMatch, columnsToAdd, order, disallowExactMatch);
    }

    static Table asOfJoin(JoinControl control, QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, SortingOrder order, boolean disallowExactMatch) {
        if (columnsToMatch.length == 0) {
            throw new IllegalArgumentException("aj() requires at least one column to match!");
        }

        checkColumnConflicts(leftTable, columnsToAdd);

        if (!leftTable.isLive() && leftTable.size() == 0) {
            return makeResult(leftTable, rightTable, new StaticSingleValueRedirectionIndexImpl(Index.NULL_KEY),
                    columnsToAdd, false);
        }

        final MatchPair stampPair = columnsToMatch[columnsToMatch.length - 1];

        final int keyColumnCount = columnsToMatch.length - 1;

        final ColumnSource<?>[] originalLeftSources = Arrays.stream(columnsToMatch).limit(keyColumnCount)
                .map(mp -> leftTable.getColumnSource(mp.leftColumn)).toArray(ColumnSource[]::new);
        final ColumnSource<?>[] leftSources = new ColumnSource[originalLeftSources.length];
        for (int ii = 0; ii < leftSources.length; ++ii) {
            leftSources[ii] = ReinterpretUtilities.maybeConvertToPrimitive(originalLeftSources[ii]);
        }
        final ColumnSource<?>[] originalRightSources = Arrays.stream(columnsToMatch).limit(keyColumnCount)
                .map(mp -> rightTable.getColumnSource(mp.rightColumn)).toArray(ColumnSource[]::new);
        final ColumnSource<?>[] rightSources = new ColumnSource[originalLeftSources.length];
        for (int ii = 0; ii < leftSources.length; ++ii) {
            rightSources[ii] = ReinterpretUtilities.maybeConvertToPrimitive(originalRightSources[ii]);
        }

        final ColumnSource<?> leftStampSource =
                ReinterpretUtilities.maybeConvertToPrimitive(leftTable.getColumnSource(stampPair.left()));
        final ColumnSource<?> originalRightStampSource = rightTable.getColumnSource(stampPair.right());
        final ColumnSource<?> rightStampSource = ReinterpretUtilities.maybeConvertToPrimitive(originalRightStampSource);

        if (leftStampSource.getType() != rightStampSource.getType()) {
            throw new IllegalArgumentException("Can not aj() with different stamp types: left="
                    + leftStampSource.getType() + ", right=" + rightStampSource.getType());
        }

        final RedirectionIndex redirectionIndex = JoinRedirectionIndex.makeRedirectionIndex(control, leftTable);
        if (keyColumnCount == 0) {
            return zeroKeyAj(control, leftTable, rightTable, columnsToAdd, stampPair, leftStampSource,
                    originalRightStampSource, rightStampSource, order, disallowExactMatch, redirectionIndex);
        }

        if (rightTable.isLive()) {
            if (leftTable.isLive()) {
                return bothIncrementalAj(control, leftTable, rightTable, columnsToMatch, columnsToAdd, order,
                        disallowExactMatch, stampPair,
                        leftSources, rightSources, leftStampSource, rightStampSource, redirectionIndex);
            }
            return rightTickingLeftStaticAj(control, leftTable, rightTable, columnsToMatch, columnsToAdd, order,
                    disallowExactMatch, stampPair, leftSources, rightSources, leftStampSource, rightStampSource,
                    redirectionIndex);
        } else {
            return rightStaticAj(control, leftTable, rightTable, columnsToMatch, columnsToAdd, order,
                    disallowExactMatch, stampPair, originalLeftSources, leftSources, rightSources, leftStampSource,
                    originalRightStampSource, rightStampSource, redirectionIndex);
        }
    }

    @NotNull
    private static Table rightStaticAj(JoinControl control,
            QueryTable leftTable,
            Table rightTable,
            MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd,
            SortingOrder order,
            boolean disallowExactMatch,
            MatchPair stampPair,
            ColumnSource<?>[] originalLeftSources,
            ColumnSource<?>[] leftSources,
            ColumnSource<?>[] rightSources,
            ColumnSource<?> leftStampSource,
            ColumnSource<?> originalRightStampSource,
            ColumnSource<?> rightStampSource,
            RedirectionIndex redirectionIndex) {
        final LongArraySource slots = new LongArraySource();
        final int slotCount;

        final boolean buildLeft;
        final int size;

        final Map<?, Index> leftGrouping;
        final Map<?, Index> rightGrouping;

        if (control.useGrouping(leftTable, leftSources)) {
            leftGrouping = leftSources[0].getGroupToRange(leftTable.getIndex());
            final int leftSize = leftGrouping.size();

            if (control.useGrouping(rightTable, rightSources)) {
                rightGrouping = rightSources[0].getGroupToRange(rightTable.getIndex());
                final int rightSize = rightGrouping.size();
                buildLeft = leftSize < rightSize;
                size = buildLeft ? control.tableSize(leftSize) : control.tableSize(rightSize);
            } else {
                buildLeft = true;
                size = control.tableSize(leftSize);
                rightGrouping = null;
            }
        } else if (control.useGrouping(rightTable, rightSources)) {
            rightGrouping = rightSources[0].getGroupToRange(rightTable.getIndex());
            leftGrouping = null;

            final int rightSize = rightGrouping.size();
            buildLeft = !leftTable.isLive() && leftTable.size() < rightSize;
            size = control.tableSize(Math.min(leftTable.size(), rightSize));
        } else {
            buildLeft = !leftTable.isLive() && control.buildLeft(leftTable, rightTable);
            size = control.initialBuildSize();
            leftGrouping = rightGrouping = null;
        }

        final StaticChunkedAsOfJoinStateManager asOfJoinStateManager =
                new StaticChunkedAsOfJoinStateManager(leftSources, size, originalLeftSources);

        final Pair<ArrayBackedColumnSource<?>, ObjectArraySource<Index>> leftGroupedSources;
        final int leftGroupingSize;
        if (leftGrouping != null) {
            final MutableInt groupSize = new MutableInt();
            // noinspection unchecked,rawtypes
            leftGroupedSources = AbstractColumnSource.groupingToFlatSources((ColumnSource) leftSources[0], leftGrouping,
                    leftTable.getIndex(), groupSize);
            leftGroupingSize = groupSize.intValue();
        } else {
            leftGroupedSources = null;
            leftGroupingSize = 0;
        }

        final Pair<ArrayBackedColumnSource<?>, ObjectArraySource<Index>> rightGroupedSources;
        final int rightGroupingSize;
        if (rightGrouping != null) {
            final MutableInt groupSize = new MutableInt();
            // noinspection unchecked,rawtypes
            rightGroupedSources = AbstractColumnSource.groupingToFlatSources((ColumnSource) rightSources[0],
                    rightGrouping, rightTable.getIndex(), groupSize);
            rightGroupingSize = groupSize.intValue();
        } else {
            rightGroupedSources = null;
            rightGroupingSize = 0;
        }

        if (buildLeft) {
            if (leftGroupedSources == null) {
                slotCount = asOfJoinStateManager.buildFromLeftSide(leftTable.getIndex(), leftSources, slots);
            } else {
                slotCount = asOfJoinStateManager.buildFromLeftSide(Index.CURRENT_FACTORY.getFlatIndex(leftGroupingSize),
                        new ColumnSource[] {leftGroupedSources.getFirst()}, slots);
            }
            if (rightGroupedSources == null) {
                asOfJoinStateManager.probeRight(rightTable.getIndex(), rightSources);
            } else {
                asOfJoinStateManager.probeRight(Index.CURRENT_FACTORY.getFlatIndex(rightGroupingSize),
                        new ColumnSource[] {rightGroupedSources.getFirst()});
            }
        } else {
            if (rightGroupedSources == null) {
                slotCount = asOfJoinStateManager.buildFromRightSide(rightTable.getIndex(), rightSources, slots);
            } else {
                slotCount =
                        asOfJoinStateManager.buildFromRightSide(Index.CURRENT_FACTORY.getFlatIndex(rightGroupingSize),
                                new ColumnSource[] {rightGroupedSources.getFirst()}, slots);
            }
            if (leftGroupedSources == null) {
                asOfJoinStateManager.probeLeft(leftTable.getIndex(), leftSources);
            } else {
                asOfJoinStateManager.probeLeft(Index.CURRENT_FACTORY.getFlatIndex(leftGroupingSize),
                        new ColumnSource[] {leftGroupedSources.getFirst()});
            }
        }

        final ArrayValuesCache arrayValuesCache;
        if (leftTable.isLive()) {
            if (rightGroupedSources != null) {
                asOfJoinStateManager.convertRightGrouping(slots, slotCount, rightGroupedSources.getSecond());
            } else {
                asOfJoinStateManager.convertRightBuildersToIndex(slots, slotCount);
            }
            arrayValuesCache = new ArrayValuesCache(asOfJoinStateManager.getTableSize());
            arrayValuesCache.ensureOverflow(asOfJoinStateManager.getOverflowSize());
        } else {
            arrayValuesCache = null;
            if (rightGroupedSources != null) {
                asOfJoinStateManager.convertRightGrouping(slots, slotCount, rightGroupedSources.getSecond());
            }
        }

        try (final AsOfStampContext stampContext = new AsOfStampContext(order, disallowExactMatch, leftStampSource,
                rightStampSource, originalRightStampSource);
                final ResettableWritableLongChunk<KeyIndices> keyChunk =
                        ResettableWritableLongChunk.makeResettableChunk();
                final ResettableWritableChunk<Values> valuesChunk =
                        rightStampSource.getChunkType().makeResettableWritableChunk()) {
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                final long slot = slots.getLong(slotIndex);
                Index leftIndex = asOfJoinStateManager.getLeftIndex(slot);
                if (leftIndex == null || leftIndex.empty()) {
                    continue;
                }

                final Index rightIndex = asOfJoinStateManager.getRightIndex(slot);
                if (rightIndex == null || rightIndex.empty()) {
                    continue;
                }

                if (leftGroupedSources != null) {
                    if (leftIndex.size() != 1) {
                        throw new IllegalStateException("Groupings should have exactly one index key!");
                    }
                    leftIndex = leftGroupedSources.getSecond().get(leftIndex.get(0));
                }

                if (arrayValuesCache != null) {
                    processLeftSlotWithRightCache(stampContext, leftIndex, rightIndex, redirectionIndex,
                            rightStampSource, keyChunk, valuesChunk, arrayValuesCache, slot);
                } else {
                    stampContext.processEntry(leftIndex, rightIndex, redirectionIndex);
                }
            }
        }

        final QueryTable result =
                makeResult(leftTable, rightTable, redirectionIndex, columnsToAdd, leftTable.isRefreshing());
        if (!leftTable.isRefreshing()) {
            return result;
        }

        final ModifiedColumnSet leftKeysOrStamps =
                leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));
        final LongArraySource updatedSlots = new LongArraySource();
        final ModifiedColumnSet allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        final ModifiedColumnSet.Transformer leftTransformer =
                leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());

        leftTable.listenForUpdates(new BaseTable.ShiftAwareListenerImpl(makeListenerDescription(columnsToMatch,
                stampPair, columnsToAdd, order == SortingOrder.Descending, disallowExactMatch), leftTable, result) {
            @Override
            public void onUpdate(Update upstream) {
                final Update downstream = upstream.copy();

                upstream.removed.forAllLongs(redirectionIndex::removeVoid);

                final boolean keysModified = upstream.modifiedColumnSet.containsAny(leftKeysOrStamps);

                final Index restampKeys;
                if (keysModified) {
                    upstream.getModifiedPreShift().forAllLongs(redirectionIndex::removeVoid);
                    restampKeys = upstream.modified.union(upstream.added);
                } else {
                    restampKeys = upstream.added;
                }

                try (final Index prevLeftIndex = leftTable.getIndex().getPrevIndex()) {
                    redirectionIndex.applyShift(prevLeftIndex, upstream.shifted);
                }

                if (restampKeys.nonempty()) {
                    final Index.RandomBuilder foundBuilder = Index.FACTORY.getRandomBuilder();
                    updatedSlots.ensureCapacity(restampKeys.size());
                    final int slotCount =
                            asOfJoinStateManager.probeLeft(restampKeys, leftSources, updatedSlots, foundBuilder);

                    try (final Index foundKeys = foundBuilder.getIndex();
                            final Index notFound = restampKeys.minus(foundKeys)) {
                        notFound.forAllLongs(redirectionIndex::removeVoid);
                    }

                    try (final AsOfStampContext stampContext = new AsOfStampContext(order, disallowExactMatch,
                            leftStampSource, rightStampSource, originalRightStampSource);
                            final ResettableWritableLongChunk<KeyIndices> keyChunk =
                                    ResettableWritableLongChunk.makeResettableChunk();
                            final ResettableWritableChunk<Values> valuesChunk =
                                    rightStampSource.getChunkType().makeResettableWritableChunk()) {
                        for (int ii = 0; ii < slotCount; ++ii) {
                            final long slot = updatedSlots.getLong(ii);

                            final Index leftIndex = asOfJoinStateManager.getLeftIndex(slot);
                            final Index rightIndex = asOfJoinStateManager.getRightIndex(slot);
                            assert arrayValuesCache != null;
                            processLeftSlotWithRightCache(stampContext, leftIndex, rightIndex, redirectionIndex,
                                    rightStampSource, keyChunk, valuesChunk, arrayValuesCache, slot);
                        }
                    }
                }

                downstream.modifiedColumnSet = result.modifiedColumnSet;
                leftTransformer.clearAndTransform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);
                if (keysModified) {
                    downstream.modifiedColumnSet.setAll(allRightColumns);
                }

                result.notifyListeners(downstream);

                if (keysModified) {
                    restampKeys.close();
                }
            }
        });
        return result;
    }

    private static void checkColumnConflicts(QueryTable leftTable, MatchPair[] columnsToAdd) {
        final Set<String> rightColumnsToAdd = new HashSet<>(Arrays.asList(MatchPair.getLeftColumns(columnsToAdd)));
        rightColumnsToAdd.retainAll(leftTable.getDefinition().getColumnNames());
        if (!rightColumnsToAdd.isEmpty()) {
            throw new RuntimeException("Conflicting column names " + rightColumnsToAdd);
        }
    }

    private static class ArrayValuesCache {
        final ObjectArraySource<long[]> cacheStampKeys;
        final ObjectArraySource<Object> cacheStampValues;

        final ObjectArraySource<long[]> overflowCachedStampKeys;
        final ObjectArraySource<Object> overflowCachedStampValues;

        private ArrayValuesCache(int size) {
            cacheStampKeys = new ObjectArraySource<>(long[].class);
            cacheStampValues = new ObjectArraySource<>(Object.class);

            cacheStampKeys.ensureCapacity(size);
            cacheStampValues.ensureCapacity(size);

            overflowCachedStampKeys = new ObjectArraySource<>(long[].class);
            overflowCachedStampValues = new ObjectArraySource<>(Object.class);
        }

        long[] getKeys(long slot) {
            if (StaticChunkedAsOfJoinStateManager.isOverflowLocation(slot)) {
                return overflowCachedStampKeys
                        .get(StaticChunkedAsOfJoinStateManager.hashLocationToOverflowLocation(slot));
            } else {
                return cacheStampKeys.get(slot);
            }
        }

        Object getValues(long slot) {
            if (StaticChunkedAsOfJoinStateManager.isOverflowLocation(slot)) {
                return overflowCachedStampValues
                        .get(StaticChunkedAsOfJoinStateManager.hashLocationToOverflowLocation(slot));
            } else {
                return cacheStampValues.get(slot);
            }
        }

        void setKeysAndValues(long slot, long[] keyIndices, Object StampArray) {
            if (StaticChunkedAsOfJoinStateManager.isOverflowLocation(slot)) {
                final long overflowLocation = StaticChunkedAsOfJoinStateManager.hashLocationToOverflowLocation(slot);
                overflowCachedStampKeys.ensureCapacity(overflowLocation + 1);
                overflowCachedStampValues.ensureCapacity(overflowLocation + 1);
                overflowCachedStampKeys.set(overflowLocation, keyIndices);
                overflowCachedStampValues.set(overflowLocation, StampArray);
            } else {
                cacheStampKeys.set(slot, keyIndices);
                cacheStampValues.set(slot, StampArray);
            }
        }

        void ensureOverflow(int overflowSize) {
            overflowCachedStampKeys.ensureCapacity(overflowSize);
            overflowCachedStampValues.ensureCapacity(overflowSize);
        }
    }

    private static void processLeftSlotWithRightCache(AsOfStampContext stampContext,
            Index leftIndex, Index rightIndex, RedirectionIndex redirectionIndex,
            ColumnSource<?> rightStampSource,
            ResettableWritableLongChunk<KeyIndices> keyChunk, ResettableWritableChunk<Values> valuesChunk,
            ArrayValuesCache arrayValuesCache,
            long slot) {
        final long[] rightStampKeys = arrayValuesCache.getKeys(slot);
        if (rightStampKeys == null) {
            final int rightSize = rightIndex.intSize();
            long[] keyIndices = new long[rightSize];

            Object rightStampArray = rightStampSource.getChunkType().makeArray(rightSize);

            keyChunk.resetFromTypedArray(keyIndices, 0, rightSize);
            valuesChunk.resetFromArray(rightStampArray, 0, rightSize);

            // noinspection unchecked
            stampContext.getAndCompactStamps(rightIndex, keyChunk, valuesChunk);

            if (keyChunk.size() < rightSize) {
                // we will hold onto these things "forever", so we would like to avoid making them too large
                keyIndices = Arrays.copyOf(keyIndices, keyChunk.size());
                final Object compactedRightValues = rightStampSource.getChunkType().makeArray(keyChunk.size());
                // noinspection SuspiciousSystemArraycopy
                System.arraycopy(rightStampArray, 0, compactedRightValues, 0, keyChunk.size());
                rightStampArray = compactedRightValues;

                keyChunk.resetFromTypedArray(keyIndices, 0, keyChunk.size());
                valuesChunk.resetFromArray(rightStampArray, 0, keyChunk.size());
            }

            arrayValuesCache.setKeysAndValues(slot, keyIndices, rightStampArray);
        } else {
            keyChunk.resetFromTypedArray(rightStampKeys, 0, rightStampKeys.length);
            valuesChunk.resetFromArray(arrayValuesCache.getValues(slot), 0, rightStampKeys.length);
        }

        // noinspection unchecked
        stampContext.processEntry(leftIndex, valuesChunk, keyChunk, redirectionIndex);
    }

    /**
     * If the asOfJoinStateManager is null, it means we are passing in the leftIndex. If the leftIndex is null; we are
     * passing in the asOfJoinStateManager and should obtain the leftIndex from the state manager if necessary.
     */
    private static void getCachedLeftStampsAndKeys(RightIncrementalChunkedAsOfJoinStateManager asOfJoinStateManager,
            Index leftIndex,
            ColumnSource<?> leftStampSource,
            SizedSafeCloseable<ChunkSource.FillContext> fillContext,
            SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> sortContext,
            ResettableWritableLongChunk<KeyIndices> keyChunk, ResettableWritableChunk<Values> valuesChunk,
            ArrayValuesCache arrayValuesCache,
            long slot) {
        final long[] leftStampKeys = arrayValuesCache.getKeys(slot);
        if (leftStampKeys == null) {
            if (leftIndex == null) {
                leftIndex = asOfJoinStateManager.getAndClearLeftIndex(slot);
                if (leftIndex == null) {
                    leftIndex = Index.FACTORY.getEmptyIndex();
                }
            }
            final int leftSize = leftIndex.intSize();
            final long[] keyIndices = new long[leftSize];

            final Object leftStampArray = leftStampSource.getChunkType().makeArray(leftSize);

            keyChunk.resetFromTypedArray(keyIndices, 0, leftSize);
            valuesChunk.resetFromArray(leftStampArray, 0, leftSize);

            // noinspection unchecked
            leftIndex.fillKeyIndicesChunk(keyChunk);

            // noinspection unchecked
            leftStampSource.fillChunk(fillContext.ensureCapacity(leftSize), valuesChunk, leftIndex);

            // noinspection unchecked
            sortContext.ensureCapacity(leftSize).sort(keyChunk, valuesChunk);

            arrayValuesCache.setKeysAndValues(slot, keyIndices, leftStampArray);
        } else {
            keyChunk.resetFromTypedArray(leftStampKeys, 0, leftStampKeys.length);
            valuesChunk.resetFromArray(arrayValuesCache.getValues(slot), 0, leftStampKeys.length);
        }
    }

    private static Table zeroKeyAj(JoinControl control, QueryTable leftTable, QueryTable rightTable,
            MatchPair[] columnsToAdd, MatchPair stampPair, ColumnSource<?> leftStampSource,
            ColumnSource<?> originalRightStampSource, ColumnSource<?> rightStampSource, SortingOrder order,
            boolean disallowExactMatch, final RedirectionIndex redirectionIndex) {
        if (rightTable.isLive() && leftTable.isLive()) {
            return zeroKeyAjBothIncremental(control, leftTable, rightTable, columnsToAdd, stampPair, leftStampSource,
                    rightStampSource, order, disallowExactMatch, redirectionIndex);
        } else if (rightTable.isLive()) {
            return zeroKeyAjRightIncremental(control, leftTable, rightTable, columnsToAdd, stampPair, leftStampSource,
                    rightStampSource, order, disallowExactMatch, redirectionIndex);
        } else {
            return zeroKeyAjRightStatic(leftTable, rightTable, columnsToAdd, stampPair, leftStampSource,
                    originalRightStampSource, rightStampSource, order, disallowExactMatch, redirectionIndex);
        }
    }

    private static Table rightTickingLeftStaticAj(JoinControl control,
            QueryTable leftTable,
            QueryTable rightTable,
            MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd,
            SortingOrder order,
            boolean disallowExactMatch,
            MatchPair stampPair,
            ColumnSource<?>[] leftSources,
            ColumnSource<?>[] rightSources,
            ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource,
            RedirectionIndex redirectionIndex) {
        if (leftTable.isRefreshing()) {
            throw new IllegalStateException();
        }

        final boolean reverse = order == SortingOrder.Descending;

        final ChunkType stampChunkType = rightStampSource.getChunkType();
        final Supplier<SegmentedSortedArray> ssaFactory =
                SegmentedSortedArray.makeFactory(stampChunkType, reverse, control.rightSsaNodeSize());
        final ChunkSsaStamp chunkSsaStamp = ChunkSsaStamp.make(stampChunkType, reverse);

        final int tableSize = control.initialBuildSize();
        final RightIncrementalChunkedAsOfJoinStateManager asOfJoinStateManager =
                new RightIncrementalChunkedAsOfJoinStateManager(leftSources, tableSize);

        final LongArraySource slots = new LongArraySource();
        final int slotCount = asOfJoinStateManager.buildFromLeftSide(leftTable.getIndex(), leftSources, slots);
        asOfJoinStateManager.probeRightInitial(rightTable.getIndex(), rightSources);

        final ArrayValuesCache leftValuesCache = new ArrayValuesCache(asOfJoinStateManager.getTableSize());
        leftValuesCache.ensureOverflow(asOfJoinStateManager.getOverflowSize());

        final SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> sortContext =
                new SizedSafeCloseable<>(size -> LongSortKernel.makeContext(stampChunkType, order, size, true));
        final SizedSafeCloseable<ChunkSource.FillContext> leftStampFillContext =
                new SizedSafeCloseable<>(leftStampSource::makeFillContext);
        final SizedSafeCloseable<ChunkSource.FillContext> rightStampFillContext =
                new SizedSafeCloseable<>(rightStampSource::makeFillContext);
        final SizedChunk<Values> rightValues = new SizedChunk<>(stampChunkType);
        final SizedLongChunk<KeyIndices> rightKeyIndices = new SizedLongChunk<>();
        final SizedLongChunk<KeyIndices> rightKeysForLeft = new SizedLongChunk<>();

        // if we have an error the closeableList cleans up for us; if not they can be used later
        try (final ResettableWritableLongChunk<KeyIndices> leftKeyChunk =
                ResettableWritableLongChunk.makeResettableChunk();
                final ResettableWritableChunk<Values> leftValuesChunk =
                        rightStampSource.getChunkType().makeResettableWritableChunk()) {
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                final long slot = slots.getLong(slotIndex);
                final Index leftIndex = asOfJoinStateManager.getAndClearLeftIndex(slot);
                assert leftIndex != null;
                assert leftIndex.size() > 0;

                final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot, (rightIndex) -> {
                    final SegmentedSortedArray ssa = ssaFactory.get();
                    final int slotSize = rightIndex.intSize();
                    if (slotSize > 0) {
                        rightStampSource.fillChunk(rightStampFillContext.ensureCapacity(slotSize),
                                rightValues.ensureCapacity(slotSize), rightIndex);
                        rightIndex.fillKeyIndicesChunk(rightKeyIndices.ensureCapacity(slotSize));
                        sortContext.ensureCapacity(slotSize).sort(rightKeyIndices.get(), rightValues.get());
                        ssa.insert(rightValues.get(), rightKeyIndices.get());
                    }
                    return ssa;
                });

                getCachedLeftStampsAndKeys(null, leftIndex, leftStampSource, leftStampFillContext, sortContext,
                        leftKeyChunk, leftValuesChunk, leftValuesCache, slot);

                if (rightSsa.size() == 0) {
                    continue;
                }

                final WritableLongChunk<KeyIndices> rightKeysForLeftChunk =
                        rightKeysForLeft.ensureCapacity(leftIndex.intSize());

                // noinspection unchecked
                chunkSsaStamp.processEntry(leftValuesChunk, leftKeyChunk, rightSsa, rightKeysForLeftChunk,
                        disallowExactMatch);

                for (int ii = 0; ii < leftKeyChunk.size(); ++ii) {
                    final long index = rightKeysForLeftChunk.get(ii);
                    if (index != Index.NULL_KEY) {
                        redirectionIndex.put(leftKeyChunk.get(ii), index);
                    }
                }
            }
        }

        // we will close them now, but the listener is able to resurrect them as needed
        SafeCloseable.closeArray(sortContext, leftStampFillContext, rightStampFillContext, rightValues, rightKeyIndices,
                rightKeysForLeft);

        final QueryTable result = makeResult(leftTable, rightTable, redirectionIndex, columnsToAdd, true);

        final ModifiedColumnSet rightMatchColumns =
                rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
        final ModifiedColumnSet rightStampColumn = rightTable.newModifiedColumnSet(stampPair.right());
        final ModifiedColumnSet rightAddedColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        final ModifiedColumnSet.Transformer rightTransformer =
                rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);

        final ObjectArraySource<Index.SequentialBuilder> sequentialBuilders =
                new ObjectArraySource<>(Index.SequentialBuilder.class);

        rightTable.listenForUpdates(new BaseTable.ShiftAwareListenerImpl(
                makeListenerDescription(columnsToMatch, stampPair, columnsToAdd, reverse, disallowExactMatch),
                rightTable, result) {
            @Override
            public void onUpdate(Update upstream) {
                final Update downstream = new Update();
                downstream.added = Index.FACTORY.getEmptyIndex();
                downstream.removed = Index.FACTORY.getEmptyIndex();
                downstream.shifted = IndexShiftData.EMPTY;
                downstream.modifiedColumnSet = result.modifiedColumnSet;

                final boolean keysModified = upstream.modifiedColumnSet.containsAny(rightMatchColumns);
                final boolean stampModified = upstream.modifiedColumnSet.containsAny(rightStampColumn);

                final Index.RandomBuilder modifiedBuilder = Index.FACTORY.getRandomBuilder();

                final Index restampRemovals;
                final Index restampAdditions;
                if (keysModified || stampModified) {
                    restampAdditions = upstream.added.union(upstream.modified);
                    restampRemovals = upstream.removed.union(upstream.getModifiedPreShift());
                } else {
                    restampAdditions = upstream.added;
                    restampRemovals = upstream.removed;
                }

                sequentialBuilders.ensureCapacity(Math.max(restampRemovals.size(), restampAdditions.size()));

                // We first do a probe pass, adding all of the removals to a builder in the as of join state manager
                final int removedSlotCount =
                        asOfJoinStateManager.markForRemoval(restampRemovals, rightSources, slots, sequentialBuilders);

                // Now that everything is marked, process the removals state by state, just as if we were doing the zero
                // key case: when removing a row, record the stamp, redirection key, and prior redirection key. Binary
                // search
                // in the left for the removed key to find the smallest value geq the removed right. Update all rows
                // with the removed redirection to the previous key.


                try (final ResettableWritableLongChunk<KeyIndices> leftKeyChunk =
                        ResettableWritableLongChunk.makeResettableChunk();
                        final ResettableWritableChunk<Values> leftValuesChunk =
                                rightStampSource.getChunkType().makeResettableWritableChunk();
                        final SizedLongChunk<KeyIndices> priorRedirections = new SizedLongChunk<>()) {
                    for (int slotIndex = 0; slotIndex < removedSlotCount; ++slotIndex) {
                        final long slot = slots.getLong(slotIndex);

                        final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot);

                        final Index rightRemoved = sequentialBuilders.get(slotIndex).getIndex();
                        sequentialBuilders.set(slotIndex, null);
                        final int slotSize = rightRemoved.intSize();


                        rightStampSource.fillPrevChunk(rightStampFillContext.ensureCapacity(slotSize),
                                rightValues.ensureCapacity(slotSize), rightRemoved);
                        rightRemoved.fillKeyIndicesChunk(rightKeyIndices.ensureCapacity(slotSize));
                        sortContext.ensureCapacity(slotSize).sort(rightKeyIndices.get(), rightValues.get());

                        getCachedLeftStampsAndKeys(asOfJoinStateManager, null, leftStampSource, leftStampFillContext,
                                sortContext, leftKeyChunk, leftValuesChunk, leftValuesCache, slot);

                        priorRedirections.ensureCapacity(slotSize).setSize(slotSize);

                        rightSsa.removeAndGetPrior(rightValues.get(), rightKeyIndices.get(), priorRedirections.get());

                        // noinspection unchecked
                        chunkSsaStamp.processRemovals(leftValuesChunk, leftKeyChunk, rightValues.get(),
                                rightKeyIndices.get(), priorRedirections.get(), redirectionIndex, modifiedBuilder,
                                disallowExactMatch);

                        rightRemoved.close();
                    }
                }

                // After all the removals are done, we do the shifts
                if (upstream.shifted.nonempty()) {
                    try (final Index fullPrevIndex = rightTable.getIndex().getPrevIndex();
                            final Index previousToShift = fullPrevIndex.minus(restampRemovals)) {
                        if (previousToShift.nonempty()) {
                            try (final ResettableWritableLongChunk<KeyIndices> leftKeyChunk =
                                    ResettableWritableLongChunk.makeResettableChunk();
                                    final ResettableWritableChunk<Values> leftValuesChunk =
                                            rightStampSource.getChunkType().makeResettableWritableChunk()) {
                                final IndexShiftData.Iterator sit = upstream.shifted.applyIterator();
                                while (sit.hasNext()) {
                                    sit.next();
                                    final Index indexToShift =
                                            previousToShift.subindexByKey(sit.beginRange(), sit.endRange());
                                    if (indexToShift.empty()) {
                                        indexToShift.close();
                                        continue;
                                    }

                                    final int shiftedSlots = asOfJoinStateManager.gatherShiftIndex(indexToShift,
                                            rightSources, slots, sequentialBuilders);
                                    indexToShift.close();

                                    for (int slotIndex = 0; slotIndex < shiftedSlots; ++slotIndex) {
                                        final long slot = slots.getLong(slotIndex);
                                        try (final Index slotShiftIndex =
                                                sequentialBuilders.get(slotIndex).getIndex()) {
                                            sequentialBuilders.set(slotIndex, null);

                                            final int shiftSize = slotShiftIndex.intSize();

                                            getCachedLeftStampsAndKeys(asOfJoinStateManager, null, leftStampSource,
                                                    leftStampFillContext, sortContext, leftKeyChunk, leftValuesChunk,
                                                    leftValuesCache, slot);

                                            rightStampSource.fillPrevChunk(
                                                    rightStampFillContext.ensureCapacity(shiftSize),
                                                    rightValues.ensureCapacity(shiftSize), slotShiftIndex);

                                            final SegmentedSortedArray rightSsa =
                                                    asOfJoinStateManager.getRightSsa(slot);

                                            slotShiftIndex
                                                    .fillKeyIndicesChunk(rightKeyIndices.ensureCapacity(shiftSize));
                                            sortContext.ensureCapacity(shiftSize).sort(rightKeyIndices.get(),
                                                    rightValues.get());

                                            if (sit.polarityReversed()) {
                                                // noinspection unchecked
                                                chunkSsaStamp.applyShift(leftValuesChunk, leftKeyChunk,
                                                        rightValues.get(), rightKeyIndices.get(), sit.shiftDelta(),
                                                        redirectionIndex, disallowExactMatch);
                                                rightSsa.applyShiftReverse(rightValues.get(), rightKeyIndices.get(),
                                                        sit.shiftDelta());
                                            } else {
                                                // noinspection unchecked
                                                chunkSsaStamp.applyShift(leftValuesChunk, leftKeyChunk,
                                                        rightValues.get(), rightKeyIndices.get(), sit.shiftDelta(),
                                                        redirectionIndex, disallowExactMatch);
                                                rightSsa.applyShift(rightValues.get(), rightKeyIndices.get(),
                                                        sit.shiftDelta());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // next we do the additions
                final int addedSlotCount =
                        asOfJoinStateManager.probeAdditions(restampAdditions, rightSources, slots, sequentialBuilders);

                try (final SizedChunk<Values> nextRightValue = new SizedChunk<>(stampChunkType);
                        final SizedChunk<Values> rightStampChunk = new SizedChunk<>(stampChunkType);
                        final SizedLongChunk<KeyIndices> insertedIndices = new SizedLongChunk<>();
                        final SizedBooleanChunk<Any> retainStamps = new SizedBooleanChunk<>();
                        final SizedSafeCloseable<ColumnSource.FillContext> rightStampFillContext =
                                new SizedSafeCloseable<>(rightStampSource::makeFillContext);
                        final ResettableWritableLongChunk<KeyIndices> leftKeyChunk =
                                ResettableWritableLongChunk.makeResettableChunk();
                        final ResettableWritableChunk<Values> leftValuesChunk =
                                rightStampSource.getChunkType().makeResettableWritableChunk()) {
                    final ChunkEquals stampChunkEquals = ChunkEquals.makeEqual(stampChunkType);
                    final CompactKernel stampCompact = CompactKernel.makeCompact(stampChunkType);

                    // When adding a row to the right hand side: we need to know which left hand side might be
                    // responsive. If we are a duplicate stamp and not the last one, we ignore it. Next, we should
                    // binary
                    // search in the left for the first value >=, everything up until the next extant right value should
                    // be
                    // restamped with our value
                    for (int slotIndex = 0; slotIndex < addedSlotCount; ++slotIndex) {
                        final long slot = slots.getLong(slotIndex);

                        final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot);

                        final Index rightAdded = sequentialBuilders.get(slotIndex).getIndex();
                        sequentialBuilders.set(slotIndex, null);

                        final int rightSize = rightAdded.intSize();

                        rightStampSource.fillChunk(rightStampFillContext.ensureCapacity(rightSize),
                                rightStampChunk.ensureCapacity(rightSize), rightAdded);
                        rightAdded.fillKeyIndicesChunk(insertedIndices.ensureCapacity(rightSize));
                        sortContext.ensureCapacity(rightSize).sort(insertedIndices.get(), rightStampChunk.get());

                        final int valuesWithNext = rightSsa.insertAndGetNextValue(rightStampChunk.get(),
                                insertedIndices.get(), nextRightValue.ensureCapacity(rightSize));

                        final boolean endsWithLastValue = valuesWithNext != rightStampChunk.get().size();
                        if (endsWithLastValue) {
                            Assert.eq(valuesWithNext, "valuesWithNext", rightStampChunk.get().size() - 1,
                                    "rightStampChunk.size() - 1");
                            rightStampChunk.get().setSize(valuesWithNext);
                            stampChunkEquals.notEqual(rightStampChunk.get(), nextRightValue.get(),
                                    retainStamps.ensureCapacity(rightSize));
                            stampCompact.compact(nextRightValue.get(), retainStamps.get());

                            retainStamps.get().setSize(rightSize);
                            retainStamps.get().set(valuesWithNext, true);
                            rightStampChunk.get().setSize(rightSize);
                        } else {
                            // remove duplicates
                            stampChunkEquals.notEqual(rightStampChunk.get(), nextRightValue.get(),
                                    retainStamps.ensureCapacity(rightSize));
                            stampCompact.compact(nextRightValue.get(), retainStamps.get());
                        }
                        LongCompactKernel.compact(insertedIndices.get(), retainStamps.get());
                        stampCompact.compact(rightStampChunk.get(), retainStamps.get());

                        getCachedLeftStampsAndKeys(asOfJoinStateManager, null, leftStampSource, leftStampFillContext,
                                sortContext, leftKeyChunk, leftValuesChunk, leftValuesCache, slot);

                        // noinspection unchecked
                        chunkSsaStamp.processInsertion(leftValuesChunk, leftKeyChunk, rightStampChunk.get(),
                                insertedIndices.get(), nextRightValue.get(), redirectionIndex, modifiedBuilder,
                                endsWithLastValue, disallowExactMatch);
                    }

                    // and then finally we handle the case where the keys and stamps were not modified, but we must
                    // identify
                    // the responsive modifications.
                    if (!keysModified && !stampModified && upstream.modified.nonempty()) {
                        // next we do the additions
                        final int modifiedSlotCount = asOfJoinStateManager.gatherModifications(upstream.modified,
                                rightSources, slots, sequentialBuilders);

                        for (int slotIndex = 0; slotIndex < modifiedSlotCount; ++slotIndex) {
                            final long slot = slots.getLong(slotIndex);

                            try (final Index rightModified = sequentialBuilders.get(slotIndex).getIndex()) {
                                sequentialBuilders.set(slotIndex, null);
                                final int rightSize = rightModified.intSize();

                                rightStampSource.fillChunk(rightStampFillContext.ensureCapacity(rightSize),
                                        rightValues.ensureCapacity(rightSize), rightModified);
                                rightModified.fillKeyIndicesChunk(rightKeyIndices.ensureCapacity(rightSize));
                                sortContext.ensureCapacity(rightSize).sort(rightKeyIndices.get(), rightValues.get());

                                getCachedLeftStampsAndKeys(asOfJoinStateManager, null, leftStampSource,
                                        leftStampFillContext, sortContext, leftKeyChunk, leftValuesChunk,
                                        leftValuesCache, slot);

                                // noinspection unchecked
                                chunkSsaStamp.findModified(0, leftValuesChunk, leftKeyChunk, redirectionIndex,
                                        rightValues.get(), rightKeyIndices.get(), modifiedBuilder, disallowExactMatch);
                            }
                        }

                        rightTransformer.transform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);
                    }
                }

                SafeCloseable.closeArray(sortContext, leftStampFillContext, rightStampFillContext, rightValues,
                        rightKeyIndices, rightKeysForLeft);

                downstream.modified = modifiedBuilder.getIndex();

                final boolean processedAdditionsOrRemovals = removedSlotCount > 0 || addedSlotCount > 0;
                if (keysModified || stampModified || processedAdditionsOrRemovals) {
                    downstream.modifiedColumnSet.setAll(rightAddedColumns);
                }

                result.notifyListeners(downstream);

                if (stampModified || keysModified) {
                    restampAdditions.close();
                    restampRemovals.close();
                }
            }
        });

        return result;
    }

    public interface SsaFactory extends Function<Index, SegmentedSortedArray>, SafeCloseable {
    }

    private static Table bothIncrementalAj(JoinControl control,
            QueryTable leftTable,
            QueryTable rightTable,
            MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd,
            SortingOrder order,
            boolean disallowExactMatch,
            MatchPair stampPair,
            ColumnSource<?>[] leftSources,
            ColumnSource<?>[] rightSources,
            ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource,
            RedirectionIndex redirectionIndex) {
        final boolean reverse = order == SortingOrder.Descending;

        final ChunkType stampChunkType = rightStampSource.getChunkType();
        final Supplier<SegmentedSortedArray> ssaFactory =
                SegmentedSortedArray.makeFactory(stampChunkType, reverse, control.rightSsaNodeSize());
        final SsaSsaStamp ssaSsaStamp = SsaSsaStamp.make(stampChunkType, reverse);

        final RightIncrementalChunkedAsOfJoinStateManager asOfJoinStateManager =
                new RightIncrementalChunkedAsOfJoinStateManager(leftSources, control.initialBuildSize());

        final LongArraySource slots = new LongArraySource();
        int slotCount = asOfJoinStateManager.buildFromLeftSide(leftTable.getIndex(), leftSources, slots);
        slotCount = asOfJoinStateManager.buildFromRightSide(rightTable.getIndex(), rightSources, slots, slotCount);

        // These contexts and chunks will be closed when the SSA factory itself is closed by the destroy function of the
        // BucketedChunkedAjMergedListener
        final SizedSafeCloseable<ColumnSource.FillContext> rightStampFillContext =
                new SizedSafeCloseable<>(rightStampSource::makeFillContext);
        final SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> sortKernel =
                new SizedSafeCloseable<>(size -> LongSortKernel.makeContext(stampChunkType, order, size, true));
        final SizedSafeCloseable<ColumnSource.FillContext> leftStampFillContext =
                new SizedSafeCloseable<>(leftStampSource::makeFillContext);
        final SizedLongChunk<KeyIndices> leftStampKeys = new SizedLongChunk<>();
        final SizedChunk<Values> leftStampValues = new SizedChunk<>(stampChunkType);
        final SizedChunk<Values> rightStampValues = new SizedChunk<>(stampChunkType);
        final SizedLongChunk<KeyIndices> rightStampKeys = new SizedLongChunk<>();

        final SsaFactory rightSsaFactory = new SsaFactory() {
            @Override
            public void close() {
                SafeCloseable.closeArray(rightStampFillContext, rightStampValues, rightStampKeys);
            }

            @Override
            public SegmentedSortedArray apply(Index rightIndex) {
                final SegmentedSortedArray ssa = ssaFactory.get();
                final int slotSize = rightIndex.intSize();
                if (slotSize > 0) {
                    rightIndex.fillKeyIndicesChunk(rightStampKeys.ensureCapacity(slotSize));
                    rightStampSource.fillChunk(rightStampFillContext.ensureCapacity(slotSize),
                            rightStampValues.ensureCapacity(slotSize), rightIndex);
                    sortKernel.ensureCapacity(slotSize).sort(rightStampKeys.get(), rightStampValues.get());
                    ssa.insert(rightStampValues.get(), rightStampKeys.get());
                }
                return ssa;
            }
        };

        final SsaFactory leftSsaFactory = new SsaFactory() {
            @Override
            public void close() {
                SafeCloseable.closeArray(sortKernel, leftStampFillContext, leftStampValues, leftStampKeys);
            }

            @Override
            public SegmentedSortedArray apply(Index leftIndex) {
                final SegmentedSortedArray ssa = ssaFactory.get();
                final int slotSize = leftIndex.intSize();
                if (slotSize > 0) {

                    leftStampSource.fillChunk(leftStampFillContext.ensureCapacity(slotSize),
                            leftStampValues.ensureCapacity(slotSize), leftIndex);

                    leftIndex.fillKeyIndicesChunk(leftStampKeys.ensureCapacity(slotSize));

                    sortKernel.ensureCapacity(slotSize).sort(leftStampKeys.get(), leftStampValues.get());

                    ssa.insert(leftStampValues.get(), leftStampKeys.get());
                }
                return ssa;
            }
        };

        final QueryTable result;
        // if we fail to create the table, then we should make sure to close the ssa factories, which contain a context.
        // if we are successful, then the mergedJoinListener will own them and be responsible for closing them
        try (final SafeCloseableList closeableList = new SafeCloseableList(leftSsaFactory, rightSsaFactory)) {
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                final long slot = slots.getLong(slotIndex);

                // if either initial state is empty, we would prefer to leave things as an index rather than process
                // them into an ssa
                final byte state = asOfJoinStateManager.getState(slot);
                if ((state
                        & RightIncrementalChunkedAsOfJoinStateManager.ENTRY_RIGHT_MASK) == RightIncrementalChunkedAsOfJoinStateManager.ENTRY_RIGHT_IS_EMPTY) {
                    continue;
                }
                if ((state
                        & RightIncrementalChunkedAsOfJoinStateManager.ENTRY_LEFT_MASK) == RightIncrementalChunkedAsOfJoinStateManager.ENTRY_LEFT_IS_EMPTY) {
                    continue;
                }

                final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot, rightSsaFactory);
                final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot, leftSsaFactory);
                ssaSsaStamp.processEntry(leftSsa, rightSsa, redirectionIndex, disallowExactMatch);
            }

            result = makeResult(leftTable, rightTable, redirectionIndex, columnsToAdd, true);
            closeableList.clear();
        }

        final String listenerDescription =
                makeListenerDescription(columnsToMatch, stampPair, columnsToAdd, reverse, disallowExactMatch);
        final JoinListenerRecorder leftRecorder =
                new JoinListenerRecorder(true, listenerDescription, leftTable, result);
        final JoinListenerRecorder rightRecorder =
                new JoinListenerRecorder(false, listenerDescription, rightTable, result);

        final BucketedChunkedAjMergedListener mergedJoinListener =
                new BucketedChunkedAjMergedListener(leftRecorder, rightRecorder,
                        listenerDescription, result, leftTable, rightTable, columnsToMatch, stampPair, columnsToAdd,
                        leftSources,
                        rightSources, leftStampSource, rightStampSource,
                        leftSsaFactory, rightSsaFactory, order, disallowExactMatch,
                        ssaSsaStamp, control, asOfJoinStateManager, redirectionIndex);

        leftRecorder.setMergedListener(mergedJoinListener);
        rightRecorder.setMergedListener(mergedJoinListener);

        leftTable.listenForUpdates(leftRecorder);
        rightTable.listenForUpdates(rightRecorder);

        result.addParentReference(mergedJoinListener);

        leftSsaFactory.close();
        rightSsaFactory.close();

        return result;
    }

    private static Table zeroKeyAjBothIncremental(JoinControl control, QueryTable leftTable, QueryTable rightTable,
            MatchPair[] columnsToAdd, MatchPair stampPair, ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource, SortingOrder order, boolean disallowExactMatch,
            final RedirectionIndex redirectionIndex) {
        final boolean reverse = order == SortingOrder.Descending;

        final ChunkType stampChunkType = rightStampSource.getChunkType();
        final int leftNodeSize = control.leftSsaNodeSize();
        final int rightNodeSize = control.rightSsaNodeSize();
        final SegmentedSortedArray leftSsa = SegmentedSortedArray.make(stampChunkType, reverse, leftNodeSize);
        final SegmentedSortedArray rightSsa = SegmentedSortedArray.make(stampChunkType, reverse, rightNodeSize);

        fillSsaWithSort(rightTable, rightStampSource, rightNodeSize, rightSsa, order);
        fillSsaWithSort(leftTable, leftStampSource, leftNodeSize, leftSsa, order);

        final SsaSsaStamp ssaSsaStamp = SsaSsaStamp.make(stampChunkType, reverse);
        ssaSsaStamp.processEntry(leftSsa, rightSsa, redirectionIndex, disallowExactMatch);

        final QueryTable result = makeResult(leftTable, rightTable, redirectionIndex, columnsToAdd, true);

        final String listenerDescription = makeListenerDescription(MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY, stampPair,
                columnsToAdd, reverse, disallowExactMatch);
        final JoinListenerRecorder leftRecorder =
                new JoinListenerRecorder(true, listenerDescription, leftTable, result);
        final JoinListenerRecorder rightRecorder =
                new JoinListenerRecorder(false, listenerDescription, rightTable, result);

        final ZeroKeyChunkedAjMergedListener mergedJoinListener =
                new ZeroKeyChunkedAjMergedListener(leftRecorder, rightRecorder,
                        listenerDescription, result, leftTable, rightTable, stampPair, columnsToAdd,
                        leftStampSource, rightStampSource, order, disallowExactMatch,
                        ssaSsaStamp, leftSsa, rightSsa, redirectionIndex, control);

        leftRecorder.setMergedListener(mergedJoinListener);
        rightRecorder.setMergedListener(mergedJoinListener);

        leftTable.listenForUpdates(leftRecorder);
        rightTable.listenForUpdates(rightRecorder);

        result.addParentReference(mergedJoinListener);

        return result;
    }

    @NotNull
    private static String makeListenerDescription(MatchPair[] columnsToMatch, MatchPair stampPair,
            MatchPair[] columnsToAdd, boolean reverse, boolean disallowExactMatch) {
        final String stampString = disallowExactMatch ? makeDisallowExactStampString(stampPair, reverse)
                : MatchPair.matchString(stampPair);
        return (reverse ? "r" : "") + "aj([" + MatchPair.matchString(columnsToMatch) + ", " + stampString + "], ["
                + MatchPair.matchString(columnsToAdd) + "])";
    }

    @NotNull
    private static String makeDisallowExactStampString(MatchPair stampPair, boolean reverse) {
        final char operator = reverse ? '>' : '<';
        return stampPair.leftColumn + operator + stampPair.rightColumn;
    }


    private static void fillSsaWithSort(QueryTable rightTable, ColumnSource<?> stampSource, int nodeSize,
            SegmentedSortedArray ssa, SortingOrder order) {
        try (final ColumnSource.FillContext context = stampSource.makeFillContext(nodeSize);
                final OrderedKeys.Iterator okit = rightTable.getIndex().getOrderedKeysIterator();
                final WritableChunk<Values> stampChunk = stampSource.getChunkType().makeWritableChunk(nodeSize);
                final WritableLongChunk<KeyIndices> keyChunk = WritableLongChunk.makeWritableChunk(nodeSize);
                final LongSortKernel<Values, KeyIndices> sortKernel =
                        LongSortKernel.makeContext(stampSource.getChunkType(), order, nodeSize, true)) {
            while (okit.hasMore()) {
                final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(nodeSize);
                stampSource.fillChunk(context, stampChunk, chunkOk);
                chunkOk.fillKeyIndicesChunk(keyChunk);

                sortKernel.sort(keyChunk, stampChunk);

                ssa.insert(stampChunk, keyChunk);
            }
        }
    }

    private static Table zeroKeyAjRightIncremental(JoinControl control, QueryTable leftTable, QueryTable rightTable,
            MatchPair[] columnsToAdd, MatchPair stampPair, ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource, SortingOrder order, boolean disallowExactMatch,
            final RedirectionIndex redirectionIndex) {
        final boolean reverse = order == SortingOrder.Descending;

        final ChunkType stampChunkType = rightStampSource.getChunkType();
        final int rightNodeSize = control.rightSsaNodeSize();
        final int rightChunkSize = control.rightChunkSize();
        final SegmentedSortedArray ssa = SegmentedSortedArray.make(stampChunkType, reverse, rightNodeSize);

        fillSsaWithSort(rightTable, rightStampSource, rightChunkSize, ssa, order);

        final int leftSize = leftTable.intSize();
        final WritableChunk<Values> leftStampValues = stampChunkType.makeWritableChunk(leftSize);
        final WritableLongChunk<KeyIndices> leftStampKeys = WritableLongChunk.makeWritableChunk(leftSize);
        leftTable.getIndex().fillKeyIndicesChunk(leftStampKeys);
        try (final ColumnSource.FillContext context = leftStampSource.makeFillContext(leftSize)) {
            leftStampSource.fillChunk(context, leftStampValues, leftTable.getIndex());
        }

        try (final LongSortKernel<Values, KeyIndices> sortKernel =
                LongSortKernel.makeContext(stampChunkType, order, leftSize, true)) {
            sortKernel.sort(leftStampKeys, leftStampValues);
        }

        final ChunkSsaStamp chunkSsaStamp = ChunkSsaStamp.make(stampChunkType, reverse);
        try (final WritableLongChunk<KeyIndices> rightKeysForLeft = WritableLongChunk.makeWritableChunk(leftSize)) {
            chunkSsaStamp.processEntry(leftStampValues, leftStampKeys, ssa, rightKeysForLeft, disallowExactMatch);

            for (int ii = 0; ii < leftStampKeys.size(); ++ii) {
                final long index = rightKeysForLeft.get(ii);
                if (index != Index.NULL_KEY) {
                    redirectionIndex.put(leftStampKeys.get(ii), index);
                }
            }
        }

        final QueryTable result = makeResult(leftTable, rightTable, redirectionIndex, columnsToAdd, true);
        final ModifiedColumnSet rightStampColumn = rightTable.newModifiedColumnSet(stampPair.right());
        final ModifiedColumnSet allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        final ModifiedColumnSet.Transformer rightTransformer =
                rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);
        final ChunkEquals stampChunkEquals = ChunkEquals.makeEqual(stampChunkType);
        final CompactKernel stampCompact = CompactKernel.makeCompact(stampChunkType);

        rightTable.listenForUpdates(
                new BaseTable.ShiftAwareListenerImpl(makeListenerDescription(MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                        stampPair, columnsToAdd, reverse, disallowExactMatch), rightTable, result) {
                    @Override
                    public void onUpdate(Update upstream) {
                        final Update downstream = new Update();
                        downstream.added = Index.FACTORY.getEmptyIndex();
                        downstream.removed = Index.FACTORY.getEmptyIndex();
                        downstream.shifted = IndexShiftData.EMPTY;
                        downstream.modifiedColumnSet = result.modifiedColumnSet;

                        final boolean stampModified = upstream.modifiedColumnSet.containsAny(rightStampColumn);

                        final Index.RandomBuilder modifiedBuilder = Index.FACTORY.getRandomBuilder();

                        try (final ColumnSource.FillContext fillContext =
                                rightStampSource.makeFillContext(rightChunkSize);
                                final LongSortKernel<Values, KeyIndices> sortKernel =
                                        LongSortKernel.makeContext(stampChunkType, order, rightChunkSize, true)) {

                            final Index restampRemovals;
                            final Index restampAdditions;
                            if (stampModified) {
                                restampAdditions = upstream.added.union(upstream.modified);
                                restampRemovals = upstream.removed.union(upstream.getModifiedPreShift());
                            } else {
                                restampAdditions = upstream.added;
                                restampRemovals = upstream.removed;
                            }

                            // When removing a row, record the stamp, redirection key, and prior redirection key. Binary
                            // search
                            // in the left for the removed key to find the smallest value geq the removed right. Update
                            // all rows
                            // with the removed redirection to the previous key.
                            try (final OrderedKeys.Iterator removeit = restampRemovals.getOrderedKeysIterator();
                                    final WritableLongChunk<KeyIndices> priorRedirections =
                                            WritableLongChunk.makeWritableChunk(rightChunkSize);
                                    final WritableLongChunk<KeyIndices> rightKeyIndices =
                                            WritableLongChunk.makeWritableChunk(rightChunkSize);
                                    final WritableChunk<Values> rightStampChunk =
                                            stampChunkType.makeWritableChunk(rightChunkSize)) {
                                while (removeit.hasMore()) {
                                    final OrderedKeys chunkOk = removeit.getNextOrderedKeysWithLength(rightChunkSize);
                                    rightStampSource.fillPrevChunk(fillContext, rightStampChunk, chunkOk);
                                    chunkOk.fillKeyIndicesChunk(rightKeyIndices);

                                    sortKernel.sort(rightKeyIndices, rightStampChunk);

                                    ssa.removeAndGetPrior(rightStampChunk, rightKeyIndices, priorRedirections);
                                    chunkSsaStamp.processRemovals(leftStampValues, leftStampKeys, rightStampChunk,
                                            rightKeyIndices, priorRedirections, redirectionIndex, modifiedBuilder,
                                            disallowExactMatch);
                                }
                            }

                            if (upstream.shifted.nonempty()) {
                                rightIncrementalApplySsaShift(upstream.shifted, ssa, sortKernel, fillContext,
                                        restampRemovals, rightTable, rightChunkSize, rightStampSource, chunkSsaStamp,
                                        leftStampValues, leftStampKeys, redirectionIndex, disallowExactMatch);
                            }

                            // When adding a row to the right hand side: we need to know which left hand side might be
                            // responsive. If we are a duplicate stamp and not the last one, we ignore it. Next, we
                            // should binary
                            // search in the left for the first value >=, everything up until the next extant right
                            // value should be
                            // restamped with our value
                            try (final WritableChunk<Values> stampChunk =
                                    stampChunkType.makeWritableChunk(rightChunkSize);
                                    final WritableChunk<Values> nextRightValue =
                                            stampChunkType.makeWritableChunk(rightChunkSize);
                                    final WritableLongChunk<KeyIndices> insertedIndices =
                                            WritableLongChunk.makeWritableChunk(rightChunkSize);
                                    final WritableBooleanChunk<Any> retainStamps =
                                            WritableBooleanChunk.makeWritableChunk(rightChunkSize)) {
                                final int chunks = (restampAdditions.intSize() + control.rightChunkSize() - 1)
                                        / control.rightChunkSize();
                                for (int ii = 0; ii < chunks; ++ii) {
                                    final long startChunk = chunks - ii - 1;
                                    try (final Index chunkOk =
                                            restampAdditions.subindexByPos(startChunk * control.rightChunkSize(),
                                                    (startChunk + 1) * control.rightChunkSize())) {
                                        rightStampSource.fillChunk(fillContext, stampChunk, chunkOk);
                                        insertedIndices.setSize(chunkOk.intSize());
                                        chunkOk.fillKeyIndicesChunk(insertedIndices);

                                        sortKernel.sort(insertedIndices, stampChunk);

                                        final int valuesWithNext =
                                                ssa.insertAndGetNextValue(stampChunk, insertedIndices, nextRightValue);

                                        final boolean endsWithLastValue = valuesWithNext != stampChunk.size();
                                        if (endsWithLastValue) {
                                            Assert.eq(valuesWithNext, "valuesWithNext", stampChunk.size() - 1,
                                                    "stampChunk.size() - 1");
                                            stampChunk.setSize(valuesWithNext);
                                            stampChunkEquals.notEqual(stampChunk, nextRightValue, retainStamps);
                                            stampCompact.compact(nextRightValue, retainStamps);

                                            retainStamps.setSize(chunkOk.intSize());
                                            retainStamps.set(valuesWithNext, true);
                                            stampChunk.setSize(chunkOk.intSize());
                                        } else {
                                            // remove duplicates
                                            stampChunkEquals.notEqual(stampChunk, nextRightValue, retainStamps);
                                            stampCompact.compact(nextRightValue, retainStamps);
                                        }
                                        LongCompactKernel.compact(insertedIndices, retainStamps);
                                        stampCompact.compact(stampChunk, retainStamps);

                                        chunkSsaStamp.processInsertion(leftStampValues, leftStampKeys, stampChunk,
                                                insertedIndices, nextRightValue, redirectionIndex, modifiedBuilder,
                                                endsWithLastValue, disallowExactMatch);
                                    }
                                }
                            }

                            // if the stamp was not modified, then we need to figure out the responsive rows to mark as
                            // modified
                            if (!stampModified && upstream.modified.nonempty()) {
                                try (final OrderedKeys.Iterator modit = upstream.modified.getOrderedKeysIterator();
                                        final WritableLongChunk<KeyIndices> rightStampIndices =
                                                WritableLongChunk.makeWritableChunk(rightChunkSize);
                                        final WritableChunk<Values> rightStampChunk =
                                                stampChunkType.makeWritableChunk(rightChunkSize)) {
                                    while (modit.hasMore()) {
                                        final OrderedKeys chunkOk = modit.getNextOrderedKeysWithLength(rightChunkSize);
                                        rightStampSource.fillChunk(fillContext, rightStampChunk, chunkOk);
                                        chunkOk.fillKeyIndicesChunk(rightStampIndices);

                                        sortKernel.sort(rightStampIndices, rightStampChunk);

                                        chunkSsaStamp.findModified(0, leftStampValues, leftStampKeys, redirectionIndex,
                                                rightStampChunk, rightStampIndices, modifiedBuilder,
                                                disallowExactMatch);
                                    }
                                }
                            }

                            if (stampModified) {
                                restampAdditions.close();
                                restampRemovals.close();
                            }
                        }

                        if (stampModified || upstream.added.nonempty() || upstream.removed.nonempty()) {
                            // If we kept track of whether or not something actually changed, then we could skip
                            // painting all
                            // the right columns as modified. It is not clear whether it is worth the additional
                            // complexity.
                            downstream.modifiedColumnSet.setAll(allRightColumns);
                        } else {
                            rightTransformer.transform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);
                        }

                        downstream.modified = modifiedBuilder.getIndex();

                        result.notifyListeners(downstream);
                    }

                    @Override
                    protected void destroy() {
                        super.destroy();
                        leftStampKeys.close();
                        leftStampValues.close();
                    }
                });

        return result;
    }

    private static void rightIncrementalApplySsaShift(IndexShiftData shiftData, SegmentedSortedArray ssa,
            LongSortKernel<Values, KeyIndices> sortKernel, ChunkSource.FillContext fillContext,
            Index restampRemovals, QueryTable table,
            int chunkSize, ColumnSource<?> stampSource, ChunkSsaStamp chunkSsaStamp,
            WritableChunk<Values> leftStampValues, WritableLongChunk<KeyIndices> leftStampKeys,
            RedirectionIndex redirectionIndex, boolean disallowExactMatch) {

        try (final Index fullPrevIndex = table.getIndex().getPrevIndex();
                final Index previousToShift = fullPrevIndex.minus(restampRemovals);
                final SizedSafeCloseable<ColumnSource.FillContext> shiftFillContext =
                        new SizedSafeCloseable<>(stampSource::makeFillContext);
                final SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> shiftSortKernel =
                        new SizedSafeCloseable<>(sz -> LongSortKernel.makeContext(stampSource.getChunkType(),
                                ssa.isReversed() ? SortingOrder.Descending : SortingOrder.Ascending, sz, true));
                final SizedChunk<Values> rightStampValues = new SizedChunk<>(stampSource.getChunkType());
                final SizedLongChunk<KeyIndices> rightStampKeys = new SizedLongChunk<>()) {

            final IndexShiftData.Iterator sit = shiftData.applyIterator();
            while (sit.hasNext()) {
                sit.next();
                try (final Index indexToShift = previousToShift.subindexByKey(sit.beginRange(), sit.endRange())) {
                    if (indexToShift.empty()) {
                        continue;
                    }

                    if (sit.polarityReversed()) {
                        final int shiftSize = indexToShift.intSize();

                        indexToShift.fillKeyIndicesChunk(rightStampKeys.ensureCapacity(shiftSize));
                        if (chunkSize >= shiftSize) {
                            stampSource.fillPrevChunk(fillContext, rightStampValues.ensureCapacity(shiftSize),
                                    indexToShift);
                            sortKernel.sort(rightStampKeys.get(), rightStampValues.get());
                        } else {
                            stampSource.fillPrevChunk(shiftFillContext.ensureCapacity(shiftSize),
                                    rightStampValues.ensureCapacity(shiftSize), indexToShift);
                            shiftSortKernel.ensureCapacity(shiftSize).sort(rightStampKeys.get(),
                                    rightStampValues.get());
                        }

                        chunkSsaStamp.applyShift(leftStampValues, leftStampKeys, rightStampValues.get(),
                                rightStampKeys.get(), sit.shiftDelta(), redirectionIndex, disallowExactMatch);
                        ssa.applyShiftReverse(rightStampValues.get(), rightStampKeys.get(), sit.shiftDelta());
                    } else {
                        if (indexToShift.size() > chunkSize) {
                            try (final OrderedKeys.Iterator shiftIt = indexToShift.getOrderedKeysIterator()) {
                                while (shiftIt.hasMore()) {
                                    final OrderedKeys chunkOk = shiftIt.getNextOrderedKeysWithLength(chunkSize);
                                    stampSource.fillPrevChunk(fillContext, rightStampValues.ensureCapacity(chunkSize),
                                            chunkOk);

                                    chunkOk.fillKeyIndicesChunk(rightStampKeys.ensureCapacity(chunkSize));

                                    sortKernel.sort(rightStampKeys.get(), rightStampValues.get());

                                    ssa.applyShift(rightStampValues.get(), rightStampKeys.get(), sit.shiftDelta());
                                    chunkSsaStamp.applyShift(leftStampValues, leftStampKeys, rightStampValues.get(),
                                            rightStampKeys.get(), sit.shiftDelta(), redirectionIndex,
                                            disallowExactMatch);
                                }
                            }
                        } else {
                            stampSource.fillPrevChunk(fillContext,
                                    rightStampValues.ensureCapacity(indexToShift.intSize()), indexToShift);
                            indexToShift.fillKeyIndicesChunk(rightStampKeys.ensureCapacity(indexToShift.intSize()));

                            sortKernel.sort(rightStampKeys.get(), rightStampValues.get());

                            ssa.applyShift(rightStampValues.get(), rightStampKeys.get(), sit.shiftDelta());
                            chunkSsaStamp.applyShift(leftStampValues, leftStampKeys, rightStampValues.get(),
                                    rightStampKeys.get(), sit.shiftDelta(), redirectionIndex, disallowExactMatch);
                        }
                    }
                }
            }
        }
    }

    private static Table zeroKeyAjRightStatic(QueryTable leftTable, Table rightTable, MatchPair[] columnsToAdd,
            MatchPair stampPair, ColumnSource<?> leftStampSource, ColumnSource<?> originalRightStampSource,
            ColumnSource<?> rightStampSource, SortingOrder order, boolean disallowExactMatch,
            final RedirectionIndex redirectionIndex) {
        final Index rightIndex = rightTable.getIndex();

        final WritableLongChunk<KeyIndices> rightStampKeys = WritableLongChunk.makeWritableChunk(rightIndex.intSize());
        final WritableChunk<Values> rightStampValues =
                rightStampSource.getChunkType().makeWritableChunk(rightIndex.intSize());

        try (final SafeCloseableList chunksToClose = new SafeCloseableList(rightStampKeys, rightStampValues)) {
            final Supplier<String> keyStringSupplier = () -> "[] (zero key columns)";
            try (final AsOfStampContext stampContext = new AsOfStampContext(order, disallowExactMatch, leftStampSource,
                    rightStampSource, originalRightStampSource)) {
                stampContext.getAndCompactStamps(rightIndex, rightStampKeys, rightStampValues);
                stampContext.processEntry(leftTable.getIndex(), rightStampValues, rightStampKeys, redirectionIndex);
            }
            final QueryTable result =
                    makeResult(leftTable, rightTable, redirectionIndex, columnsToAdd, leftTable.isRefreshing());
            if (!leftTable.isRefreshing()) {
                return result;
            }

            final ModifiedColumnSet leftStampColumn = leftTable.newModifiedColumnSet(stampPair.left());
            final ModifiedColumnSet allRightColumns =
                    result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
            final ModifiedColumnSet.Transformer leftTransformer =
                    leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());

            final WritableLongChunk<KeyIndices> compactedRightStampKeys;
            final WritableChunk<Values> compactedRightStampValues;
            if (rightStampKeys.size() < rightIndex.size()) {
                compactedRightStampKeys = WritableLongChunk.makeWritableChunk(rightStampKeys.size());
                compactedRightStampValues = rightStampSource.getChunkType().makeWritableChunk(rightStampKeys.size());

                rightStampKeys.copyToChunk(0, compactedRightStampKeys, 0, rightStampKeys.size());
                rightStampValues.copyToChunk(0, compactedRightStampValues, 0, rightStampKeys.size());
            } else {
                chunksToClose.clear();
                compactedRightStampKeys = rightStampKeys;
                compactedRightStampValues = rightStampValues;
            }

            leftTable
                    .listenForUpdates(
                            new BaseTable.ShiftAwareListenerImpl(
                                    makeListenerDescription(MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY, stampPair,
                                            columnsToAdd, order == SortingOrder.Descending, disallowExactMatch),
                                    leftTable, result) {
                                @Override
                                public void onUpdate(Update upstream) {
                                    final Update downstream = upstream.copy();

                                    upstream.removed.forAllLongs(redirectionIndex::removeVoid);

                                    final boolean stampModified = upstream.modified.nonempty()
                                            && upstream.modifiedColumnSet.containsAny(leftStampColumn);

                                    final Index restampKeys;
                                    if (stampModified) {
                                        upstream.getModifiedPreShift().forAllLongs(redirectionIndex::removeVoid);
                                        restampKeys = upstream.modified.union(upstream.added);
                                    } else {
                                        restampKeys = upstream.added;
                                    }

                                    try (final Index prevLeftIndex = leftTable.getIndex().getPrevIndex()) {
                                        redirectionIndex.applyShift(prevLeftIndex, upstream.shifted);
                                    }

                                    try (final AsOfStampContext stampContext =
                                            new AsOfStampContext(order, disallowExactMatch, leftStampSource,
                                                    rightStampSource, originalRightStampSource)) {
                                        stampContext.processEntry(restampKeys, compactedRightStampValues,
                                                compactedRightStampKeys, redirectionIndex);
                                    }

                                    downstream.modifiedColumnSet = result.modifiedColumnSet;
                                    leftTransformer.clearAndTransform(upstream.modifiedColumnSet,
                                            downstream.modifiedColumnSet);
                                    if (stampModified) {
                                        downstream.modifiedColumnSet.setAll(allRightColumns);
                                    }

                                    result.notifyListeners(downstream);

                                    if (stampModified) {
                                        restampKeys.close();
                                    }
                                }

                                @Override
                                protected void destroy() {
                                    super.destroy();
                                    compactedRightStampKeys.close();
                                    compactedRightStampValues.close();
                                }
                            });

            return result;
        }
    }


    private static QueryTable makeResult(QueryTable leftTable, Table rightTable, RedirectionIndex redirectionIndex,
            MatchPair[] columnsToAdd, boolean refreshing) {
        final Map<String, ColumnSource<?>> columnSources = new LinkedHashMap<>(leftTable.getColumnSourceMap());
        Arrays.stream(columnsToAdd).forEach(mp -> {
            final ReadOnlyRedirectedColumnSource<?> rightSource =
                    new ReadOnlyRedirectedColumnSource<>(redirectionIndex, rightTable.getColumnSource(mp.right()));
            if (refreshing) {
                rightSource.startTrackingPrevValues();
            }
            columnSources.put(mp.left(), rightSource);
        });
        if (refreshing) {
            redirectionIndex.startTrackingPrevValues();
        }
        return new QueryTable(leftTable.getIndex(), columnSources);
    }
}
