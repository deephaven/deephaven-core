/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedBooleanChunk;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalHashedAsOfJoinStateManager;
import io.deephaven.engine.table.impl.asofjoin.StaticAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.asofjoin.StaticHashedAsOfJoinStateManager;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.asofjoin.BucketedChunkedAjMergedListener;
import io.deephaven.engine.table.impl.join.JoinListenerRecorder;
import io.deephaven.engine.table.impl.asofjoin.ZeroKeyChunkedAjMergedListener;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.ssa.ChunkSsaStamp;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.ssa.SsaSsaStamp;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.SingleValueRowRedirection;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.engine.table.impl.util.compact.LongCompactKernel;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class AsOfJoinHelper {

    private AsOfJoinHelper() {} // static use only

    static Table asOfJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, SortingOrder order, boolean disallowExactMatch) {
        final JoinControl joinControl = new JoinControl();
        return asOfJoin(joinControl, leftTable, rightTable, columnsToMatch, columnsToAdd, order, disallowExactMatch);
    }

    static Table asOfJoin(JoinControl control, QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd, SortingOrder order, boolean disallowExactMatch) {
        QueryTable.checkInitiateBinaryOperation(leftTable, rightTable);

        if (columnsToMatch.length == 0) {
            throw new IllegalArgumentException("aj() requires at least one column to match!");
        }

        checkColumnConflicts(leftTable, columnsToAdd);

        if (!leftTable.isRefreshing() && leftTable.size() == 0) {
            return makeResult(leftTable, rightTable, new SingleValueRowRedirection(RowSequence.NULL_ROW_KEY),
                    columnsToAdd, false);
        }

        final MatchPair stampPair = columnsToMatch[columnsToMatch.length - 1];

        final int keyColumnCount = columnsToMatch.length - 1;

        final ColumnSource<?>[] originalLeftSources = Arrays.stream(columnsToMatch).limit(keyColumnCount)
                .map(mp -> leftTable.getColumnSource(mp.leftColumn)).toArray(ColumnSource[]::new);
        final ColumnSource<?>[] leftSources = new ColumnSource[originalLeftSources.length];
        for (int ii = 0; ii < leftSources.length; ++ii) {
            leftSources[ii] = ReinterpretUtils.maybeConvertToPrimitive(originalLeftSources[ii]);
        }
        final ColumnSource<?>[] originalRightSources = Arrays.stream(columnsToMatch).limit(keyColumnCount)
                .map(mp -> rightTable.getColumnSource(mp.rightColumn)).toArray(ColumnSource[]::new);
        final ColumnSource<?>[] rightSources = new ColumnSource[originalLeftSources.length];
        for (int ii = 0; ii < leftSources.length; ++ii) {
            rightSources[ii] = ReinterpretUtils.maybeConvertToPrimitive(originalRightSources[ii]);
        }

        final ColumnSource<?> leftStampSource =
                ReinterpretUtils.maybeConvertToPrimitive(leftTable.getColumnSource(stampPair.leftColumn()));
        final ColumnSource<?> originalRightStampSource = rightTable.getColumnSource(stampPair.rightColumn());
        final ColumnSource<?> rightStampSource = ReinterpretUtils.maybeConvertToPrimitive(originalRightStampSource);

        if (leftStampSource.getType() != rightStampSource.getType()) {
            throw new IllegalArgumentException("Can not aj() with different stamp types: left="
                    + leftStampSource.getType() + ", right=" + rightStampSource.getType());
        }

        final WritableRowRedirection rowRedirection = JoinRowRedirection.makeRowRedirection(control, leftTable);
        if (keyColumnCount == 0) {
            return zeroKeyAj(control, leftTable, rightTable, columnsToAdd, stampPair, leftStampSource,
                    originalRightStampSource, rightStampSource, order, disallowExactMatch, rowRedirection);
        }

        if (rightTable.isRefreshing()) {
            if (leftTable.isRefreshing()) {
                return bothIncrementalAj(control, leftTable, rightTable, columnsToMatch, columnsToAdd, order,
                        disallowExactMatch, stampPair,
                        originalLeftSources, leftSources, rightSources, leftStampSource, originalRightStampSource,
                        rightStampSource, rowRedirection);
            }
            return rightTickingLeftStaticAj(control, leftTable, rightTable, columnsToMatch, columnsToAdd, order,
                    disallowExactMatch, stampPair, originalLeftSources, leftSources, rightSources, leftStampSource,
                    originalRightStampSource, rightStampSource,
                    rowRedirection);
        } else {
            return rightStaticAj(control, leftTable, rightTable, columnsToMatch, columnsToAdd, order,
                    disallowExactMatch, stampPair, originalLeftSources, leftSources, rightSources, leftStampSource,
                    originalRightStampSource, rightStampSource, rowRedirection);
        }
    }

    @NotNull
    private static Table rightStaticAj(JoinControl control,
            QueryTable leftTable,
            QueryTable rightTable,
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
            WritableRowRedirection rowRedirection) {

        final IntegerArraySource slots = new IntegerArraySource();
        final int slotCount;

        final boolean buildLeft;
        final int size;

        final Map<?, RowSet> leftGrouping;
        final Map<?, RowSet> rightGrouping;

        if (control.useGrouping(leftTable, leftSources)) {
            leftGrouping = leftSources[0].getGroupToRange(leftTable.getRowSet());
            final int leftSize = leftGrouping.size();

            if (control.useGrouping(rightTable, rightSources)) {
                rightGrouping = rightSources[0].getGroupToRange(rightTable.getRowSet());
                final int rightSize = rightGrouping.size();
                buildLeft = leftSize < rightSize;
                size = buildLeft ? control.tableSize(leftSize) : control.tableSize(rightSize);
            } else {
                buildLeft = true;
                size = control.tableSize(leftSize);
                rightGrouping = null;
            }
        } else if (control.useGrouping(rightTable, rightSources)) {
            rightGrouping = rightSources[0].getGroupToRange(rightTable.getRowSet());
            leftGrouping = null;

            final int rightSize = rightGrouping.size();
            buildLeft = !leftTable.isRefreshing() && leftTable.size() < rightSize;
            size = control.tableSize(Math.min(leftTable.size(), rightSize));
        } else {
            buildLeft = !leftTable.isRefreshing() && control.buildLeft(leftTable, rightTable);
            size = control.initialBuildSize();
            leftGrouping = rightGrouping = null;
        }

        final StaticHashedAsOfJoinStateManager asOfJoinStateManager;
        if (buildLeft) {
            asOfJoinStateManager =
                    TypedHasherFactory.make(StaticAsOfJoinStateManagerTypedBase.class,
                            leftSources, originalLeftSources, size,
                            control.getMaximumLoadFactor(), control.getTargetLoadFactor());
        } else {
            asOfJoinStateManager =
                    TypedHasherFactory.make(StaticAsOfJoinStateManagerTypedBase.class,
                            leftSources, originalLeftSources, size,
                            control.getMaximumLoadFactor(), control.getTargetLoadFactor());
        }


        final Pair<ArrayBackedColumnSource<?>, ObjectArraySource<RowSet>> leftGroupedSources;
        final int leftGroupingSize;
        if (leftGrouping != null) {
            final MutableInt groupSize = new MutableInt();
            // noinspection unchecked,rawtypes
            leftGroupedSources = GroupingUtils.groupingToFlatSources((ColumnSource) leftSources[0], leftGrouping,
                    leftTable.getRowSet(), groupSize);
            leftGroupingSize = groupSize.intValue();
        } else {
            leftGroupedSources = null;
            leftGroupingSize = 0;
        }

        final Pair<ArrayBackedColumnSource<?>, ObjectArraySource<RowSet>> rightGroupedSources;
        final int rightGroupingSize;
        if (rightGrouping != null) {
            final MutableInt groupSize = new MutableInt();
            // noinspection unchecked,rawtypes
            rightGroupedSources = GroupingUtils.groupingToFlatSources((ColumnSource) rightSources[0],
                    rightGrouping, rightTable.getRowSet(), groupSize);
            rightGroupingSize = groupSize.intValue();
        } else {
            rightGroupedSources = null;
            rightGroupingSize = 0;
        }

        if (buildLeft) {
            if (leftGroupedSources == null) {
                slotCount = asOfJoinStateManager.buildFromLeftSide(leftTable.getRowSet(), leftSources, slots);
            } else {
                slotCount = asOfJoinStateManager.buildFromLeftSide(RowSetFactory.flat(leftGroupingSize),
                        new ColumnSource[] {leftGroupedSources.getFirst()}, slots);
            }
            if (rightGroupedSources == null) {
                asOfJoinStateManager.probeRight(rightTable.getRowSet(), rightSources);
            } else {
                asOfJoinStateManager.probeRight(RowSetFactory.flat(rightGroupingSize),
                        new ColumnSource[] {rightGroupedSources.getFirst()});
            }
        } else {
            if (rightGroupedSources == null) {
                slotCount = asOfJoinStateManager.buildFromRightSide(rightTable.getRowSet(), rightSources, slots);
            } else {
                slotCount =
                        asOfJoinStateManager.buildFromRightSide(RowSetFactory.flat(rightGroupingSize),
                                new ColumnSource[] {rightGroupedSources.getFirst()}, slots);
            }
            if (leftGroupedSources == null) {
                asOfJoinStateManager.probeLeft(leftTable.getRowSet(), leftSources);
            } else {
                asOfJoinStateManager.probeLeft(RowSetFactory.flat(leftGroupingSize),
                        new ColumnSource[] {leftGroupedSources.getFirst()});
            }
        }

        final ArrayValuesCache arrayValuesCache;
        if (leftTable.isRefreshing()) {
            if (rightGroupedSources != null) {
                asOfJoinStateManager.convertRightGrouping(slots, slotCount, rightGroupedSources.getSecond());
            } else {
                asOfJoinStateManager.convertRightBuildersToIndex(slots, slotCount);
            }
            arrayValuesCache = new ArrayValuesCache(asOfJoinStateManager.getTableSize());
        } else {
            arrayValuesCache = null;
            if (rightGroupedSources != null) {
                asOfJoinStateManager.convertRightGrouping(slots, slotCount, rightGroupedSources.getSecond());
            } else {
                asOfJoinStateManager.convertRightBuildersToIndex(slots, slotCount);
            }
        }

        try (final AsOfStampContext stampContext = new AsOfStampContext(order, disallowExactMatch, leftStampSource,
                rightStampSource, originalRightStampSource);
                final ResettableWritableLongChunk<RowKeys> keyChunk =
                        ResettableWritableLongChunk.makeResettableChunk();
                final ResettableWritableChunk<Values> valuesChunk =
                        rightStampSource.getChunkType().makeResettableWritableChunk()) {
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                final int slot = slots.getInt(slotIndex);
                RowSet leftRowSet = asOfJoinStateManager.getLeftIndex(slot);
                if (leftRowSet == null || leftRowSet.isEmpty()) {
                    continue;
                }

                final RowSet rightRowSet = asOfJoinStateManager.getRightIndex(slot);
                if (rightRowSet == null || rightRowSet.isEmpty()) {
                    continue;
                }

                if (leftGroupedSources != null) {
                    if (leftRowSet.size() != 1) {
                        throw new IllegalStateException("Groupings should have exactly one row key!");
                    }
                    leftRowSet = leftGroupedSources.getSecond().get(leftRowSet.get(0));
                }

                if (arrayValuesCache != null) {
                    processLeftSlotWithRightCache(stampContext, leftRowSet, rightRowSet, rowRedirection,
                            rightStampSource, keyChunk, valuesChunk, arrayValuesCache, slot);
                } else {
                    stampContext.processEntry(leftRowSet, rightRowSet, rowRedirection);
                }
            }
        }

        final QueryTable result =
                makeResult(leftTable, rightTable, rowRedirection, columnsToAdd, leftTable.isRefreshing());
        if (!leftTable.isRefreshing()) {
            return result;
        }

        final ModifiedColumnSet leftKeysOrStamps =
                leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));
        final IntegerArraySource updatedSlots = new IntegerArraySource();
        final ModifiedColumnSet allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        final ModifiedColumnSet.Transformer leftTransformer =
                leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());

        leftTable.addUpdateListener(new BaseTable.ListenerImpl(makeListenerDescription(columnsToMatch,
                stampPair, columnsToAdd, order == SortingOrder.Descending, disallowExactMatch), leftTable, result) {
            @Override
            public void onUpdate(TableUpdate upstream) {
                final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);

                rowRedirection.removeAll(upstream.removed());

                final boolean keysModified = upstream.modifiedColumnSet().containsAny(leftKeysOrStamps);

                final RowSet restampKeys;
                if (keysModified) {
                    rowRedirection.removeAll(upstream.getModifiedPreShift());
                    restampKeys = upstream.modified().union(upstream.added());
                } else {
                    restampKeys = upstream.added();
                }

                try (final RowSet prevLeftRowSet = leftTable.getRowSet().copyPrev()) {
                    rowRedirection.applyShift(prevLeftRowSet, upstream.shifted());
                }

                if (restampKeys.isNonempty()) {
                    final RowSetBuilderRandom foundBuilder = RowSetFactory.builderRandom();
                    updatedSlots.ensureCapacity(restampKeys.size());
                    final int slotCount =
                            asOfJoinStateManager.probeLeft(restampKeys, leftSources, updatedSlots, foundBuilder);

                    try (final RowSet foundKeys = foundBuilder.build();
                            final RowSet notFound = restampKeys.minus(foundKeys)) {
                        rowRedirection.removeAll(notFound);
                    }

                    try (final AsOfStampContext stampContext = new AsOfStampContext(order, disallowExactMatch,
                            leftStampSource, rightStampSource, originalRightStampSource);
                            final ResettableWritableLongChunk<RowKeys> keyChunk =
                                    ResettableWritableLongChunk.makeResettableChunk();
                            final ResettableWritableChunk<Values> valuesChunk =
                                    rightStampSource.getChunkType().makeResettableWritableChunk()) {
                        for (int ii = 0; ii < slotCount; ++ii) {
                            final int slot = updatedSlots.getInt(ii);

                            final RowSet leftRowSet = asOfJoinStateManager.getLeftIndex(slot);
                            final RowSet rightRowSet = asOfJoinStateManager.getRightIndex(slot);
                            assert arrayValuesCache != null;
                            processLeftSlotWithRightCache(stampContext, leftRowSet, rightRowSet, rowRedirection,
                                    rightStampSource, keyChunk, valuesChunk, arrayValuesCache, slot);
                        }
                    }
                }

                downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                leftTransformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet());
                if (keysModified) {
                    downstream.modifiedColumnSet().setAll(allRightColumns);
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

        private ArrayValuesCache(int size) {
            cacheStampKeys = new ObjectArraySource<>(long[].class);
            cacheStampValues = new ObjectArraySource<>(Object.class);

            cacheStampKeys.ensureCapacity(size);
            cacheStampValues.ensureCapacity(size);
        }

        long[] getKeys(long slot) {
            return cacheStampKeys.get(slot);
        }

        Object getValues(long slot) {
            return cacheStampValues.get(slot);
        }

        void setKeysAndValues(long slot, long[] keyIndices, Object StampArray) {
            cacheStampKeys.set(slot, keyIndices);
            cacheStampValues.set(slot, StampArray);
        }
    }

    private static void processLeftSlotWithRightCache(AsOfStampContext stampContext,
            RowSet leftRowSet, RowSet rightRowSet, WritableRowRedirection rowRedirection,
            ColumnSource<?> rightStampSource,
            ResettableWritableLongChunk<RowKeys> keyChunk, ResettableWritableChunk<Values> valuesChunk,
            ArrayValuesCache arrayValuesCache,
            long slot) {
        final long[] rightStampKeys = arrayValuesCache.getKeys(slot);
        if (rightStampKeys == null) {
            final int rightSize = rightRowSet.intSize();
            long[] keyIndices = new long[rightSize];

            Object rightStampArray = rightStampSource.getChunkType().makeArray(rightSize);

            keyChunk.resetFromTypedArray(keyIndices, 0, rightSize);
            valuesChunk.resetFromArray(rightStampArray, 0, rightSize);

            // noinspection unchecked
            stampContext.getAndCompactStamps(rightRowSet, keyChunk, valuesChunk);

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
        stampContext.processEntry(leftRowSet, valuesChunk, keyChunk, rowRedirection);
    }

    /**
     * If the asOfJoinStateManager is null, it means we are passing in the leftRowSet. If the leftRowSet is null; we are
     * passing in the asOfJoinStateManager and should obtain the leftRowSet from the state manager if necessary.
     */
    private static void getCachedLeftStampsAndKeys(RightIncrementalHashedAsOfJoinStateManager asOfJoinStateManager,
            RowSet leftRowSet,
            ColumnSource<?> leftStampSource,
            SizedSafeCloseable<ChunkSource.FillContext> fillContext,
            SizedSafeCloseable<LongSortKernel<Values, RowKeys>> sortContext,
            ResettableWritableLongChunk<RowKeys> keyChunk, ResettableWritableChunk<Values> valuesChunk,
            ArrayValuesCache arrayValuesCache,
            int slot) {
        final long[] leftStampKeys = arrayValuesCache.getKeys(slot);
        if (leftStampKeys == null) {
            if (leftRowSet == null) {
                leftRowSet = asOfJoinStateManager.getAndClearLeftIndex(slot);
                if (leftRowSet == null) {
                    leftRowSet = RowSetFactory.empty();
                }
            }
            final int leftSize = leftRowSet.intSize();
            final long[] keyIndices = new long[leftSize];

            final Object leftStampArray = leftStampSource.getChunkType().makeArray(leftSize);

            keyChunk.resetFromTypedArray(keyIndices, 0, leftSize);
            valuesChunk.resetFromArray(leftStampArray, 0, leftSize);

            // noinspection unchecked
            leftRowSet.fillRowKeyChunk(keyChunk);

            // noinspection unchecked
            leftStampSource.fillChunk(fillContext.ensureCapacity(leftSize), valuesChunk, leftRowSet);

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
            boolean disallowExactMatch, final WritableRowRedirection rowRedirection) {
        if (rightTable.isRefreshing() && leftTable.isRefreshing()) {
            return zeroKeyAjBothIncremental(control, leftTable, rightTable, columnsToAdd, stampPair, leftStampSource,
                    rightStampSource, order, disallowExactMatch, rowRedirection);
        } else if (rightTable.isRefreshing()) {
            return zeroKeyAjRightIncremental(control, leftTable, rightTable, columnsToAdd, stampPair, leftStampSource,
                    rightStampSource, order, disallowExactMatch, rowRedirection);
        } else {
            return zeroKeyAjRightStatic(leftTable, rightTable, columnsToAdd, stampPair, leftStampSource,
                    originalRightStampSource, rightStampSource, order, disallowExactMatch, rowRedirection);
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
            ColumnSource<?>[] originalLeftSources,
            ColumnSource<?>[] leftSources,
            ColumnSource<?>[] rightSources,
            ColumnSource<?> leftStampSource,
            ColumnSource<?> originalRightStampSource,
            ColumnSource<?> rightStampSource,
            WritableRowRedirection rowRedirection) {
        if (leftTable.isRefreshing()) {
            throw new IllegalStateException();
        }

        final boolean reverse = order == SortingOrder.Descending;

        final ChunkType stampChunkType = rightStampSource.getChunkType();
        final Supplier<SegmentedSortedArray> ssaFactory =
                SegmentedSortedArray.makeFactory(stampChunkType, reverse, control.rightSsaNodeSize());
        final ChunkSsaStamp chunkSsaStamp = ChunkSsaStamp.make(stampChunkType, reverse);

        final int tableSize = control.initialBuildSize();

        final RightIncrementalHashedAsOfJoinStateManager asOfJoinStateManager =
                TypedHasherFactory.make(RightIncrementalAsOfJoinStateManagerTypedBase.class,
                        leftSources, originalLeftSources, tableSize,
                        control.getMaximumLoadFactor(), control.getTargetLoadFactor());

        final IntegerArraySource slots = new IntegerArraySource();
        final int slotCount = asOfJoinStateManager.buildFromLeftSide(leftTable.getRowSet(), leftSources, slots);
        asOfJoinStateManager.probeRightInitial(rightTable.getRowSet(), rightSources);

        final ArrayValuesCache leftValuesCache = new ArrayValuesCache(asOfJoinStateManager.getTableSize());
        final SizedSafeCloseable<LongSortKernel<Values, RowKeys>> sortContext =
                new SizedSafeCloseable<>(size -> LongSortKernel.makeContext(stampChunkType, order, size, true));
        final SizedSafeCloseable<ChunkSource.FillContext> leftStampFillContext =
                new SizedSafeCloseable<>(leftStampSource::makeFillContext);
        final SizedSafeCloseable<ChunkSource.FillContext> rightStampFillContext =
                new SizedSafeCloseable<>(rightStampSource::makeFillContext);
        final SizedChunk<Values> rightValues = new SizedChunk<>(stampChunkType);
        final SizedLongChunk<RowKeys> rightKeyIndices = new SizedLongChunk<>();
        final SizedLongChunk<RowKeys> rightKeysForLeft = new SizedLongChunk<>();

        // if we have an error the closeableList cleans up for us; if not they can be used later
        try (final ResettableWritableLongChunk<RowKeys> leftKeyChunk =
                ResettableWritableLongChunk.makeResettableChunk();
                final ResettableWritableChunk<Values> leftValuesChunk =
                        rightStampSource.getChunkType().makeResettableWritableChunk()) {
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                final int slot = slots.getInt(slotIndex);
                final RowSet leftRowSet = asOfJoinStateManager.getAndClearLeftIndex(slot);
                assert leftRowSet != null;
                assert leftRowSet.size() > 0;

                final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot, (rightIndex) -> {
                    final SegmentedSortedArray ssa = ssaFactory.get();
                    final int slotSize = rightIndex.intSize();
                    if (slotSize > 0) {
                        rightStampSource.fillChunk(rightStampFillContext.ensureCapacity(slotSize),
                                rightValues.ensureCapacity(slotSize), rightIndex);
                        rightIndex.fillRowKeyChunk(rightKeyIndices.ensureCapacity(slotSize));
                        sortContext.ensureCapacity(slotSize).sort(rightKeyIndices.get(), rightValues.get());
                        ssa.insert(rightValues.get(), rightKeyIndices.get());
                    }
                    return ssa;
                });

                getCachedLeftStampsAndKeys(null, leftRowSet, leftStampSource, leftStampFillContext, sortContext,
                        leftKeyChunk, leftValuesChunk, leftValuesCache, slot);

                if (rightSsa.size() == 0) {
                    continue;
                }

                final WritableLongChunk<RowKeys> rightKeysForLeftChunk =
                        rightKeysForLeft.ensureCapacity(leftRowSet.intSize());

                // noinspection unchecked
                chunkSsaStamp.processEntry(leftValuesChunk, leftKeyChunk, rightSsa, rightKeysForLeftChunk,
                        disallowExactMatch);

                for (int ii = 0; ii < leftKeyChunk.size(); ++ii) {
                    final long index = rightKeysForLeftChunk.get(ii);
                    if (index != RowSequence.NULL_ROW_KEY) {
                        rowRedirection.put(leftKeyChunk.get(ii), index);
                    }
                }
            }
        }

        // we will close them now, but the listener is able to resurrect them as needed
        SafeCloseable.closeAll(sortContext, leftStampFillContext, rightStampFillContext, rightValues, rightKeyIndices,
                rightKeysForLeft);

        final QueryTable result = makeResult(leftTable, rightTable, rowRedirection, columnsToAdd, true);

        final ModifiedColumnSet rightMatchColumns =
                rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
        final ModifiedColumnSet rightStampColumn = rightTable.newModifiedColumnSet(stampPair.rightColumn());
        final ModifiedColumnSet rightAddedColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        final ModifiedColumnSet.Transformer rightTransformer =
                rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);

        final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders =
                new ObjectArraySource<>(RowSetBuilderSequential.class);

        rightTable.addUpdateListener(new BaseTable.ListenerImpl(
                makeListenerDescription(columnsToMatch, stampPair, columnsToAdd, reverse, disallowExactMatch),
                rightTable, result) {
            @Override
            public void onUpdate(TableUpdate upstream) {
                final TableUpdateImpl downstream = new TableUpdateImpl();
                downstream.added = RowSetFactory.empty();
                downstream.removed = RowSetFactory.empty();
                downstream.shifted = RowSetShiftData.EMPTY;
                downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();

                final boolean keysModified = upstream.modifiedColumnSet().containsAny(rightMatchColumns);
                final boolean stampModified = upstream.modifiedColumnSet().containsAny(rightStampColumn);

                final RowSetBuilderRandom modifiedBuilder = RowSetFactory.builderRandom();

                final RowSet restampRemovals;
                final RowSet restampAdditions;
                if (keysModified || stampModified) {
                    restampAdditions = upstream.added().union(upstream.modified());
                    restampRemovals = upstream.removed().union(upstream.getModifiedPreShift());
                } else {
                    restampAdditions = upstream.added();
                    restampRemovals = upstream.removed();
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


                try (final ResettableWritableLongChunk<RowKeys> leftKeyChunk =
                        ResettableWritableLongChunk.makeResettableChunk();
                        final ResettableWritableChunk<Values> leftValuesChunk =
                                rightStampSource.getChunkType().makeResettableWritableChunk();
                        final SizedLongChunk<RowKeys> priorRedirections = new SizedLongChunk<>()) {
                    for (int slotIndex = 0; slotIndex < removedSlotCount; ++slotIndex) {
                        final int slot = slots.getInt(slotIndex);

                        final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot);

                        final RowSet rightRemoved = sequentialBuilders.get(slotIndex).build();
                        sequentialBuilders.set(slotIndex, null);
                        final int slotSize = rightRemoved.intSize();


                        rightStampSource.fillPrevChunk(rightStampFillContext.ensureCapacity(slotSize),
                                rightValues.ensureCapacity(slotSize), rightRemoved);
                        rightRemoved.fillRowKeyChunk(rightKeyIndices.ensureCapacity(slotSize));
                        sortContext.ensureCapacity(slotSize).sort(rightKeyIndices.get(), rightValues.get());

                        getCachedLeftStampsAndKeys(asOfJoinStateManager, null, leftStampSource, leftStampFillContext,
                                sortContext, leftKeyChunk, leftValuesChunk, leftValuesCache, slot);

                        priorRedirections.ensureCapacity(slotSize).setSize(slotSize);

                        rightSsa.removeAndGetPrior(rightValues.get(), rightKeyIndices.get(), priorRedirections.get());

                        // noinspection unchecked
                        chunkSsaStamp.processRemovals(leftValuesChunk, leftKeyChunk, rightValues.get(),
                                rightKeyIndices.get(), priorRedirections.get(), rowRedirection, modifiedBuilder,
                                disallowExactMatch);

                        rightRemoved.close();
                    }
                }

                // After all the removals are done, we do the shifts
                if (upstream.shifted().nonempty()) {
                    try (final RowSet fullPrevRowSet = rightTable.getRowSet().copyPrev();
                            final RowSet previousToShift = fullPrevRowSet.minus(restampRemovals)) {
                        if (previousToShift.isNonempty()) {
                            try (final ResettableWritableLongChunk<RowKeys> leftKeyChunk =
                                    ResettableWritableLongChunk.makeResettableChunk();
                                    final ResettableWritableChunk<Values> leftValuesChunk =
                                            rightStampSource.getChunkType().makeResettableWritableChunk()) {
                                final RowSetShiftData.Iterator sit = upstream.shifted().applyIterator();
                                while (sit.hasNext()) {
                                    sit.next();
                                    final RowSet rowSetToShift =
                                            previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange());
                                    if (rowSetToShift.isEmpty()) {
                                        rowSetToShift.close();
                                        continue;
                                    }

                                    final int shiftedSlots = asOfJoinStateManager.gatherShiftIndex(rowSetToShift,
                                            rightSources, slots, sequentialBuilders);
                                    rowSetToShift.close();

                                    for (int slotIndex = 0; slotIndex < shiftedSlots; ++slotIndex) {
                                        final int slot = slots.getInt(slotIndex);
                                        try (final RowSet slotShiftRowSet =
                                                sequentialBuilders.get(slotIndex).build()) {
                                            sequentialBuilders.set(slotIndex, null);

                                            final int shiftSize = slotShiftRowSet.intSize();

                                            getCachedLeftStampsAndKeys(asOfJoinStateManager, null, leftStampSource,
                                                    leftStampFillContext, sortContext, leftKeyChunk, leftValuesChunk,
                                                    leftValuesCache, slot);

                                            rightStampSource.fillPrevChunk(
                                                    rightStampFillContext.ensureCapacity(shiftSize),
                                                    rightValues.ensureCapacity(shiftSize), slotShiftRowSet);

                                            final SegmentedSortedArray rightSsa =
                                                    asOfJoinStateManager.getRightSsa(slot);

                                            slotShiftRowSet
                                                    .fillRowKeyChunk(rightKeyIndices.ensureCapacity(shiftSize));
                                            sortContext.ensureCapacity(shiftSize).sort(rightKeyIndices.get(),
                                                    rightValues.get());

                                            if (sit.polarityReversed()) {
                                                // noinspection unchecked
                                                chunkSsaStamp.applyShift(leftValuesChunk, leftKeyChunk,
                                                        rightValues.get(), rightKeyIndices.get(), sit.shiftDelta(),
                                                        rowRedirection, disallowExactMatch);
                                                rightSsa.applyShiftReverse(rightValues.get(), rightKeyIndices.get(),
                                                        sit.shiftDelta());
                                            } else {
                                                // noinspection unchecked
                                                chunkSsaStamp.applyShift(leftValuesChunk, leftKeyChunk,
                                                        rightValues.get(), rightKeyIndices.get(), sit.shiftDelta(),
                                                        rowRedirection, disallowExactMatch);
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
                        final SizedLongChunk<RowKeys> insertedIndices = new SizedLongChunk<>();
                        final SizedBooleanChunk<Any> retainStamps = new SizedBooleanChunk<>();
                        final SizedSafeCloseable<ColumnSource.FillContext> rightStampFillContext =
                                new SizedSafeCloseable<>(rightStampSource::makeFillContext);
                        final ResettableWritableLongChunk<RowKeys> leftKeyChunk =
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
                        final int slot = slots.getInt(slotIndex);

                        final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot);

                        final RowSet rightAdded = sequentialBuilders.get(slotIndex).build();
                        sequentialBuilders.set(slotIndex, null);

                        final int rightSize = rightAdded.intSize();

                        rightStampSource.fillChunk(rightStampFillContext.ensureCapacity(rightSize),
                                rightStampChunk.ensureCapacity(rightSize), rightAdded);
                        rightAdded.fillRowKeyChunk(insertedIndices.ensureCapacity(rightSize));
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
                                insertedIndices.get(), nextRightValue.get(), rowRedirection, modifiedBuilder,
                                endsWithLastValue, disallowExactMatch);
                    }

                    // and then finally we handle the case where the keys and stamps were not modified, but we must
                    // identify
                    // the responsive modifications.
                    if (!keysModified && !stampModified && upstream.modified().isNonempty()) {
                        // next we do the additions
                        final int modifiedSlotCount = asOfJoinStateManager.gatherModifications(upstream.modified(),
                                rightSources, slots, sequentialBuilders);

                        for (int slotIndex = 0; slotIndex < modifiedSlotCount; ++slotIndex) {
                            final int slot = slots.getInt(slotIndex);

                            try (final RowSet rightModified = sequentialBuilders.get(slotIndex).build()) {
                                sequentialBuilders.set(slotIndex, null);
                                final int rightSize = rightModified.intSize();

                                rightStampSource.fillChunk(rightStampFillContext.ensureCapacity(rightSize),
                                        rightValues.ensureCapacity(rightSize), rightModified);
                                rightModified.fillRowKeyChunk(rightKeyIndices.ensureCapacity(rightSize));
                                sortContext.ensureCapacity(rightSize).sort(rightKeyIndices.get(), rightValues.get());

                                getCachedLeftStampsAndKeys(asOfJoinStateManager, null, leftStampSource,
                                        leftStampFillContext, sortContext, leftKeyChunk, leftValuesChunk,
                                        leftValuesCache, slot);

                                // noinspection unchecked
                                chunkSsaStamp.findModified(0, leftValuesChunk, leftKeyChunk, rowRedirection,
                                        rightValues.get(), rightKeyIndices.get(), modifiedBuilder, disallowExactMatch);
                            }
                        }

                        rightTransformer.transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet());
                    }
                }

                SafeCloseable.closeAll(sortContext, leftStampFillContext, rightStampFillContext, rightValues,
                        rightKeyIndices, rightKeysForLeft);

                downstream.modified = modifiedBuilder.build();

                final boolean processedAdditionsOrRemovals = removedSlotCount > 0 || addedSlotCount > 0;
                if (keysModified || stampModified || processedAdditionsOrRemovals) {
                    downstream.modifiedColumnSet().setAll(rightAddedColumns);
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

    public interface SsaFactory extends Function<RowSet, SegmentedSortedArray>, SafeCloseable {
    }

    private static Table bothIncrementalAj(JoinControl control,
            QueryTable leftTable,
            QueryTable rightTable,
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
            WritableRowRedirection rowRedirection) {
        final boolean reverse = order == SortingOrder.Descending;

        final ChunkType stampChunkType = rightStampSource.getChunkType();
        final Supplier<SegmentedSortedArray> ssaFactory =
                SegmentedSortedArray.makeFactory(stampChunkType, reverse, control.rightSsaNodeSize());
        final SsaSsaStamp ssaSsaStamp = SsaSsaStamp.make(stampChunkType, reverse);

        final int tableSize = control.initialBuildSize();

        final RightIncrementalHashedAsOfJoinStateManager asOfJoinStateManager =
                TypedHasherFactory.make(RightIncrementalAsOfJoinStateManagerTypedBase.class,
                        leftSources, originalLeftSources, tableSize,
                        control.getMaximumLoadFactor(), control.getTargetLoadFactor());

        final IntegerArraySource slots = new IntegerArraySource();
        int slotCount = asOfJoinStateManager.buildFromLeftSide(leftTable.getRowSet(), leftSources, slots);
        slotCount = asOfJoinStateManager.buildFromRightSide(rightTable.getRowSet(), rightSources, slots, slotCount);

        // These contexts and chunks will be closed when the SSA factory itself is closed by the destroy function of the
        // BucketedChunkedAjMergedListener
        final SizedSafeCloseable<ColumnSource.FillContext> rightStampFillContext =
                new SizedSafeCloseable<>(rightStampSource::makeFillContext);
        final SizedSafeCloseable<LongSortKernel<Values, RowKeys>> sortKernel =
                new SizedSafeCloseable<>(size -> LongSortKernel.makeContext(stampChunkType, order, size, true));
        final SizedSafeCloseable<ColumnSource.FillContext> leftStampFillContext =
                new SizedSafeCloseable<>(leftStampSource::makeFillContext);
        final SizedLongChunk<RowKeys> leftStampKeys = new SizedLongChunk<>();
        final SizedChunk<Values> leftStampValues = new SizedChunk<>(stampChunkType);
        final SizedChunk<Values> rightStampValues = new SizedChunk<>(stampChunkType);
        final SizedLongChunk<RowKeys> rightStampKeys = new SizedLongChunk<>();

        final SsaFactory rightSsaFactory = new SsaFactory() {
            @Override
            public void close() {
                SafeCloseable.closeAll(rightStampFillContext, rightStampValues, rightStampKeys);
            }

            @Override
            public SegmentedSortedArray apply(RowSet rightIndex) {
                final SegmentedSortedArray ssa = ssaFactory.get();
                final int slotSize = rightIndex.intSize();
                if (slotSize > 0) {
                    rightIndex.fillRowKeyChunk(rightStampKeys.ensureCapacity(slotSize));
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
                SafeCloseable.closeAll(sortKernel, leftStampFillContext, leftStampValues, leftStampKeys);
            }

            @Override
            public SegmentedSortedArray apply(RowSet leftIndex) {
                final SegmentedSortedArray ssa = ssaFactory.get();
                final int slotSize = leftIndex.intSize();
                if (slotSize > 0) {

                    leftStampSource.fillChunk(leftStampFillContext.ensureCapacity(slotSize),
                            leftStampValues.ensureCapacity(slotSize), leftIndex);

                    leftIndex.fillRowKeyChunk(leftStampKeys.ensureCapacity(slotSize));

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
                final int slot = slots.getInt(slotIndex);

                // if either initial state is empty, we would prefer to leave things as a RowSet rather than process
                // them into an ssa
                final byte state = asOfJoinStateManager.getState(slot);
                if ((state
                        & RightIncrementalHashedAsOfJoinStateManager.ENTRY_RIGHT_MASK) == RightIncrementalHashedAsOfJoinStateManager.ENTRY_RIGHT_IS_EMPTY) {
                    continue;
                }
                if ((state
                        & RightIncrementalHashedAsOfJoinStateManager.ENTRY_LEFT_MASK) == RightIncrementalHashedAsOfJoinStateManager.ENTRY_LEFT_IS_EMPTY) {
                    continue;
                }

                final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot, rightSsaFactory);
                final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot, leftSsaFactory);
                ssaSsaStamp.processEntry(leftSsa, rightSsa, rowRedirection, disallowExactMatch);
            }

            result = makeResult(leftTable, rightTable, rowRedirection, columnsToAdd, true);
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
                        ssaSsaStamp, control, asOfJoinStateManager, rowRedirection);

        leftRecorder.setMergedListener(mergedJoinListener);
        rightRecorder.setMergedListener(mergedJoinListener);

        leftTable.addUpdateListener(leftRecorder);
        rightTable.addUpdateListener(rightRecorder);

        result.addParentReference(mergedJoinListener);

        leftSsaFactory.close();
        rightSsaFactory.close();

        return result;
    }

    private static Table zeroKeyAjBothIncremental(JoinControl control, QueryTable leftTable, QueryTable rightTable,
            MatchPair[] columnsToAdd, MatchPair stampPair, ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource, SortingOrder order, boolean disallowExactMatch,
            final WritableRowRedirection rowRedirection) {
        final boolean reverse = order == SortingOrder.Descending;

        final ChunkType stampChunkType = rightStampSource.getChunkType();
        final int leftNodeSize = control.leftSsaNodeSize();
        final int rightNodeSize = control.rightSsaNodeSize();
        final SegmentedSortedArray leftSsa = SegmentedSortedArray.make(stampChunkType, reverse, leftNodeSize);
        final SegmentedSortedArray rightSsa = SegmentedSortedArray.make(stampChunkType, reverse, rightNodeSize);

        fillSsaWithSort(rightTable, rightStampSource, rightNodeSize, rightSsa, order);
        fillSsaWithSort(leftTable, leftStampSource, leftNodeSize, leftSsa, order);

        final SsaSsaStamp ssaSsaStamp = SsaSsaStamp.make(stampChunkType, reverse);
        ssaSsaStamp.processEntry(leftSsa, rightSsa, rowRedirection, disallowExactMatch);

        final QueryTable result = makeResult(leftTable, rightTable, rowRedirection, columnsToAdd, true);

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
                        ssaSsaStamp, leftSsa, rightSsa, rowRedirection, control);

        leftRecorder.setMergedListener(mergedJoinListener);
        rightRecorder.setMergedListener(mergedJoinListener);

        leftTable.addUpdateListener(leftRecorder);
        rightTable.addUpdateListener(rightRecorder);

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
                final RowSequence.Iterator rsIt = rightTable.getRowSet().getRowSequenceIterator();
                final WritableChunk<Values> stampChunk = stampSource.getChunkType().makeWritableChunk(nodeSize);
                final WritableLongChunk<RowKeys> keyChunk = WritableLongChunk.makeWritableChunk(nodeSize);
                final LongSortKernel<Values, RowKeys> sortKernel =
                        LongSortKernel.makeContext(stampSource.getChunkType(), order, nodeSize, true)) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(nodeSize);
                stampSource.fillChunk(context, stampChunk, chunkOk);
                chunkOk.fillRowKeyChunk(keyChunk);

                sortKernel.sort(keyChunk, stampChunk);

                ssa.insert(stampChunk, keyChunk);
            }
        }
    }

    private static Table zeroKeyAjRightIncremental(JoinControl control, QueryTable leftTable, QueryTable rightTable,
            MatchPair[] columnsToAdd, MatchPair stampPair, ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource, SortingOrder order, boolean disallowExactMatch,
            final WritableRowRedirection rowRedirection) {
        final boolean reverse = order == SortingOrder.Descending;

        final ChunkType stampChunkType = rightStampSource.getChunkType();
        final int rightNodeSize = control.rightSsaNodeSize();
        final int rightChunkSize = control.rightChunkSize();
        final SegmentedSortedArray ssa = SegmentedSortedArray.make(stampChunkType, reverse, rightNodeSize);

        fillSsaWithSort(rightTable, rightStampSource, rightChunkSize, ssa, order);

        final int leftSize = leftTable.intSize();
        final WritableChunk<Values> leftStampValues = stampChunkType.makeWritableChunk(leftSize);
        final WritableLongChunk<RowKeys> leftStampKeys = WritableLongChunk.makeWritableChunk(leftSize);
        leftTable.getRowSet().fillRowKeyChunk(leftStampKeys);
        try (final ColumnSource.FillContext context = leftStampSource.makeFillContext(leftSize)) {
            leftStampSource.fillChunk(context, leftStampValues, leftTable.getRowSet());
        }

        try (final LongSortKernel<Values, RowKeys> sortKernel =
                LongSortKernel.makeContext(stampChunkType, order, leftSize, true)) {
            sortKernel.sort(leftStampKeys, leftStampValues);
        }

        final ChunkSsaStamp chunkSsaStamp = ChunkSsaStamp.make(stampChunkType, reverse);
        try (final WritableLongChunk<RowKeys> rightKeysForLeft = WritableLongChunk.makeWritableChunk(leftSize)) {
            chunkSsaStamp.processEntry(leftStampValues, leftStampKeys, ssa, rightKeysForLeft, disallowExactMatch);

            for (int ii = 0; ii < leftStampKeys.size(); ++ii) {
                final long index = rightKeysForLeft.get(ii);
                if (index != RowSequence.NULL_ROW_KEY) {
                    rowRedirection.put(leftStampKeys.get(ii), index);
                }
            }
        }

        final QueryTable result = makeResult(leftTable, rightTable, rowRedirection, columnsToAdd, true);
        final ModifiedColumnSet rightStampColumn = rightTable.newModifiedColumnSet(stampPair.rightColumn());
        final ModifiedColumnSet allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        final ModifiedColumnSet.Transformer rightTransformer =
                rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);
        final ChunkEquals stampChunkEquals = ChunkEquals.makeEqual(stampChunkType);
        final CompactKernel stampCompact = CompactKernel.makeCompact(stampChunkType);

        rightTable.addUpdateListener(
                new BaseTable.ListenerImpl(makeListenerDescription(MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                        stampPair, columnsToAdd, reverse, disallowExactMatch), rightTable, result) {
                    @Override
                    public void onUpdate(TableUpdate upstream) {
                        final TableUpdateImpl downstream = new TableUpdateImpl();
                        downstream.added = RowSetFactory.empty();
                        downstream.removed = RowSetFactory.empty();
                        downstream.shifted = RowSetShiftData.EMPTY;
                        downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();

                        final boolean stampModified = upstream.modifiedColumnSet().containsAny(rightStampColumn);

                        final RowSetBuilderRandom modifiedBuilder = RowSetFactory.builderRandom();

                        try (final ColumnSource.FillContext fillContext =
                                rightStampSource.makeFillContext(rightChunkSize);
                                final LongSortKernel<Values, RowKeys> sortKernel =
                                        LongSortKernel.makeContext(stampChunkType, order, rightChunkSize, true)) {

                            final RowSet restampRemovals;
                            final RowSet restampAdditions;
                            if (stampModified) {
                                restampAdditions = upstream.added().union(upstream.modified());
                                restampRemovals = upstream.removed().union(upstream.getModifiedPreShift());
                            } else {
                                restampAdditions = upstream.added();
                                restampRemovals = upstream.removed();
                            }

                            // When removing a row, record the stamp, redirection key, and prior redirection key. Binary
                            // search
                            // in the left for the removed key to find the smallest value geq the removed right. Update
                            // all rows
                            // with the removed redirection to the previous key.
                            try (final RowSequence.Iterator removeit = restampRemovals.getRowSequenceIterator();
                                    final WritableLongChunk<RowKeys> priorRedirections =
                                            WritableLongChunk.makeWritableChunk(rightChunkSize);
                                    final WritableLongChunk<RowKeys> rightKeyIndices =
                                            WritableLongChunk.makeWritableChunk(rightChunkSize);
                                    final WritableChunk<Values> rightStampChunk =
                                            stampChunkType.makeWritableChunk(rightChunkSize)) {
                                while (removeit.hasMore()) {
                                    final RowSequence chunkOk = removeit.getNextRowSequenceWithLength(rightChunkSize);
                                    rightStampSource.fillPrevChunk(fillContext, rightStampChunk, chunkOk);
                                    chunkOk.fillRowKeyChunk(rightKeyIndices);

                                    sortKernel.sort(rightKeyIndices, rightStampChunk);

                                    ssa.removeAndGetPrior(rightStampChunk, rightKeyIndices, priorRedirections);
                                    chunkSsaStamp.processRemovals(leftStampValues, leftStampKeys, rightStampChunk,
                                            rightKeyIndices, priorRedirections, rowRedirection, modifiedBuilder,
                                            disallowExactMatch);
                                }
                            }

                            if (upstream.shifted().nonempty()) {
                                rightIncrementalApplySsaShift(upstream.shifted(), ssa, sortKernel, fillContext,
                                        restampRemovals, rightTable, rightChunkSize, rightStampSource, chunkSsaStamp,
                                        leftStampValues, leftStampKeys, rowRedirection, disallowExactMatch);
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
                                    final WritableLongChunk<RowKeys> insertedIndices =
                                            WritableLongChunk.makeWritableChunk(rightChunkSize);
                                    final WritableBooleanChunk<Any> retainStamps =
                                            WritableBooleanChunk.makeWritableChunk(rightChunkSize)) {
                                final int chunks = (restampAdditions.intSize() + control.rightChunkSize() - 1)
                                        / control.rightChunkSize();
                                for (int ii = 0; ii < chunks; ++ii) {
                                    final long startChunk = chunks - ii - 1;
                                    try (final RowSet chunkOk =
                                            restampAdditions.subSetByPositionRange(
                                                    startChunk * control.rightChunkSize(),
                                                    (startChunk + 1) * control.rightChunkSize())) {
                                        rightStampSource.fillChunk(fillContext, stampChunk, chunkOk);
                                        insertedIndices.setSize(chunkOk.intSize());
                                        chunkOk.fillRowKeyChunk(insertedIndices);

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
                                                insertedIndices, nextRightValue, rowRedirection, modifiedBuilder,
                                                endsWithLastValue, disallowExactMatch);
                                    }
                                }
                            }

                            // if the stamp was not modified, then we need to figure out the responsive rows to mark as
                            // modified
                            if (!stampModified && upstream.modified().isNonempty()) {
                                try (final RowSequence.Iterator modit = upstream.modified().getRowSequenceIterator();
                                        final WritableLongChunk<RowKeys> rightStampIndices =
                                                WritableLongChunk.makeWritableChunk(rightChunkSize);
                                        final WritableChunk<Values> rightStampChunk =
                                                stampChunkType.makeWritableChunk(rightChunkSize)) {
                                    while (modit.hasMore()) {
                                        final RowSequence chunkOk = modit.getNextRowSequenceWithLength(rightChunkSize);
                                        rightStampSource.fillChunk(fillContext, rightStampChunk, chunkOk);
                                        chunkOk.fillRowKeyChunk(rightStampIndices);

                                        sortKernel.sort(rightStampIndices, rightStampChunk);

                                        chunkSsaStamp.findModified(0, leftStampValues, leftStampKeys, rowRedirection,
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

                        if (stampModified || upstream.added().isNonempty() || upstream.removed().isNonempty()) {
                            // If we kept track of whether or not something actually changed, then we could skip
                            // painting all
                            // the right columns as modified. It is not clear whether it is worth the additional
                            // complexity.
                            downstream.modifiedColumnSet().setAll(allRightColumns);
                        } else {
                            rightTransformer.transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet());
                        }

                        downstream.modified = modifiedBuilder.build();

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

    private static void rightIncrementalApplySsaShift(RowSetShiftData shiftData, SegmentedSortedArray ssa,
            LongSortKernel<Values, RowKeys> sortKernel, ChunkSource.FillContext fillContext,
            RowSet restampRemovals, QueryTable table,
            int chunkSize, ColumnSource<?> stampSource, ChunkSsaStamp chunkSsaStamp,
            WritableChunk<Values> leftStampValues, WritableLongChunk<RowKeys> leftStampKeys,
            WritableRowRedirection rowRedirection, boolean disallowExactMatch) {

        try (final RowSet fullPrevRowSet = table.getRowSet().copyPrev();
                final RowSet previousToShift = fullPrevRowSet.minus(restampRemovals);
                final SizedSafeCloseable<ColumnSource.FillContext> shiftFillContext =
                        new SizedSafeCloseable<>(stampSource::makeFillContext);
                final SizedSafeCloseable<LongSortKernel<Values, RowKeys>> shiftSortKernel =
                        new SizedSafeCloseable<>(sz -> LongSortKernel.makeContext(stampSource.getChunkType(),
                                ssa.isReversed() ? SortingOrder.Descending : SortingOrder.Ascending, sz, true));
                final SizedChunk<Values> rightStampValues = new SizedChunk<>(stampSource.getChunkType());
                final SizedLongChunk<RowKeys> rightStampKeys = new SizedLongChunk<>()) {

            final RowSetShiftData.Iterator sit = shiftData.applyIterator();
            while (sit.hasNext()) {
                sit.next();
                try (final RowSet rowSetToShift = previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange())) {
                    if (rowSetToShift.isEmpty()) {
                        continue;
                    }

                    if (sit.polarityReversed()) {
                        final int shiftSize = rowSetToShift.intSize();

                        rowSetToShift.fillRowKeyChunk(rightStampKeys.ensureCapacity(shiftSize));
                        if (chunkSize >= shiftSize) {
                            stampSource.fillPrevChunk(fillContext, rightStampValues.ensureCapacity(shiftSize),
                                    rowSetToShift);
                            sortKernel.sort(rightStampKeys.get(), rightStampValues.get());
                        } else {
                            stampSource.fillPrevChunk(shiftFillContext.ensureCapacity(shiftSize),
                                    rightStampValues.ensureCapacity(shiftSize), rowSetToShift);
                            shiftSortKernel.ensureCapacity(shiftSize).sort(rightStampKeys.get(),
                                    rightStampValues.get());
                        }

                        chunkSsaStamp.applyShift(leftStampValues, leftStampKeys, rightStampValues.get(),
                                rightStampKeys.get(), sit.shiftDelta(), rowRedirection, disallowExactMatch);
                        ssa.applyShiftReverse(rightStampValues.get(), rightStampKeys.get(), sit.shiftDelta());
                    } else {
                        if (rowSetToShift.size() > chunkSize) {
                            try (final RowSequence.Iterator shiftIt = rowSetToShift.getRowSequenceIterator()) {
                                while (shiftIt.hasMore()) {
                                    final RowSequence chunkOk = shiftIt.getNextRowSequenceWithLength(chunkSize);
                                    stampSource.fillPrevChunk(fillContext, rightStampValues.ensureCapacity(chunkSize),
                                            chunkOk);

                                    chunkOk.fillRowKeyChunk(rightStampKeys.ensureCapacity(chunkSize));

                                    sortKernel.sort(rightStampKeys.get(), rightStampValues.get());

                                    ssa.applyShift(rightStampValues.get(), rightStampKeys.get(), sit.shiftDelta());
                                    chunkSsaStamp.applyShift(leftStampValues, leftStampKeys, rightStampValues.get(),
                                            rightStampKeys.get(), sit.shiftDelta(), rowRedirection,
                                            disallowExactMatch);
                                }
                            }
                        } else {
                            stampSource.fillPrevChunk(fillContext,
                                    rightStampValues.ensureCapacity(rowSetToShift.intSize()), rowSetToShift);
                            rowSetToShift.fillRowKeyChunk(rightStampKeys.ensureCapacity(rowSetToShift.intSize()));

                            sortKernel.sort(rightStampKeys.get(), rightStampValues.get());

                            ssa.applyShift(rightStampValues.get(), rightStampKeys.get(), sit.shiftDelta());
                            chunkSsaStamp.applyShift(leftStampValues, leftStampKeys, rightStampValues.get(),
                                    rightStampKeys.get(), sit.shiftDelta(), rowRedirection, disallowExactMatch);
                        }
                    }
                }
            }
        }
    }

    private static Table zeroKeyAjRightStatic(QueryTable leftTable, Table rightTable, MatchPair[] columnsToAdd,
            MatchPair stampPair, ColumnSource<?> leftStampSource, ColumnSource<?> originalRightStampSource,
            ColumnSource<?> rightStampSource, SortingOrder order, boolean disallowExactMatch,
            final WritableRowRedirection rowRedirection) {
        final RowSet rightRowSet = rightTable.getRowSet();

        final WritableLongChunk<RowKeys> rightStampKeys = WritableLongChunk.makeWritableChunk(rightRowSet.intSize());
        final WritableChunk<Values> rightStampValues =
                rightStampSource.getChunkType().makeWritableChunk(rightRowSet.intSize());

        try (final SafeCloseableList chunksToClose = new SafeCloseableList(rightStampKeys, rightStampValues)) {
            final Supplier<String> keyStringSupplier = () -> "[] (zero key columns)";
            try (final AsOfStampContext stampContext = new AsOfStampContext(order, disallowExactMatch, leftStampSource,
                    rightStampSource, originalRightStampSource)) {
                stampContext.getAndCompactStamps(rightRowSet, rightStampKeys, rightStampValues);
                stampContext.processEntry(leftTable.getRowSet(), rightStampValues, rightStampKeys, rowRedirection);
            }
            final QueryTable result =
                    makeResult(leftTable, rightTable, rowRedirection, columnsToAdd, leftTable.isRefreshing());
            if (!leftTable.isRefreshing()) {
                return result;
            }

            final ModifiedColumnSet leftStampColumn = leftTable.newModifiedColumnSet(stampPair.leftColumn());
            final ModifiedColumnSet allRightColumns =
                    result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
            final ModifiedColumnSet.Transformer leftTransformer =
                    leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());

            final WritableLongChunk<RowKeys> compactedRightStampKeys;
            final WritableChunk<Values> compactedRightStampValues;
            if (rightStampKeys.size() < rightRowSet.size()) {
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
                    .addUpdateListener(
                            new BaseTable.ListenerImpl(
                                    makeListenerDescription(MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                                            stampPair,
                                            columnsToAdd,
                                            order == SortingOrder.Descending,
                                            disallowExactMatch),
                                    leftTable, result) {
                                @Override
                                public void onUpdate(TableUpdate upstream) {
                                    final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);

                                    rowRedirection.removeAll(upstream.removed());

                                    final boolean stampModified = upstream.modified().isNonempty()
                                            && upstream.modifiedColumnSet().containsAny(leftStampColumn);

                                    final RowSet restampKeys;
                                    if (stampModified) {
                                        rowRedirection.removeAll(upstream.getModifiedPreShift());
                                        restampKeys = upstream.modified().union(upstream.added());
                                    } else {
                                        restampKeys = upstream.added();
                                    }

                                    try (final RowSet prevLeftRowSet = leftTable.getRowSet().copyPrev()) {
                                        rowRedirection.applyShift(prevLeftRowSet, upstream.shifted());
                                    }

                                    try (final AsOfStampContext stampContext =
                                            new AsOfStampContext(order, disallowExactMatch, leftStampSource,
                                                    rightStampSource, originalRightStampSource)) {
                                        stampContext.processEntry(restampKeys, compactedRightStampValues,
                                                compactedRightStampKeys, rowRedirection);
                                    }

                                    downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                                    leftTransformer.clearAndTransform(upstream.modifiedColumnSet(),
                                            downstream.modifiedColumnSet());
                                    if (stampModified) {
                                        downstream.modifiedColumnSet().setAll(allRightColumns);
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


    private static QueryTable makeResult(QueryTable leftTable, Table rightTable, RowRedirection rowRedirection,
            MatchPair[] columnsToAdd, boolean refreshing) {
        final Map<String, ColumnSource<?>> columnSources = new LinkedHashMap<>(leftTable.getColumnSourceMap());
        Arrays.stream(columnsToAdd).forEach(mp -> {
            // note that we must always redirect the right-hand side, because unmatched rows will be redirected to null
            final ColumnSource<?> rightSource =
                    RedirectedColumnSource.alwaysRedirect(rowRedirection, rightTable.getColumnSource(mp.rightColumn()));
            if (refreshing) {
                rightSource.startTrackingPrevValues();
            }
            columnSources.put(mp.leftColumn(), rightSource);
        });
        if (refreshing) {
            rowRedirection.writableCast().startTrackingPrevValues();
        }
        return new QueryTable(leftTable.getRowSet(), columnSources);
    }
}
