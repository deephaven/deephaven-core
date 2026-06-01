//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRollupUniqueOperator and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.unique;

import io.deephaven.engine.table.impl.by.RollupConstants;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.*;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.compare.ObjectComparisons;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * This operator computes the single unique value of a particular aggregated state at the second and higher levels of a
 * rollup. Each constituent state below reports, via its value column and its {@code singletonCount} column, whether it
 * is empty ({@code 0}/{@code NULL_LONG}), uniquely a single value ({@code > 0}), or non-unique ({@code < 0}).
 *
 * <p>
 * This operator ignores the values of non-unique constituents, tracking only how many constituents are non-unique
 * ({@code nonUniqueCount}); a single such constituent forces this state's result to the non-unique sentinel. The values
 * of the unique constituents are gathered into a multiset, represented like the chunked operator's: a single distinct
 * value lives in {@code singletonValue}/{@code internalSingletonCount} with no SSM, and two or more distinct values are
 * held in an SSM in {@code ssms}. The exposed result ({@code internalResult} + {@code singletonCount}) is derived from
 * the multiset and {@code nonUniqueCount}, so a grandparent rollup reads this level exactly as it reads a base
 * constituent.
 */
public class ObjectRollupUniqueOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final ObjectSsmBackedSource ssms;
    private final ObjectArraySource internalResult;
    private final LongArraySource singletonCount;
    private final ObjectArraySource singletonValue;
    private final LongArraySource internalSingletonCount;
    private final LongArraySource nonUniqueCount;
    private final ColumnSource<?> externalResult;
    private final ColumnSource<?> constituentSingletonCount;
    private final Object onlyNullsSentinel;
    private final Object nonUniqueSentinel;

    public ObjectRollupUniqueOperator(
            // region Constructor
            Class<?> type,
            // endregion Constructor
            String name,
            Object onlyNullsSentinel,
            Object nonUniqueSentinel,
            ColumnSource<?> constituentSingletonCount) {
        this.name = name;
        this.nonUniqueSentinel = nonUniqueSentinel;
        this.onlyNullsSentinel = onlyNullsSentinel;
        this.constituentSingletonCount = constituentSingletonCount;
        // region SsmCreation
        this.ssms = new ObjectSsmBackedSource(type);
        // endregion SsmCreation
        // region ResultCreation
        this.internalResult = new ObjectArraySource(type);
        // endregion ResultCreation
        this.singletonValue = new ObjectArraySource(type);
        this.singletonCount = new LongArraySource();
        this.internalSingletonCount = new LongArraySource();
        this.nonUniqueCount = new LongArraySource();
        // region ResultAssignment
        this.externalResult = internalResult;
        // endregion ResultAssignment
    }

    // region Bucketed Updates
    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<Object, ? extends Values> valueChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final Object priorValue = internalResult.getUnsafe(destination);
            long nonUniqueDelta = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                final long sc = constituentSingletonCount.getLong(inputRowKeys.get(kk));
                if (sc > 0) {
                    addValueToMultiset(destination, valueChunk.get(kk));
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta++;
                }
            }
            applyNonUniqueDelta(destination, nonUniqueDelta);
            finalizeState(destination);
            stateModified.set(ii, exposedChanged(destination, priorState, priorValue));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<Object, ? extends Values> valueChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final Object priorValue = internalResult.getUnsafe(destination);
            long nonUniqueDelta = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                // the removed constituents are gone from the current table, so read their prior singletonCount
                final long sc = constituentSingletonCount.getPrevLong(inputRowKeys.get(kk));
                if (sc > 0) {
                    removeValueFromMultiset(destination, valueChunk.get(kk));
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta--;
                }
            }
            applyNonUniqueDelta(destination, nonUniqueDelta);
            finalizeState(destination);
            stateModified.set(ii, exposedChanged(destination, priorState, priorValue));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<Object, ? extends Values> prevValueChunk = preValues.asObjectChunk();
        final ObjectChunk<Object, ? extends Values> postValueChunk = postValues.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final Object priorValue = internalResult.getUnsafe(destination);
            long nonUniqueDelta = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                final long rowKey = postShiftRowKeys.get(kk);
                // reverse the constituent's previous contribution, then apply its current one
                final long prevSc = constituentSingletonCount.getPrevLong(rowKey);
                if (prevSc > 0) {
                    removeValueFromMultiset(destination, prevValueChunk.get(kk));
                } else if (isNonUniqueState(prevSc)) {
                    nonUniqueDelta--;
                }
                final long sc = constituentSingletonCount.getLong(rowKey);
                if (sc > 0) {
                    addValueToMultiset(destination, postValueChunk.get(kk));
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta++;
                }
            }
            applyNonUniqueDelta(destination, nonUniqueDelta);
            finalizeState(destination);
            stateModified.set(ii, exposedChanged(destination, priorState, priorValue));
        }
    }
    // endregion

    // region Singleton Updates
    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final ObjectChunk<Object, ? extends Values> valueChunk = values.asObjectChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final Object priorValue = internalResult.getUnsafe(destination);
        long nonUniqueDelta = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            final long sc = constituentSingletonCount.getLong(inputRowKeys.get(kk));
            if (sc > 0) {
                addValueToMultiset(destination, valueChunk.get(kk));
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta++;
            }
        }
        applyNonUniqueDelta(destination, nonUniqueDelta);
        finalizeState(destination);
        return exposedChanged(destination, priorState, priorValue);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final ObjectChunk<Object, ? extends Values> valueChunk = values.asObjectChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final Object priorValue = internalResult.getUnsafe(destination);
        long nonUniqueDelta = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            // the removed constituents are gone from the current table, so read their prior singletonCount
            final long sc = constituentSingletonCount.getPrevLong(inputRowKeys.get(kk));
            if (sc > 0) {
                removeValueFromMultiset(destination, valueChunk.get(kk));
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta--;
            }
        }
        applyNonUniqueDelta(destination, nonUniqueDelta);
        finalizeState(destination);
        return exposedChanged(destination, priorState, priorValue);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final ObjectChunk<Object, ? extends Values> prevValueChunk = preValues.asObjectChunk();
        final ObjectChunk<Object, ? extends Values> postValueChunk = postValues.asObjectChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final Object priorValue = internalResult.getUnsafe(destination);
        long nonUniqueDelta = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            final long rowKey = postShiftRowKeys.get(kk);
            final long prevSc = constituentSingletonCount.getPrevLong(rowKey);
            if (prevSc > 0) {
                removeValueFromMultiset(destination, prevValueChunk.get(kk));
            } else if (isNonUniqueState(prevSc)) {
                nonUniqueDelta--;
            }
            final long sc = constituentSingletonCount.getLong(rowKey);
            if (sc > 0) {
                addValueToMultiset(destination, postValueChunk.get(kk));
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta++;
            }
        }
        applyNonUniqueDelta(destination, nonUniqueDelta);
        finalizeState(destination);
        return exposedChanged(destination, priorState, priorValue);
    }
    // endregion

    // region State Machine
    /**
     * Add one occurrence of {@code value} (one unique constituent holding it) to the destination's multiset,
     * transitioning empty â unique â non-unique (allocating an SSM) as a second distinct value arrives.
     */
    private void addValueToMultiset(long destination, Object value) {
        final long mc = internalSingletonCount.getUnsafe(destination);
        if (mc > 0) {
            final Object held = singletonValue.getUnsafe(destination);
            if (ObjectComparisons.eq(value, held)) {
                internalSingletonCount.set(destination, mc + 1);
                return;
            }
            final ObjectSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.insert(held, mc);
            ssm.insert(value, 1);
            internalSingletonCount.set(destination, -1L);
        } else if (isNonUniqueState(mc)) {
            ssmForSlot(destination).insert(value, 1);
        } else {
            singletonValue.set(destination, value);
            internalSingletonCount.set(destination, 1L);
        }
    }

    /**
     * Remove one occurrence of {@code value} from the destination's multiset, collapsing a non-unique SSM back to a
     * unique value (or empty) as its cardinality drops. The value must currently be present.
     */
    private void removeValueFromMultiset(long destination, Object value) {
        final long mc = internalSingletonCount.getUnsafe(destination);
        if (mc > 0) {
            if (mc == 1) {
                internalSingletonCount.set(destination, 0L);
            } else {
                internalSingletonCount.set(destination, mc - 1);
            }
        } else {
            final ObjectSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.remove(value, 1);
            if (ssm.isEmpty()) {
                clearSsm(destination);
                internalSingletonCount.set(destination, 0L);
            } else if (ssm.size() == 1) {
                singletonValue.set(destination, ssm.get(0));
                internalSingletonCount.set(destination, ssm.getMaxCount());
                clearSsm(destination);
            }
        }
    }

    private void applyNonUniqueDelta(long destination, long nonUniqueDelta) {
        if (nonUniqueDelta != 0) {
            final long prior = nonUniqueCount.getUnsafe(destination);
            nonUniqueCount.set(destination, (prior == NULL_LONG ? 0 : prior) + nonUniqueDelta);
        }
    }

    /**
     * Derive the exposed result ({@code internalResult} + {@code singletonCount}) from the multiset and
     * {@code nonUniqueCount}: any non-unique constituent forces the non-unique sentinel, otherwise the multiset's
     * cardinality decides empty / unique / non-unique.
     */
    private void finalizeState(long destination) {
        final long nuc = nonUniqueCount.getUnsafe(destination);
        final long mc = internalSingletonCount.getUnsafe(destination);
        if (nuc != NULL_LONG && nuc > 0) {
            internalResult.set(destination, nonUniqueSentinel);
            singletonCount.set(destination, -1L);
        } else if (mc > 0) {
            internalResult.set(destination, singletonValue.getUnsafe(destination));
            singletonCount.set(destination, mc);
        } else if (isNonUniqueState(mc)) {
            internalResult.set(destination, nonUniqueSentinel);
            singletonCount.set(destination, -1L);
        } else {
            internalResult.set(destination, onlyNullsSentinel);
            singletonCount.set(destination, 0L);
        }
    }

    private static boolean isNonUniqueState(long stateCount) {
        return stateCount != NULL_LONG && stateCount < 0;
    }

    /**
     * Whether an exposed result column of {@code destination} changed: its result value, or its singletonCount (whose
     * magnitude can change while the value does not). Both are exposed for a grandparent rollup.
     */
    private boolean exposedChanged(long destination, long priorState, Object priorValue) {
        if (!ObjectComparisons.eq(internalResult.getUnsafe(destination), priorValue)) {
            return true;
        }
        return singletonCount.getUnsafe(destination) != priorState;
    }
    // endregion

    // region IterativeOperator / DistinctAggregationOperator
    @Override
    public void ensureCapacity(long tableSize) {
        internalResult.ensureCapacity(tableSize);
        singletonCount.ensureCapacity(tableSize);
        singletonValue.ensureCapacity(tableSize);
        internalSingletonCount.ensureCapacity(tableSize);
        nonUniqueCount.ensureCapacity(tableSize);
        ssms.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put(name, externalResult);
        columns.put(name + RollupConstants.ROLLUP_DISTINCT_SSM_COUNT_COLUMN_ID + RollupConstants.ROLLUP_COLUMN_SUFFIX,
                singletonCount);
        return columns;
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
        singletonCount.startTrackingPrevValues();
    }
    // endregion

    // region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctRollupContext(ChunkType.Object, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctRollupContext(ChunkType.Object);
    }
    // endregion

    // region Private Helpers
    private ObjectSegmentedSortedMultiset ssmForSlot(long destination) {
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }
    // endregion
}
