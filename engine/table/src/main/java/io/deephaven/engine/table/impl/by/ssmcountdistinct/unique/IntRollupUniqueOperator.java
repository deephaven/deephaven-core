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
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.ssms.IntSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.compare.IntComparisons;

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
 * of the unique constituents are gathered into a multiset: a single distinct value lives in {@code singletonValue} with
 * its count in {@code singletonCount}, and two or more distinct values are held in an SSM in {@code ssms}.
 *
 * <p>
 * {@code singletonCount} is also the column a grandparent reads, so it encodes both the result and the multiset's
 * count: {@code 0}/{@code NULL_LONG} empty, {@code > 0} the unique count, and {@code < 0} non-unique. When a non-unique
 * constituent forces a non-unique result while the multiset still holds a single value, that value's count {@code C} is
 * preserved as {@code -1 - C} (using the otherwise-unused {@code [NULL_LONG, -2]} range) so it survives until the
 * non-unique constituents are gone; an {@code ssms} entry, when present, means two or more distinct values.
 */
public class IntRollupUniqueOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final IntSsmBackedSource ssms;
    private final IntegerArraySource internalResult;
    private final LongArraySource singletonCount;
    private final IntegerArraySource singletonValue;
    private final LongArraySource nonUniqueCount;
    private final ColumnSource<?> externalResult;
    private final ColumnSource<?> constituentSingletonCount;
    private final int onlyNullsSentinel;
    private final int nonUniqueSentinel;

    public IntRollupUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name,
            int onlyNullsSentinel,
            int nonUniqueSentinel,
            ColumnSource<?> constituentSingletonCount) {
        this.name = name;
        this.nonUniqueSentinel = nonUniqueSentinel;
        this.onlyNullsSentinel = onlyNullsSentinel;
        this.constituentSingletonCount = constituentSingletonCount;
        // region SsmCreation
        this.ssms = new IntSsmBackedSource();
        // endregion SsmCreation
        // region ResultCreation
        this.internalResult = new IntegerArraySource();
        // endregion ResultCreation
        this.singletonValue = new IntegerArraySource();
        this.singletonCount = new LongArraySource();
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
        final IntChunk<? extends Values> valueChunk = values.asIntChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final int priorValue = internalResult.getUnsafe(destination);
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
        final IntChunk<? extends Values> valueChunk = values.asIntChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final int priorValue = internalResult.getUnsafe(destination);
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
        final IntChunk<? extends Values> prevValueChunk = preValues.asIntChunk();
        final IntChunk<? extends Values> postValueChunk = postValues.asIntChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final int priorValue = internalResult.getUnsafe(destination);
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
        final IntChunk<? extends Values> valueChunk = values.asIntChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final int priorValue = internalResult.getUnsafe(destination);
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
        final IntChunk<? extends Values> valueChunk = values.asIntChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final int priorValue = internalResult.getUnsafe(destination);
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
        final IntChunk<? extends Values> prevValueChunk = preValues.asIntChunk();
        final IntChunk<? extends Values> postValueChunk = postValues.asIntChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final int priorValue = internalResult.getUnsafe(destination);
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
     * transitioning empty -> unique -> non-unique (allocating an SSM) as a second distinct value arrives. The
     * multiset's singleton count lives in {@code singletonCount} (see {@link #singletonCountValue}); two or more
     * distinct values live in an SSM in {@code ssms}. {@link #finalizeState} re-derives the exposed encoding afterward.
     */
    private void addValueToMultiset(long destination, int value) {
        final IntSegmentedSortedMultiset ssm = ssms.getCurrentSsm(destination);
        if (ssm != null) {
            ssm.insert(value, 1);
            return;
        }
        final long count = singletonCountValue(destination);
        if (count == 0) {
            singletonValue.set(destination, value);
            singletonCount.set(destination, 1L);
        } else {
            final int held = singletonValue.getUnsafe(destination);
            if (IntComparisons.eq(value, held)) {
                singletonCount.set(destination, count + 1);
            } else {
                // a second distinct value is arriving; materialize an SSM holding both
                final IntSegmentedSortedMultiset newSsm = ssmForSlot(destination);
                newSsm.insert(held, count);
                newSsm.insert(value, 1);
            }
        }
    }

    /**
     * Remove one occurrence of {@code value} from the destination's multiset, collapsing a non-unique SSM back to a
     * unique value (or empty) as its cardinality drops. The value must currently be present.
     */
    private void removeValueFromMultiset(long destination, int value) {
        final IntSegmentedSortedMultiset ssm = ssms.getCurrentSsm(destination);
        if (ssm != null) {
            ssm.remove(value, 1);
            if (ssm.isEmpty()) {
                clearSsm(destination);
                singletonCount.set(destination, 0L);
            } else if (ssm.size() == 1) {
                singletonValue.set(destination, ssm.get(0));
                singletonCount.set(destination, ssm.getMaxCount());
                clearSsm(destination);
            }
            return;
        }
        // a singleton multiset can only be asked to remove its single held value
        singletonCount.set(destination, singletonCountValue(destination) - 1);
    }

    private void applyNonUniqueDelta(long destination, long nonUniqueDelta) {
        if (nonUniqueDelta != 0) {
            final long prior = nonUniqueCount.getUnsafe(destination);
            nonUniqueCount.set(destination, (prior == NULL_LONG ? 0 : prior) + nonUniqueDelta);
        }
    }

    /**
     * The multiset's singleton count (the number of unique constituents holding {@link #singletonValue}), or {@code 0}
     * when empty. Only meaningful when there is no SSM. Recovers the count whether {@code singletonCount} currently
     * holds the unique encoding ({@code count}) or the non-unique encoding ({@code -1 - count}); see
     * {@link #finalizeState}.
     */
    private long singletonCountValue(long destination) {
        final long sc = singletonCount.getUnsafe(destination);
        if (sc == NULL_LONG) {
            return 0;
        }
        return sc >= 0 ? sc : -1 - sc;
    }

    /**
     * Re-derive the exposed encoding of {@code singletonCount} (and {@code internalResult}) from the multiset and
     * {@code nonUniqueCount}. A grandparent reads {@code singletonCount} as: {@code > 0} unique (the count),
     * {@code 0}/{@code NULL_LONG} empty, {@code < 0} non-unique. When a non-unique constituent forces a non-unique
     * result while the multiset is still a single value, the count is preserved as {@code -1 - count} so it survives
     * until the non-unique constituents are gone.
     */
    private void finalizeState(long destination) {
        if (ssms.getCurrentSsm(destination) != null) {
            // two or more distinct values
            internalResult.set(destination, nonUniqueSentinel);
            singletonCount.set(destination, -1L);
            return;
        }
        final long nuc = nonUniqueCount.getUnsafe(destination);
        final long count = singletonCountValue(destination);
        if (nuc != NULL_LONG && nuc > 0) {
            internalResult.set(destination, nonUniqueSentinel);
            singletonCount.set(destination, -1 - count);
        } else if (count > 0) {
            internalResult.set(destination, singletonValue.getUnsafe(destination));
            singletonCount.set(destination, count);
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
    private boolean exposedChanged(long destination, long priorState, int priorValue) {
        if (!IntComparisons.eq(internalResult.getUnsafe(destination), priorValue)) {
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
        return new BucketSsmDistinctRollupContext(ChunkType.Int, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctRollupContext(ChunkType.Int);
    }
    // endregion

    // region Private Helpers
    private IntSegmentedSortedMultiset ssmForSlot(long destination) {
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }
    // endregion
}
