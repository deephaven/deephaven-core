//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct.unique;

import io.deephaven.engine.table.impl.sources.BooleanArraySource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.CharComparisons;

/**
 * Computes the single unique value of each aggregated state for an input that only adds rows.
 *
 * <p>
 * Without removals or modifications, a non-unique state never becomes unique again, so no state keeps its values or
 * their multiplicities; {@code addOnlyUnique} records whether each state is empty ({@code null}), unique
 * ({@code TRUE}), or non-unique ({@code FALSE}).
 */
public class CharChunkedAddOnlyUniqueOperator extends BaseCharChunkedUniqueOperator {
    private final BooleanArraySource addOnlyUnique;

    public CharChunkedAddOnlyUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name, boolean countNullNaN, char onlyNullsSentinel, char nonUniqueSentinel) {
        super(
                // region SuperConstructor
                // endregion SuperConstructor
                name, countNullNaN, onlyNullsSentinel, nonUniqueSentinel);
        this.addOnlyUnique = new BooleanArraySource();
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw removalsUnsupported();
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw removalsUnsupported();
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        throw removalsUnsupported();
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        throw removalsUnsupported();
    }

    private static UnsupportedOperationException removalsUnsupported() {
        return new UnsupportedOperationException("Add-only unique operators do not support removals or modifications");
    }

    /**
     * Apply an addition of the compacted distinct values in {@code [start, start + len)} to an add-only
     * {@code destination}'s state, transitioning empty -> unique -> non-unique.
     */
    @Override
    protected boolean addToState(long destination, WritableCharChunk<? extends Values> values,
            WritableIntChunk<ChunkLengths> counts, int start, int len) {
        final Boolean priorUnique = addOnlyUnique.getUnsafe(destination);
        if (priorUnique == null) {
            // empty -> non-empty is always a change to the result
            if (len == 1) {
                addOnlyUnique.set(destination, Boolean.TRUE);
                internalResult.set(destination, values.get(start));
            } else {
                addOnlyUnique.set(destination, Boolean.FALSE);
                internalResult.set(destination, nonUniqueSentinel);
            }
            return true;
        }
        if (!priorUnique) {
            return false;
        }
        final char held = internalResult.getUnsafe(destination);
        if (len == 1 && eq(values.get(start), held)) {
            return false;
        }
        addOnlyUnique.set(destination, Boolean.FALSE);
        internalResult.set(destination, nonUniqueSentinel);
        return !CharComparisons.eq(held, nonUniqueSentinel);
    }

    @Override
    public void ensureCapacity(long tableSize) {
        super.ensureCapacity(tableSize);
        addOnlyUnique.ensureCapacity(tableSize);
    }
}
