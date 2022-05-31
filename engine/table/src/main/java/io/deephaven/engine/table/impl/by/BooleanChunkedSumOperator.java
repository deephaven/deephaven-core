/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.NullSafeAddition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative Boolean Sum.
 *
 * Any true value makes the result true.  If there are no values, the result is null.  All false values result in false.
*/
public final class BooleanChunkedSumOperator implements IterativeChunkedAggregationOperator {
    private final String name;
    private final LongArraySource resultColumn = new LongArraySource();
    private final LongArraySource falseCount = new LongArraySource();

    BooleanChunkedSumOperator(String name) {
        this.name = name;
    }

    /**
     * Add to the value at the given position, return the old value.
     *
     * @param source the column source to update
     * @param destPos the position within the column source
     * @return the value, before the update update
     */
    private static long add(LongArraySource source, long destPos, long count) {
        final long value = source.getUnsafe(destPos);
        if (value == QueryConstants.NULL_LONG) {
            source.set(destPos, count);
            return 0;
        } else {
            source.set(destPos, value + count);
            return value;
        }
    }

    /**
     * Returns true if the source has a positive value at the given position.
     *
     * @param source the column source to interrogate
     * @param destPos the position within the column source
     * @return true if the value is not null and positive
     */
    private static boolean hasValue(LongArraySource source, long destPos) {
        final long value = source.getUnsafe(destPos);
        //noinspection ConditionCoveredByFurtherCondition
        return value != QueryConstants.NULL_LONG && value > 0;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean hasTrue(LongArraySource trueCount, long destPos) {
        return hasValue(trueCount, destPos);
    }

    private boolean hasFalse(long destPos) {
        return hasValue(falseCount, destPos);
    }

    private static void sumChunk(ObjectChunk<Boolean, ? extends Values> values, int chunkStart, int chunkSize, MutableInt chunkNull, MutableInt chunkTrue, MutableInt chunkFalse) {
        final int chunkEnd = chunkStart + chunkSize;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final Boolean aBoolean = values.get(ii);
            if (aBoolean == null) {
                chunkNull.increment();
            } else if (aBoolean) {
                chunkTrue.increment();
            } else {
                chunkFalse.increment();
            }
        }
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<Boolean, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<Boolean, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<Boolean, ? extends Values> preAsObjectChunk = previousValues.asObjectChunk();
        final ObjectChunk<Boolean, ? extends Values> postAsObjectChunk = newValues.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, modifyChunk(preAsObjectChunk, postAsObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        return modifyChunk(previousValues.asObjectChunk(), newValues.asObjectChunk(), destination, 0, newValues.size());
    }

    private boolean addChunk(ObjectChunk<Boolean, ? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNull = new MutableInt(0);
        final MutableInt chunkTrue = new MutableInt(0);
        final MutableInt chunkFalse = new MutableInt(0);
        sumChunk(values, chunkStart, chunkSize, chunkNull, chunkTrue, chunkFalse);

        boolean modified = false;
        if (chunkTrue.intValue() > 0) {
            add(resultColumn, destination,  chunkTrue.intValue());
            modified = true;
        }
        if (chunkFalse.intValue() > 0) {
            final long oldFalse = add(falseCount, destination, chunkFalse.intValue());
            if (chunkTrue.intValue() == 0 && oldFalse == 0 && !hasTrue(resultColumn, destination)) {
                resultColumn.set(destination, 0L);
                modified = true;
            }
        }
        if (chunkNull.intValue() > 0 && chunkFalse.intValue() == 0 && chunkTrue.intValue() == 0) {
            if (!hasTrue(resultColumn, destination) && !hasFalse(destination)) {
                resultColumn.set(destination, QueryConstants.NULL_LONG);
                modified = true;
            }
        }
        return modified;
    }

    private boolean removeChunk(ObjectChunk<Boolean, ? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNull = new MutableInt(0);
        final MutableInt chunkTrue = new MutableInt(0);
        final MutableInt chunkFalse = new MutableInt(0);
        sumChunk(values, chunkStart, chunkSize, chunkNull, chunkTrue, chunkFalse);

        boolean modified = false;

        long newFalse = -1; // impossible value
        if (chunkFalse.intValue() > 0) {
            final long oldFalse = falseCount.getUnsafe(destination);
            newFalse = NullSafeAddition.plusLong(oldFalse, -chunkFalse.intValue());
            falseCount.set(destination, newFalse);
        }
        if (chunkTrue.intValue() > 0) {
            final long oldTrue = resultColumn.getUnsafe(destination);
            final long newTrue;
            if (oldTrue == chunkTrue.intValue()) {
                // we are zeroing ourselves out
                if (newFalse < 0) {
                    newFalse = falseCount.getUnsafe(destination);
                }
                if (newFalse > 0) { // we've set it, or read a null above
                    newTrue = 0;
                } else {
                    newTrue = QueryConstants.NULL_LONG;
                }
            } else {
                newTrue = oldTrue - chunkTrue.intValue();
            }
            modified = true;
            resultColumn.set(destination, newTrue);
        } else if (newFalse == 0) {
            // we may have gone to zero, in which case it must be nulled out
            if (resultColumn.getUnsafe(destination) == 0) {
                modified = true;
                resultColumn.set(destination, QueryConstants.NULL_LONG);
            }
        }

        return modified;
    }

    private boolean modifyChunk(ObjectChunk<Boolean, ? extends Values> preValues, ObjectChunk<Boolean, ? extends Values> postValues, long destination, int chunkStart, int chunkSize) {
        final MutableInt preChunkNull = new MutableInt(0);
        final MutableInt preChunkTrue = new MutableInt(0);
        final MutableInt preChunkFalse = new MutableInt(0);
        sumChunk(preValues, chunkStart, chunkSize, preChunkNull, preChunkTrue, preChunkFalse);

        final MutableInt postChunkNull = new MutableInt(0);
        final MutableInt postChunkTrue = new MutableInt(0);
        final MutableInt postChunkFalse = new MutableInt(0);
        sumChunk(postValues, chunkStart, chunkSize, postChunkNull, postChunkTrue, postChunkFalse);

        final boolean trueModified = preChunkTrue.intValue() != postChunkTrue.intValue();
        final boolean falseModified = preChunkFalse.intValue() != postChunkFalse.intValue();

        if (!trueModified && !falseModified) {
            return false;
        }

        long newFalse = -1;
        long oldFalse = -1;
        if (falseModified) {
            oldFalse = falseCount.getUnsafe(destination);
            if (oldFalse == QueryConstants.NULL_LONG) {
                oldFalse = 0;
            }
            newFalse = oldFalse + postChunkFalse.intValue() - preChunkFalse.intValue();
            falseCount.set(destination, newFalse);
        }

        if (trueModified) {
            final long oldTrue = resultColumn.getUnsafe(destination);
            final long newTrue = NullSafeAddition.plusLong(oldTrue, postChunkTrue.intValue() - preChunkTrue.intValue());
            resultColumn.set(destination, newTrue);
            if (newTrue > 0) {
                // if we're still above zero, then we are done
                return true;
            }
        }

        if (oldFalse > 0 && newFalse > 0) {
            // we are still positive, so the result will not change
            return trueModified;
        }

        // if true was modified, and we got to this point; then it must be zero
        final boolean hasTrue = !trueModified && hasTrue(resultColumn, destination);
        if (hasTrue) {
            return false;
        }

        // we only hit the first two cases when we have a false change
        if (oldFalse > 0 && newFalse == 0) {
            // set to null, because we had a false but do not anymore
            resultColumn.set(destination, QueryConstants.NULL_LONG);
            return true;
        }
        else if (oldFalse == 0 && newFalse > 0) {
            // set to 0, because we were null; but now are true
            resultColumn.set(destination, 0L);
            return true;
        } else if (trueModified && !falseModified) {
            newFalse = falseCount.getUnsafe(destination);
            resultColumn.set(destination, newFalse > 0 ? 0 : QueryConstants.NULL_LONG);
            return true;
        }

        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        falseCount.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(name, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }
}
