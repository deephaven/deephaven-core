/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.util.NullSafeAddition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Iterative Boolean Sum. Result is the number of {@code true} values, or {@code null} if all values are {@code null}.
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
     * @return the value, before the update
     */
    private static long getAndAdd(LongArraySource source, long destPos, long count) {
        final long oldValue = source.getUnsafe(destPos);
        if (oldValue == NULL_LONG || oldValue == 0) {
            Assert.gtZero(count, "count");
            source.set(destPos, count);
            return 0;
        } else {
            source.set(destPos, oldValue + count);
            return oldValue;
        }
    }

    /**
     * Add to the value at the given position, return the new value.
     *
     * @param source the column source to update
     * @param destPos the position within the column source
     * @return the value, after the update
     */
    private static long addAndGet(LongArraySource source, long destPos, long count) {
        final long oldValue = source.getUnsafe(destPos);
        if (oldValue == NULL_LONG || oldValue == 0) {
            Assert.gtZero(count, "count");
            source.set(destPos, count);
            return count;
        } else {
            final long newValue = oldValue + count;
            source.set(destPos, newValue);
            return newValue;
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
        return value != NULL_LONG && value > 0;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean hasTrue(long destPos) {
        return hasValue(resultColumn, destPos);
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
            getAndAdd(resultColumn, destination, chunkTrue.intValue());
            modified = true;
        }
        if (chunkFalse.intValue() > 0) {
            final long oldFalse = getAndAdd(falseCount, destination, chunkFalse.intValue());
            if (oldFalse == 0 && chunkTrue.intValue() == 0 && !hasTrue(destination)) {
                resultColumn.set(destination, 0L);
                modified = true;
            }
        }
        // New or resurrected slots are adds, not modifies, and will already have null in the result, so we never need
        // to update the result to null or report a mod if there are only nulls in the chunk.
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
            newFalse = addAndGet(falseCount, destination, -chunkFalse.intValue());
        }
        if (chunkTrue.intValue() > 0) {
            final long oldTrue = resultColumn.getUnsafe(destination);
            Assert.gtZero(oldTrue, "oldTrue");
            long newTrue = oldTrue - chunkTrue.intValue();
            if (newTrue == 0) {
                // we are zeroing ourselves out
                if (newFalse < 0) {
                    newFalse = falseCount.getUnsafe(destination);
                }
                if (newFalse == NULL_LONG || newFalse == 0) {
                    newTrue = NULL_LONG;
                }
            }
            resultColumn.set(destination, newTrue);
            return true;
        } else if (newFalse == 0) {
            // we may have gone to zero, in which case it must be nulled out
            if (resultColumn.getUnsafe(destination) == 0) {
                resultColumn.set(destination, NULL_LONG);
                modified = true;
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
        if (falseModified) {
            newFalse = addAndGet(falseCount, destination, postChunkFalse.intValue() - preChunkFalse.intValue());
        }

        if (trueModified) {
            final long oldTrue = resultColumn.getUnsafe(destination);
            long newTrue = NullSafeAddition.plusLong(oldTrue, postChunkTrue.intValue() - preChunkTrue.intValue());
            if (newTrue == 0) {
                // we are zeroing ourselves out
                if (!falseModified) {
                    newFalse = falseCount.getUnsafe(destination);
                }
                if (newFalse == NULL_LONG || newFalse == 0) {
                    newTrue = NULL_LONG;
                }
            }
            resultColumn.set(destination, newTrue);
            return true;
        }

        if (newFalse == 0) {
            resultColumn.set(destination, NULL_LONG);
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
