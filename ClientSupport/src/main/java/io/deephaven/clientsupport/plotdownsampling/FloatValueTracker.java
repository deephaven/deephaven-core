/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharValueTracker and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.clientsupport.plotdownsampling;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.FloatArraySource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * See ReplicateDownsamplingValueTrackers for notes on changing this file.
 */
public final class FloatValueTracker extends ValueTracker {
    private final FloatArraySource source = new FloatArraySource();

    @Override
    protected void ensureCapacity(int bucketCount) {
        super.ensureCapacity(bucketCount);
        source.ensureCapacity(bucketCount * 2);
    }

    private float minValue(int offset) {
        return source.getUnsafe(minValuePosition(offset));
    }
    private float maxValue(int offset) {
        return source.getUnsafe(maxValuePosition(offset));
    }

    private void setMinValue(int offset, float value) {
        source.set(minValuePosition(offset), value);
    }
    private void setMaxValue(int offset, float value) {
        source.set(maxValuePosition(offset), value);
    }

    @Override
    public void append(int offset, long rowIndex, Chunk<? extends Attributes.Values> valuesChunk, int indexInChunk, @Nullable Index nulls) {
        final float val = valuesChunk.asFloatChunk().get(indexInChunk);
        if (val == NULL_FLOAT) {
            if (nulls != null) {
                nulls.insert(rowIndex);
            }
            return;
        }

        // if max and min indexes are null, then this is the first non-null value, so we always want it
        final boolean first = maxIndex(offset) == QueryConstants.NULL_LONG;
        Assert.eq(first, "first", minIndex(offset) == QueryConstants.NULL_LONG, "minIndex(" + offset +") == QueryConstants.NULL_LONG");

        if (first || val > maxValue(offset)) {
            setMaxValue(offset, val);
            setMaxIndex(offset, rowIndex);
            maxValueValid(offset, true);
        }
        if (first || val < minValue(offset)) {
            setMinValue(offset, val);
            setMinIndex(offset, rowIndex);
            minValueValid(offset, true);
        }
    }

    @Override
    public void update(int offset, long rowIndex, Chunk<? extends Attributes.Values> valuesChunk, int indexInChunk, @Nullable Index nulls) {
        float val = valuesChunk.asFloatChunk().get(indexInChunk);
        if (val == NULL_FLOAT) {
            if (nulls != null) {
                nulls.insert(rowIndex);
            }
            // whether or not we are tracking nulls, if the row was our max/min, mark the value as invalid so we can rescan
            if (rowIndex == maxIndex(offset)) {
                maxValueValid(offset, false);// invalid will force a rescan
            }
            if (rowIndex == minIndex(offset)) {
                minValueValid(offset, false);
            }
        } else {
            if (nulls != null) {
                nulls.remove(rowIndex);
            }

            long maxIndex = maxIndex(offset);
            if (rowIndex == maxIndex) {
                if (val >= maxValue(offset)) {
                    // This is still the max, but update the value
                    setMaxValue(offset, val);
                } else {
                    // May no longer be the max, rescan to check - leave old value in place for another update to compare
                    // against it or replace in rescan
                    maxValueValid(offset, false);
                }
            } else {
                // if the new val is bigger than before, or the old value was null,
                if (val > maxValue(offset) || maxIndex == QueryConstants.NULL_LONG) {
                    // this is the new max
                    setMaxValue(offset, val);
                    setMaxIndex(offset, rowIndex);
                    maxValueValid(offset, true);
                }
            }
            long minIndex = minIndex(offset);
            if (rowIndex == minIndex) {
                if (val <= minValue(offset)) {
                    setMinValue(offset, val);
                } else {
                    minValueValid(offset, false);
                }
            } else {
                // if the new val is smaller than before, or the old value was null,
                if (val < minValue(offset) || minIndex == QueryConstants.NULL_LONG) {
                    // this is the new min
                    setMinValue(offset, val);
                    setMinIndex(offset, rowIndex);
                    minValueValid(offset, true);
                }
            }
        }

    }

    @Override
    public void validate(int offset, long rowIndex, Chunk<? extends Attributes.Values> valuesChunk, int indexInChunk, @Nullable Index nulls) {
        float val = valuesChunk.asFloatChunk().get(indexInChunk);
        if (val == NULL_FLOAT) {
            // can't check if our min/max is valid, or anything about positions, only can confirm that this index is in nulls
            if (nulls != null) {
                Assert.eqTrue(nulls.containsRange(rowIndex, rowIndex), "nulls.containsRange(rowIndex, rowIndex)");
            }
            return;
        }
        // else we found a non-null value in the current bucket, so verify that we are valid, that the
        // value makes sense with what we already found
        Assert.eqTrue(minValueValid(offset), "minValueValid(offset)");
        Assert.eqTrue(maxValueValid(offset), "maxValueValid(offset)");

        if (maxIndex(offset) == rowIndex) {
            Assert.eq(val, "val", maxValue(offset), "maxValue(offset)");
        } else {
            Assert.leq(val, "val", maxValue(offset), "maxValue(offset)");
        }
        if (minIndex(offset) == rowIndex) {
            Assert.eq(val, "val", minValue(offset), "minValue(offset)");
        } else {
            Assert.geq(val, "val", minValue(offset), "minValue(offset)");
        }
    }

    @Override
    public String toString(int offset) {
        return "FloatValueTracker("+offset+") { max=" + maxValue(offset) + ", min=" + minValue(offset) + " }";
    }
}
