//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharValueTracker and run "./gradlew replicateDownsamplingValueTrackers" to regenerate
//
// @formatter:off
package io.deephaven.clientsupport.plotdownsampling;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.chunk.Chunk;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * See ReplicateDownsamplingValueTrackers for notes on changing this file.
 */
public final class DoubleValueTracker extends ValueTracker {
    private final DoubleArraySource source = new DoubleArraySource();

    @Override
    protected void ensureCapacity(int bucketCount) {
        super.ensureCapacity(bucketCount);
        source.ensureCapacity(bucketCount * 2);
    }

    private double minValue(int offset) {
        return source.getUnsafe(minValuePosition(offset));
    }

    private double maxValue(int offset) {
        return source.getUnsafe(maxValuePosition(offset));
    }

    private void setMinValue(int offset, double value) {
        source.set(minValuePosition(offset), value);
    }

    private void setMaxValue(int offset, double value) {
        source.set(maxValuePosition(offset), value);
    }

    @Override
    public void append(int offset, long rowKey, Chunk<? extends Values> valuesChunk, int indexInChunk,
            @Nullable WritableRowSet nulls) {
        final double val = valuesChunk.asDoubleChunk().get(indexInChunk);
        if (val == NULL_DOUBLE) {
            if (nulls != null) {
                nulls.insert(rowKey);
            }
            return;
        }

        // if max and min indexes are null, then this is the first non-null value, so we always want it
        final boolean first = maxIndex(offset) == QueryConstants.NULL_LONG;
        Assert.eq(first, "first", minIndex(offset) == QueryConstants.NULL_LONG,
                "minIndex(" + offset + ") == QueryConstants.NULL_LONG");

        if (first || val > maxValue(offset)) {
            setMaxValue(offset, val);
            setMaxIndex(offset, rowKey);
            maxValueValid(offset, true);
        }
        if (first || val < minValue(offset)) {
            setMinValue(offset, val);
            setMinIndex(offset, rowKey);
            minValueValid(offset, true);
        }
    }

    @Override
    public void update(int offset, long rowKey, Chunk<? extends Values> valuesChunk, int indexInChunk,
            @Nullable WritableRowSet nulls) {
        double val = valuesChunk.asDoubleChunk().get(indexInChunk);
        if (val == NULL_DOUBLE) {
            if (nulls != null) {
                nulls.insert(rowKey);
            }
            // whether or not we are tracking nulls, if the row was our max/min, mark the value as invalid so we can
            // rescan
            if (rowKey == maxIndex(offset)) {
                maxValueValid(offset, false);// invalid will force a rescan
            }
            if (rowKey == minIndex(offset)) {
                minValueValid(offset, false);
            }
        } else {
            if (nulls != null) {
                nulls.remove(rowKey);
            }

            long maxIndex = maxIndex(offset);
            if (rowKey == maxIndex) {
                if (val >= maxValue(offset)) {
                    // This is still the max, but update the value
                    setMaxValue(offset, val);
                } else {
                    // May no longer be the max, rescan to check - leave old value in place for another update to
                    // compare
                    // against it or replace in rescan
                    maxValueValid(offset, false);
                }
            } else {
                // if the new val is bigger than before, or the old value was null,
                if (val > maxValue(offset) || maxIndex == QueryConstants.NULL_LONG) {
                    // this is the new max
                    setMaxValue(offset, val);
                    setMaxIndex(offset, rowKey);
                    maxValueValid(offset, true);
                }
            }
            long minIndex = minIndex(offset);
            if (rowKey == minIndex) {
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
                    setMinIndex(offset, rowKey);
                    minValueValid(offset, true);
                }
            }
        }

    }

    @Override
    public void validate(int offset, long rowKey, Chunk<? extends Values> valuesChunk, int indexInChunk,
            @Nullable RowSet nulls) {
        double val = valuesChunk.asDoubleChunk().get(indexInChunk);
        if (val == NULL_DOUBLE) {
            // can't check if our min/max is valid, or anything about positions, only can confirm that this rowKey is in
            // nulls
            if (nulls != null) {
                Assert.eqTrue(nulls.containsRange(rowKey, rowKey), "nulls.containsRange(rowIndex, rowIndex)");
            }
            return;
        }
        // else we found a non-null value in the current bucket, so verify that we are valid, that the
        // value makes sense with what we already found
        Assert.eqTrue(minValueValid(offset), "minValueValid(offset)");
        Assert.eqTrue(maxValueValid(offset), "maxValueValid(offset)");

        if (maxIndex(offset) == rowKey) {
            Assert.eq(val, "val", maxValue(offset), "maxValue(offset)");
        } else {
            Assert.leq(val, "val", maxValue(offset), "maxValue(offset)");
        }
        if (minIndex(offset) == rowKey) {
            Assert.eq(val, "val", minValue(offset), "minValue(offset)");
        } else {
            Assert.geq(val, "val", minValue(offset), "minValue(offset)");
        }
    }

    @Override
    public String toString(int offset) {
        return "DoubleValueTracker(" + offset + ") { max=" + maxValue(offset) + ", min=" + minValue(offset) + " }";
    }
}
