package io.deephaven.clientsupport.plotdownsampling;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.chunk.Chunk;
import org.jetbrains.annotations.Nullable;

public final class ObjectValueTracker<T extends Comparable<T>> extends ValueTracker {
    private final ObjectArraySource<T> source;

    public ObjectValueTracker(ColumnSource<?> columnSource) {
        Require.eqTrue(Comparable.class.isAssignableFrom(columnSource.getType()), "Comparable.class.isAssignableFrom(columnSource.getType())");
        source = new ObjectArraySource(columnSource.getType(), columnSource.getComponentType());
    }

    @Override
    protected void ensureCapacity(int bucketCount) {
        super.ensureCapacity(bucketCount);
        source.ensureCapacity(bucketCount * 2);
    }

    private T minValue(int offset) {
        return source.getUnsafe(minValuePosition(offset));
    }
    private T maxValue(int offset) {
        return source.getUnsafe(maxValuePosition(offset));
    }

    private void setMinValue(int offset, T value) {
        source.set(minValuePosition(offset), value);
    }
    private void setMaxValue(int offset, T value) {
        source.set(maxValuePosition(offset), value);
    }

    @Override
    public void append(int offset, long rowKey, Chunk<? extends Values> valuesChunk, int indexInChunk, @Nullable WritableRowSet nulls) {
        final T val = valuesChunk.<T>asObjectChunk().get(indexInChunk);
        if (val == null) {
            if (nulls != null) {
                nulls.insert(rowKey);
            }
            return;
        }

        // if max and min indexes are null, then this is the first non-null value, so we always want it
        final boolean first = maxIndex(offset) == QueryConstants.NULL_LONG;
        Assert.eq(first, "first", minIndex(offset) == QueryConstants.NULL_LONG, "minIndex(" + offset +") == QueryConstants.NULL_LONG");

        if (first || val.compareTo(maxValue(offset)) > 0) {
            setMaxValue(offset, val);
            setMaxIndex(offset, rowKey);
            maxValueValid(offset, true);
        }
        if (first || val.compareTo(minValue(offset)) < 0) {
            setMinValue(offset, val);
            setMinIndex(offset, rowKey);
            minValueValid(offset, true);
        }
    }

    @Override
    public void update(int offset, long rowKey, Chunk<? extends Values> valuesChunk, int indexInChunk, @Nullable WritableRowSet nulls) {
        T val = valuesChunk.<T>asObjectChunk().get(indexInChunk);
        if (val == null) {
            if (nulls != null) {
                nulls.insert(rowKey);
            } else {
                // whether or not we are tracking nulls, if the row was our max/min, mark the row key as garbage and the
                // value as invalid
                if (rowKey == maxIndex(offset)) {
                    maxValueValid(offset, false);
                    setMaxIndex(offset, QueryConstants.NULL_LONG);
                }
                if (rowKey == minIndex(offset)) {
                    minValueValid(offset, false);
                    setMinIndex(offset, QueryConstants.NULL_LONG);
                }
            }
        } else {
            if (nulls != null) {
                nulls.remove(rowKey);
            }

            if (rowKey == maxIndex(offset)) {
                if (val.compareTo(maxValue(offset)) >= 0) {
                    // This is still the max, but update the value
                    setMaxValue(offset, val);
                } else {
                    // May no longer be the max, rescan to check - leave old value in place for another update to compare
                    // against it or replace in rescan
                    maxValueValid(offset, false);
                }
            } else {
                if (val.compareTo(maxValue(offset)) > 0) {
                    // this is the new max
                    setMaxValue(offset, val);
                    setMaxIndex(offset, rowKey);
                    maxValueValid(offset, true);
                }
            }
            if (rowKey == minIndex(offset)) {
                if (val.compareTo(minValue(offset)) <= 0) {
                    setMinValue(offset, val);
                } else {
                    minValueValid(offset, false);
                }
            } else {
                if (val.compareTo(minValue(offset)) < 0) {
                    setMinValue(offset, val);
                    setMinIndex(offset, rowKey);
                    minValueValid(offset, true);
                }
            }
        }
    }

    @Override
    public void validate(int offset, long rowKey, Chunk<? extends Values> valuesChunk, int indexInChunk, @Nullable RowSet nulls) {
        T val = valuesChunk.<T>asObjectChunk().get(indexInChunk);
        if (val == null) {
            // can't check if our min/max is valid, or anything about positions, only can confirm that this row key is in
            // nulls
            if (nulls != null) {
                Assert.eqTrue(nulls.containsRange(rowKey, rowKey), "nulls.containsRange(rowIndex, rowIndex)");
            }
            return;
        }
        Assert.eqTrue(minValueValid(offset), "minValueValid(offset)");
        Assert.eqTrue(maxValueValid(offset), "maxValueValid(offset)");

        if (maxIndex(offset) == rowKey) {
            Assert.eq(val.compareTo(maxValue(offset)), "val.compareTo(maxValue(offset))", 0, "0");
            Assert.eq(val, "val", maxValue(offset), "maxValue(offset)");
        } else {
            Assert.leq(val.compareTo(maxValue(offset)), "val.compareTo(maxValue(offset))", 0, "0");
        }
        if (minIndex(offset) == rowKey) {
            Assert.eq(val.compareTo(minValue(offset)), "val.compareTo(minValue(offset))", 0, "0");
            Assert.eq(val, "val", minValue(offset), "minValue(offset)");
        } else {
            Assert.geq(val.compareTo(minValue(offset)), "val.compareTo(minValue(offset))", 0, "0");
        }
    }

    @Override
    public String toString(int offset) {
        return "ObjectValueTracker("+offset+") { max=" + maxValue(offset) + ", min=" + minValue(offset) + " }";
    }
}
