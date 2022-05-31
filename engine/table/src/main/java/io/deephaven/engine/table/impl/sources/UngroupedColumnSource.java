/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource;

public abstract class UngroupedColumnSource<T> extends AbstractColumnSource<T> {
    long lastPreviousClockTick = LogicalClock.DEFAULT.currentStep();

    public void initializeBase(long base) {
        this.prevBase = base;
        this.base = base;
    }

    @Override
    public void startTrackingPrevValues() {
        // Nothing to do.
    }

    public void setBase(long base) {
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (lastPreviousClockTick != currentStep) {
            prevBase = this.base;
            lastPreviousClockTick = currentStep;
        }
        this.base = base;
    }

    public long getPrevBase() {
        if (lastPreviousClockTick == LogicalClock.DEFAULT.currentStep()) {
            return prevBase;
        } else {
            return base;
        }
    }

    protected long base;
    private long prevBase;

    protected UngroupedColumnSource(Class<T> type) {
        super(type);
    }

    public UngroupedColumnSource(Class<T> type, Class<?> elementType) {
        super(type, elementType);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static UngroupedColumnSource getColumnSource(ColumnSource column) {
        if (column instanceof AggregateColumnSource) {
            return ((AggregateColumnSource) column).ungrouped();
        }
        if (ObjectVector.class.isAssignableFrom(column.getType())) {
            if (column.getComponentType() == Byte.class || column.getComponentType() == byte.class) {
                return new UngroupedBoxedByteObjectVectorColumnSource(column);
            } else if (column.getComponentType() == Character.class || column.getComponentType() == char.class) {
                return new UngroupedBoxedCharObjectVectorColumnSource(column);
            } else if (column.getComponentType() == Double.class || column.getComponentType() == double.class) {
                return new UngroupedBoxedDoubleObjectVectorColumnSource(column);
            } else if (column.getComponentType() == Float.class || column.getComponentType() == float.class) {
                return new UngroupedBoxedFloatObjectVectorColumnSource(column);
            } else if (column.getComponentType() == Integer.class || column.getComponentType() == int.class) {
                return new UngroupedBoxedIntObjectVectorColumnSource(column);
            } else if (column.getComponentType() == Long.class || column.getComponentType() == long.class) {
                return new UngroupedBoxedLongObjectVectorColumnSource(column);
            } else if (column.getComponentType() == Short.class || column.getComponentType() == short.class) {
                return new UngroupedBoxedShortObjectVectorColumnSource(column);
            } else {
                return new UngroupedObjectVectorColumnSource(column);
            }
        } else if (Vector.class.isAssignableFrom(column.getType())) {
            if (column.getComponentType() == Byte.class || column.getComponentType() == byte.class) {
                return new UngroupedByteVectorColumnSource(column);
            } else if (column.getComponentType() == Character.class || column.getComponentType() == char.class) {
                return new UngroupedCharVectorColumnSource(column);
            } else if (column.getComponentType() == Double.class || column.getComponentType() == double.class) {
                return new UngroupedDoubleVectorColumnSource(column);
            } else if (column.getComponentType() == Float.class || column.getComponentType() == float.class) {
                return new UngroupedFloatVectorColumnSource(column);
            } else if (column.getComponentType() == Integer.class || column.getComponentType() == int.class) {
                return new UngroupedIntVectorColumnSource(column);
            } else if (column.getComponentType() == Long.class || column.getComponentType() == long.class) {
                return new UngroupedLongVectorColumnSource(column);
            } else if (column.getComponentType() == Short.class || column.getComponentType() == short.class) {
                return new UngroupedShortVectorColumnSource(column);
            } else {
                return new UngroupedObjectVectorColumnSource(column);
            }
        } else if (column.getType().isArray()) {
            if (column.getComponentType() == byte.class) {
                return new UngroupedByteArrayColumnSource(column);
            } else if (column.getComponentType() == char.class) {
                return new UngroupedCharArrayColumnSource(column);
            } else if (column.getComponentType() == double.class) {
                return new UngroupedDoubleArrayColumnSource(column);
            } else if (column.getComponentType() == float.class) {
                return new UngroupedFloatArrayColumnSource(column);
            } else if (column.getComponentType() == int.class) {
                return new UngroupedIntArrayColumnSource(column);
            } else if (column.getComponentType() == long.class) {
                return new UngroupedLongArrayColumnSource(column);
            } else if (column.getComponentType() == short.class) {
                return new UngroupedShortArrayColumnSource(column);
            } else if (column.getComponentType() == boolean.class) {
                return new UngroupedBooleanArrayColumnSource(column);
            } else if (column.getComponentType() == Byte.class) {
                return new UngroupedBoxedByteArrayColumnSource(column);
            } else if (column.getComponentType() == Character.class) {
                return new UngroupedBoxedCharArrayColumnSource(column);
            } else if (column.getComponentType() == Double.class) {
                return new UngroupedBoxedDoubleArrayColumnSource(column);
            } else if (column.getComponentType() == Float.class) {
                return new UngroupedBoxedFloatArrayColumnSource(column);
            } else if (column.getComponentType() == Integer.class) {
                return new UngroupedBoxedIntArrayColumnSource(column);
            } else if (column.getComponentType() == Long.class) {
                return new UngroupedBoxedLongArrayColumnSource(column);
            } else if (column.getComponentType() == Short.class) {
                return new UngroupedBoxedShortArrayColumnSource(column);
            } else if (column.getComponentType() == Boolean.class) {
                return new UngroupedBoxedBooleanArrayColumnSource(column);
            } else {
                return new UngroupedArrayColumnSource(column);
            }
        }
        throw new UnsupportedOperationException(
                "column.getType() = " + column.getType() + " column.getClass() = " + column.getClass());
    }
}
