/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.LongConsumer;

/**
 * The TreeMapSource is a ColumnSource used only for testing; not in live code.
 *
 * It boxes all of its objects, and maps Long key values to the data values.
 */
public class TreeMapSource<T> extends AbstractColumnSource<T> {
    private long lastAdditionTime = LogicalClock.DEFAULT.currentStep();
    protected final TreeMap<Long, T> data = new TreeMap<>();
    private TreeMap<Long, T> prevData = new TreeMap<>();

    public TreeMapSource(Class<T> type) {
        super(type);
    }

    public TreeMapSource(Class<T> type, RowSet rowSet, T[] data) {
        // noinspection unchecked
        super(convertType(type));
        add(rowSet, data);
        prevData = this.data;
    }

    private static Class convertType(Class type) {
        if (type == Boolean.class || type.isPrimitive()) {
            return type;
        }
        if (io.deephaven.util.type.TypeUtils.getUnboxedType(type) == null) {
            return type;
        } else {
            return io.deephaven.util.type.TypeUtils.getUnboxedType(type);
        }
    }

    public synchronized void add(final RowSet rowSet, T[] vs) {
        if (groupToRange != null) {
            setGroupToRange(null);
        }

        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (currentStep != lastAdditionTime) {
            prevData = new TreeMap<>(this.data);
            lastAdditionTime = currentStep;
        }
        if (rowSet.size() != vs.length) {
            throw new IllegalArgumentException(
                    "RowSet=" + rowSet + ", data(" + vs.length + ")=" + Arrays.toString(vs));
        }

        rowSet.forAllRowKeys(new LongConsumer() {
            private final MutableInt ii = new MutableInt(0);

            @Override
            public void accept(final long v) {
                data.put(v, vs[ii.intValue()]);
                ii.increment();
            }
        });
    }

    public synchronized void remove(RowSet rowSet) {
        if (groupToRange != null) {
            setGroupToRange(null);
        }

        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (currentStep != lastAdditionTime) {
            prevData = new TreeMap<>(this.data);
            lastAdditionTime = currentStep;
        }
        for (final RowSet.Iterator iterator = rowSet.iterator(); iterator.hasNext();) {
            this.data.remove(iterator.nextLong());
        }
    }

    public synchronized void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta) {
        if (groupToRange != null) {
            setGroupToRange(null);
        }

        // Note: moving to the right, we need to start with rightmost data first.
        final long dir = shiftDelta > 0 ? -1 : 1;
        final long len = endKeyInclusive - startKeyInclusive + 1;
        for (long offset = dir < 0 ? len - 1 : 0; dir < 0 ? offset >= 0 : offset < len; offset += dir) {
            data.put(startKeyInclusive + offset + shiftDelta, data.remove(startKeyInclusive + offset));
        }
    }

    @Override
    public synchronized T get(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        // If a test asks for a non-existent positive row key something is wrong.
        // We have to accept negative values, because e.g. a join may find no matching right key, in which case it
        // has an empty row redirection entry that just gets passed through to the inner column source as -1.
        final T retVal = data.get(rowKey);
        if (retVal == null && !data.containsKey(rowKey)) {
            throw new IllegalStateException("Asking for a non-existent row key: " + rowKey);
        }
        return retVal;
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public Boolean getBoolean(long index) {
        return (Boolean) get(index);
    }

    @Override
    public byte getByte(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Byte) get(index));
    }

    @Override
    public char getChar(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Character) get(index));
    }

    @Override
    public double getDouble(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Double) get(index));
    }

    @Override
    public float getFloat(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Float) get(index));
    }

    @Override
    public int getInt(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Integer) get(index));
    }

    @Override
    public long getLong(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Long) get(index));
    }

    @Override
    public short getShort(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Short) get(index));
    }

    @Override
    synchronized public T getPrev(long index) {
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (currentStep != lastAdditionTime) {
            prevData = new TreeMap<>(this.data);
            lastAdditionTime = currentStep;
        }
        return prevData.get(index);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        return (Boolean) getPrev(index);
    }

    @Override
    public byte getPrevByte(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Byte) getPrev(index));
    }

    @Override
    public char getPrevChar(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Character) getPrev(index));
    }

    @Override
    public double getPrevDouble(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Double) getPrev(index));
    }

    @Override
    public float getPrevFloat(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Float) getPrev(index));
    }

    @Override
    public int getPrevInt(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Integer) getPrev(index));
    }

    @Override
    public long getPrevLong(long index) {
        return io.deephaven.util.type.TypeUtils.unbox((Long) getPrev(index));
    }

    @Override
    public short getPrevShort(long index) {
        return TypeUtils.unbox((Short) getPrev(index));
    }

    @Override
    public void startTrackingPrevValues() {}
}
