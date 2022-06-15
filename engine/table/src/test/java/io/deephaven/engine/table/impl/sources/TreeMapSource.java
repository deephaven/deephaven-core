/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
    public Boolean getBoolean(long rowKey) {
        return (Boolean) get(rowKey);
    }

    @Override
    public byte getByte(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Byte) get(rowKey));
    }

    @Override
    public char getChar(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Character) get(rowKey));
    }

    @Override
    public double getDouble(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Double) get(rowKey));
    }

    @Override
    public float getFloat(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Float) get(rowKey));
    }

    @Override
    public int getInt(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Integer) get(rowKey));
    }

    @Override
    public long getLong(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Long) get(rowKey));
    }

    @Override
    public short getShort(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Short) get(rowKey));
    }

    @Override
    synchronized public T getPrev(long rowKey) {
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (currentStep != lastAdditionTime) {
            prevData = new TreeMap<>(this.data);
            lastAdditionTime = currentStep;
        }
        return prevData.get(rowKey);
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        return (Boolean) getPrev(rowKey);
    }

    @Override
    public byte getPrevByte(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Byte) getPrev(rowKey));
    }

    @Override
    public char getPrevChar(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Character) getPrev(rowKey));
    }

    @Override
    public double getPrevDouble(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Double) getPrev(rowKey));
    }

    @Override
    public float getPrevFloat(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Float) getPrev(rowKey));
    }

    @Override
    public int getPrevInt(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Integer) getPrev(rowKey));
    }

    @Override
    public long getPrevLong(long rowKey) {
        return io.deephaven.util.type.TypeUtils.unbox((Long) getPrev(rowKey));
    }

    @Override
    public short getPrevShort(long rowKey) {
        return TypeUtils.unbox((Short) getPrev(rowKey));
    }

    @Override
    public void startTrackingPrevValues() {}
}
