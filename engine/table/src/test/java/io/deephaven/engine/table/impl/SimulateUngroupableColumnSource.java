//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupableColumnSource;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;

/**
 * Wraps an array or vector source, and reports that it is ungroupable. The implementations of the ungroupable calls
 * will create garbage, etc. so it should only ever be used in tests. This makes it easier for us to cover the
 * ungroupable paths inside of {@link UngroupOperation}.
 */
class SimulateUngroupableColumnSource<T> extends AbstractColumnSource<T> implements UngroupableColumnSource {
    final ColumnSource<T> delegate;

    public SimulateUngroupableColumnSource(final ColumnSource<T> delegate) {
        super(delegate.getType(), delegate.getComponentType());
        this.delegate = delegate;
    }

    @Override
    public boolean isImmutable() {
        return delegate.isImmutable();
    }

    @Override
    public @Nullable T get(final long rowKey) {
        return delegate.get(rowKey);
    }

    @Override
    public @Nullable Boolean getBoolean(final long rowKey) {
        return delegate.getBoolean(rowKey);
    }

    @Override
    public byte getByte(final long rowKey) {
        return delegate.getByte(rowKey);
    }

    @Override
    public char getChar(final long rowKey) {
        return delegate.getChar(rowKey);
    }

    @Override
    public double getDouble(final long rowKey) {
        return delegate.getDouble(rowKey);
    }

    @Override
    public float getFloat(final long rowKey) {
        return delegate.getFloat(rowKey);
    }

    @Override
    public int getInt(final long rowKey) {
        return delegate.getInt(rowKey);
    }

    @Override
    public long getLong(final long rowKey) {
        return delegate.getLong(rowKey);
    }

    @Override
    public short getShort(final long rowKey) {
        return delegate.getShort(rowKey);
    }

    @Override
    public @Nullable T getPrev(final long rowKey) {
        return delegate.getPrev(rowKey);
    }

    @Override
    public @Nullable Boolean getPrevBoolean(final long rowKey) {
        return delegate.getPrevBoolean(rowKey);
    }

    @Override
    public byte getPrevByte(final long rowKey) {
        return delegate.getPrevByte(rowKey);
    }

    @Override
    public char getPrevChar(final long rowKey) {
        return delegate.getPrevChar(rowKey);
    }

    @Override
    public double getPrevDouble(final long rowKey) {
        return delegate.getPrevDouble(rowKey);
    }

    @Override
    public float getPrevFloat(final long rowKey) {
        return delegate.getPrevFloat(rowKey);
    }

    @Override
    public int getPrevInt(final long rowKey) {
        return delegate.getPrevInt(rowKey);
    }

    @Override
    public long getPrevLong(final long rowKey) {
        return delegate.getPrevLong(rowKey);
    }

    @Override
    public short getPrevShort(final long rowKey) {
        return delegate.getPrevShort(rowKey);
    }

    @Override
    public boolean isUngroupable() {
        return true;
    }

    @Override
    public long getUngroupedSize(final long groupRowKey) {
        final Object o = get(groupRowKey);
        if (o == null) {
            return 0;
        }
        if (o instanceof Vector) {
            return ((Vector) o).size();
        }
        return Array.getLength(o);
    }

    @Override
    public long getUngroupedPrevSize(final long groupRowKey) {
        final Object o = getPrev(groupRowKey);
        if (o == null) {
            return 0;
        }
        if (o instanceof Vector) {
            return ((Vector<?>) o).size();
        }
        return Array.getLength(o);
    }

    @Override
    public Object getUngrouped(final long groupRowKey, final int offsetInGroup) {
        return unpack(get(groupRowKey), offsetInGroup);
    }

    private static Object unpack(Object o, final int offsetInGroup) {
        if (o == null) {
            return 0;
        }
        if (o instanceof Vector) {
            o = ((Vector<?>) o).toArray();
        }
        return Array.get(o, offsetInGroup);
    }

    @Override
    public Object getUngroupedPrev(final long groupRowKey, final int offsetInGroup) {
        return unpack(getPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public Boolean getUngroupedBoolean(final long groupRowKey, final int offsetInGroup) {
        return (Boolean) getUngrouped(groupRowKey, offsetInGroup);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(final long groupRowKey, final int offsetInGroup) {
        return (Boolean) getUngroupedPrev(groupRowKey, offsetInGroup);
    }

    @Override
    public double getUngroupedDouble(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Double) unpack(get(groupRowKey), offsetInGroup));
    }

    @Override
    public double getUngroupedPrevDouble(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Double) unpack(getPrev(groupRowKey), offsetInGroup));
    }

    @Override
    public float getUngroupedFloat(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Float) unpack(get(groupRowKey), offsetInGroup));
    }

    @Override
    public float getUngroupedPrevFloat(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Float) unpack(getPrev(groupRowKey), offsetInGroup));
    }

    @Override
    public byte getUngroupedByte(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Byte) unpack(get(groupRowKey), offsetInGroup));
    }

    @Override
    public byte getUngroupedPrevByte(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Byte) unpack(getPrev(groupRowKey), offsetInGroup));
    }

    @Override
    public char getUngroupedChar(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Character) unpack(get(groupRowKey), offsetInGroup));
    }

    @Override
    public char getUngroupedPrevChar(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Character) unpack(getPrev(groupRowKey), offsetInGroup));
    }

    @Override
    public short getUngroupedShort(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Short) unpack(get(groupRowKey), offsetInGroup));
    }

    @Override
    public short getUngroupedPrevShort(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Short) unpack(getPrev(groupRowKey), offsetInGroup));
    }

    @Override
    public int getUngroupedInt(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Integer) unpack(get(groupRowKey), offsetInGroup));
    }

    @Override
    public int getUngroupedPrevInt(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Integer) unpack(getPrev(groupRowKey), offsetInGroup));
    }

    @Override
    public long getUngroupedLong(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Long) unpack(get(groupRowKey), offsetInGroup));
    }

    @Override
    public long getUngroupedPrevLong(final long groupRowKey, final int offsetInGroup) {
        return TypeUtils.unbox((Long) unpack(getPrev(groupRowKey), offsetInGroup));
    }

    @Override
    public void startTrackingPrevValues() {
        delegate.startTrackingPrevValues();
    }
}
