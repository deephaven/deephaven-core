/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.util.ShiftData;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public abstract class SingleValueColumnSource<T> extends AbstractColumnSource<T>
        implements WritableColumnSource<T>, ChunkSink<Values>, ShiftData.ShiftCallback, Serializable {

    protected transient long changeTime;
    protected boolean isTrackingPrevValues;

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
    }

    SingleValueColumnSource(Class<T> type) {
        super(type);
    }

    @Override
    public final void startTrackingPrevValues() {
        isTrackingPrevValues = true;
    }

    @Override
    public void shift(long start, long end, long offset) {}

    public static <T> SingleValueColumnSource<T> getSingleValueColumnSource(Class<T> type) {
        SingleValueColumnSource<?> result;
        if (type == Byte.class || type == byte.class) {
            result = new ByteSingleValueSource();
        } else if (type == Character.class || type == char.class) {
            result = new CharacterSingleValueSource();
        } else if (type == Double.class || type == double.class) {
            result = new DoubleSingleValueSource();
        } else if (type == Float.class || type == float.class) {
            result = new FloatSingleValueSource();
        } else if (type == Integer.class || type == int.class) {
            result = new IntegerSingleValueSource();
        } else if (type == Long.class || type == long.class) {
            result = new LongSingleValueSource();
        } else if (type == Short.class || type == short.class) {
            result = new ShortSingleValueSource();
        } else {
            result = new ObjectSingleValueSource<>(type);
        }
        // noinspection unchecked
        return (SingleValueColumnSource<T>) result;
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    public void set(char value) {
        throw new UnsupportedOperationException();
    }

    public void set(byte value) {
        throw new UnsupportedOperationException();
    }

    public void set(double value) {
        throw new UnsupportedOperationException();
    }

    public void set(float value) {
        throw new UnsupportedOperationException();
    }

    public void set(short value) {
        throw new UnsupportedOperationException();
    }

    public void set(long value) {
        throw new UnsupportedOperationException();
    }

    public void set(int value) {
        throw new UnsupportedOperationException();
    }

    public void set(T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void ensureCapacity(long capacity, boolean nullFilled) {
        // Do nothing
    }

    @Override
    public FillFromContext makeFillFromContext(int chunkCapacity) {
        return DEFAULT_FILL_FROM_INSTANCE;
    }
}
