/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.utils.ShiftData;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

@AbstractColumnSource.IsSerializable(value = true)
public abstract class SingleValueColumnSource<T> extends AbstractColumnSource<T>
    implements WritableSource<T>, WritableChunkSink<Attributes.Values>, ShiftData.ShiftCallback,
    Serializable {

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

    public static <T> SingleValueColumnSource getSingleValueColumnSource(Class<T> type) {
        if (type == Byte.class || type == byte.class) {
            return new ByteSingleValueSource();
        } else if (type == Character.class || type == char.class) {
            return new CharacterSingleValueSource();
        } else if (type == Double.class || type == double.class) {
            return new DoubleSingleValueSource();
        } else if (type == Float.class || type == float.class) {
            return new FloatSingleValueSource();
        } else if (type == Integer.class || type == int.class) {
            return new IntegerSingleValueSource();
        } else if (type == Long.class || type == long.class) {
            return new LongSingleValueSource();
        } else if (type == Short.class || type == short.class) {
            return new ShortSingleValueSource();
        } else {
            return new ObjectSingleValueSource<>(type);
        }
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
