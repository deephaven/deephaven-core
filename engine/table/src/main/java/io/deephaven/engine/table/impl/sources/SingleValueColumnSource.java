//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class SingleValueColumnSource<T> extends AbstractColumnSource<T>
        implements WritableColumnSource<T>, ChunkSink<Values>, InMemoryColumnSource,
        RowKeyAgnosticChunkSource<Values> {

    protected transient long changeTime;
    protected boolean isTrackingPrevValues;

    SingleValueColumnSource(@NotNull final Class<T> type) {
        this(type, null);
    }

    SingleValueColumnSource(@NotNull final Class<T> type, @Nullable final Class<?> elementType) {
        super(type, elementType);
    }

    @Override
    public final void startTrackingPrevValues() {
        isTrackingPrevValues = true;
    }

    public static <T> SingleValueColumnSource<T> getSingleValueColumnSource(Class<T> type) {
        return getSingleValueColumnSource(type, null);
    }

    public static <T> SingleValueColumnSource<T> getSingleValueColumnSource(Class<T> type, Class<?> componentType) {
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
        } else if (type == Boolean.class || type == boolean.class) {
            result = new BooleanSingleValueSource();
        } else {
            result = new ObjectSingleValueSource<>(type, componentType);
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

    public void setNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void setNull(long key) {
        setNull();
    }

    @Override
    public final void setNull(RowSequence rowSequence) {
        if (!rowSequence.isEmpty()) {
            setNull();
        }
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
