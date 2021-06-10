/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ShiftData;
import org.jetbrains.annotations.NotNull;

/**
 * A column source that returns null for all keys.
 */
@AbstractColumnSource.IsSerializable(value = true)
public class NullValueColumnSource<T> extends AbstractColumnSource<T> implements ShiftData.ShiftCallback {
    private static final KeyedObjectKey.Basic<Class, NullValueColumnSource> KEY_TYPE = new KeyedObjectKey.Basic<Class, NullValueColumnSource>() {
        @Override
        public Class getKey(NullValueColumnSource columnSource) {
            return columnSource.getType();
        }
    };

    private static final KeyedObjectHashMap<Class, NullValueColumnSource> INSTANCES = new KeyedObjectHashMap<>(KEY_TYPE);
    private static final ColumnSource<Byte> BOOL_AS_BYTE_SOURCE = new BooleanAsByteColumnSource(getInstance(boolean.class));

    public static <T2> NullValueColumnSource<T2> getInstance(Class<T2> clazz) {
        //noinspection unchecked
        return INSTANCES.putIfAbsent(clazz, NullValueColumnSource::new);
    }

    private NullValueColumnSource(Class<T> type) {
        super(type);
    }

    @Override
    public void startTrackingPrevValues() {
        // Do nothing.
    }

    @Override
    public T get(long index) {
        return null;
    }

    @Override
    public Boolean getBoolean(long index) {
        return null;
    }

    @Override
    public byte getByte(long index) {
        return QueryConstants.NULL_BYTE;
    }

    @Override
    public char getChar(long index) {
        return QueryConstants.NULL_CHAR;
    }

    @Override
    public double getDouble(long index) {
        return QueryConstants.NULL_DOUBLE;
    }

    @Override
    public float getFloat(long index) {
        return QueryConstants.NULL_FLOAT;
    }

    @Override
    public int getInt(long index) {
        return QueryConstants.NULL_INT;
    }

    @Override
    public long getLong(long index) {
        return QueryConstants.NULL_LONG;
    }

    @Override
    public short getShort(long index) {
        return QueryConstants.NULL_SHORT;
    }

    @Override
    public T getPrev(long index) {
        return null;
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        return null;
    }

    @Override
    public byte getPrevByte(long index) {
        return QueryConstants.NULL_BYTE;
    }

    @Override
    public char getPrevChar(long index) {
        return QueryConstants.NULL_CHAR;
    }

    @Override
    public double getPrevDouble(long index) {
        return QueryConstants.NULL_DOUBLE;
    }

    @Override
    public float getPrevFloat(long index) {
        return QueryConstants.NULL_FLOAT;
    }

    @Override
    public int getPrevInt(long index) {
        return QueryConstants.NULL_INT;
    }

    @Override
    public long getPrevLong(long index) {
        return QueryConstants.NULL_LONG;
    }

    @Override
    public short getPrevShort(long index) {
        return QueryConstants.NULL_SHORT;
    }

    @Override
    public void shift(long start, long end, long offset) {
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return true;
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if((type == Boolean.class || type == boolean.class) &&
           (alternateDataType == byte.class || alternateDataType == Byte.class)) {
            //noinspection unchecked
            return (ColumnSource)BOOL_AS_BYTE_SOURCE;
        }

        return getInstance(alternateDataType);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
        destination.setSize(orderedKeys.intSize());
        destination.fillWithNullValue(0, orderedKeys.intSize());
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
        fillChunk(context, destination, orderedKeys);
    }
}
