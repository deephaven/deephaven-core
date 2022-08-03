/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.util.ShiftData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A column source that returns null for all keys.
 */
public class NullValueColumnSource<T> extends AbstractColumnSource<T> implements ShiftData.ShiftCallback {
    private static final KeyedObjectKey.Basic<Pair<Class<?>, Class<?>>, NullValueColumnSource<?>> KEY_TYPE =
            new KeyedObjectKey.Basic<Pair<Class<?>, Class<?>>, NullValueColumnSource<?>>() {
                @Override
                public Pair<Class<?>, Class<?>> getKey(NullValueColumnSource columnSource) {
                    // noinspection unchecked,rawtypes
                    return new Pair<>(columnSource.getType(), columnSource.getComponentType());
                }
            };

    private static final KeyedObjectHashMap<Pair<Class<?>, Class<?>>, NullValueColumnSource<?>> INSTANCES =
            new KeyedObjectHashMap<>(KEY_TYPE);
    private static final ColumnSource<Byte> BOOL_AS_BYTE_SOURCE =
            new BooleanAsByteColumnSource(getInstance(Boolean.class, null));

    public static <T2> NullValueColumnSource<T2> getInstance(Class<T2> clazz, @Nullable final Class elementType) {
        // noinspection unchecked,rawtypes
        return (NullValueColumnSource) INSTANCES.putIfAbsent(new Pair<>(clazz, elementType),
                p -> new NullValueColumnSource<T2>(clazz, elementType));
    }

    public static Map<String, ColumnSource<?>> createColumnSourceMap(TableDefinition definition) {
        return definition.getColumnStream().collect(Collectors.toMap(
                ColumnDefinition::getName,
                c -> getInstance(c.getDataType(), c.getComponentType()),
                Assert::neverInvoked,
                LinkedHashMap::new));
    }

    private NullValueColumnSource(Class<T> type, @Nullable final Class elementType) {
        super(type, elementType);
    }

    @Override
    public void startTrackingPrevValues() {
        // Do nothing.
    }

    @Override
    public T get(long rowKey) {
        return null;
    }

    @Override
    public Boolean getBoolean(long rowKey) {
        return null;
    }

    @Override
    public byte getByte(long rowKey) {
        return QueryConstants.NULL_BYTE;
    }

    @Override
    public char getChar(long rowKey) {
        return QueryConstants.NULL_CHAR;
    }

    @Override
    public double getDouble(long rowKey) {
        return QueryConstants.NULL_DOUBLE;
    }

    @Override
    public float getFloat(long rowKey) {
        return QueryConstants.NULL_FLOAT;
    }

    @Override
    public int getInt(long rowKey) {
        return QueryConstants.NULL_INT;
    }

    @Override
    public long getLong(long rowKey) {
        return QueryConstants.NULL_LONG;
    }

    @Override
    public short getShort(long rowKey) {
        return QueryConstants.NULL_SHORT;
    }

    @Override
    public T getPrev(long rowKey) {
        return null;
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        return null;
    }

    @Override
    public byte getPrevByte(long rowKey) {
        return QueryConstants.NULL_BYTE;
    }

    @Override
    public char getPrevChar(long rowKey) {
        return QueryConstants.NULL_CHAR;
    }

    @Override
    public double getPrevDouble(long rowKey) {
        return QueryConstants.NULL_DOUBLE;
    }

    @Override
    public float getPrevFloat(long rowKey) {
        return QueryConstants.NULL_FLOAT;
    }

    @Override
    public int getPrevInt(long rowKey) {
        return QueryConstants.NULL_INT;
    }

    @Override
    public long getPrevLong(long rowKey) {
        return QueryConstants.NULL_LONG;
    }

    @Override
    public short getPrevShort(long rowKey) {
        return QueryConstants.NULL_SHORT;
    }

    @Override
    public void shift(long start, long end, long offset) {}

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return true;
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if ((type == Boolean.class || type == boolean.class) &&
                (alternateDataType == byte.class || alternateDataType == Byte.class)) {
            // noinspection unchecked
            return (ColumnSource) BOOL_AS_BYTE_SOURCE;
        }

        return getInstance(alternateDataType, null);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        destination.setSize(rowSequence.intSize());
        destination.fillWithNullValue(0, rowSequence.intSize());
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }
}
