//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.stream.Collectors;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * A column source that returns null for all keys. Trivially "writable" since it can only contain null values.
 */
public final class NullValueColumnSource<T> extends AbstractColumnSource<T>
        implements InMemoryColumnSource, RowKeyAgnosticChunkSource<Values>,
        WritableColumnSource<T> {

    private static final KeyedObjectKey.Basic<Pair<Class<?>, Class<?>>, NullValueColumnSource<?>> KEY_TYPE =
            new KeyedObjectKey.Basic<>() {
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

    public static <T2> NullValueColumnSource<T2> getInstance(Class<T2> clazz, @Nullable final Class<?> elementType) {
        // noinspection unchecked,rawtypes
        return (NullValueColumnSource) INSTANCES.putIfAbsent(new Pair<>(clazz, elementType),
                p -> new NullValueColumnSource<>(clazz, elementType));
    }

    public static LinkedHashMap<String, ColumnSource<?>> createColumnSourceMap(TableDefinition definition) {
        return definition.getColumnStream().collect(Collectors.toMap(
                ColumnDefinition::getName,
                c -> getInstance(c.getDataType(), c.getComponentType()),
                Assert::neverInvoked,
                LinkedHashMap::new));
    }

    private NullValueColumnSource(Class<T> type, @Nullable final Class<?> elementType) {
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
        return NULL_BYTE;
    }

    @Override
    public char getChar(long rowKey) {
        return NULL_CHAR;
    }

    @Override
    public double getDouble(long rowKey) {
        return NULL_DOUBLE;
    }

    @Override
    public float getFloat(long rowKey) {
        return NULL_FLOAT;
    }

    @Override
    public int getInt(long rowKey) {
        return NULL_INT;
    }

    @Override
    public long getLong(long rowKey) {
        return NULL_LONG;
    }

    @Override
    public short getShort(long rowKey) {
        return NULL_SHORT;
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
        return NULL_BYTE;
    }

    @Override
    public char getPrevChar(long rowKey) {
        return NULL_CHAR;
    }

    @Override
    public double getPrevDouble(long rowKey) {
        return NULL_DOUBLE;
    }

    @Override
    public float getPrevFloat(long rowKey) {
        return NULL_FLOAT;
    }

    @Override
    public int getPrevInt(long rowKey) {
        return NULL_INT;
    }

    @Override
    public long getPrevLong(long rowKey) {
        return NULL_LONG;
    }

    @Override
    public short getPrevShort(long rowKey) {
        return NULL_SHORT;
    }

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
            return (ColumnSource<ALTERNATE_DATA_TYPE>) BOOL_AS_BYTE_SOURCE;
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

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull LongChunk<? extends RowKeys> keys) {
        // note that we do not need to look for RowSequence.NULL_ROW_KEY; all values are null
        destination.setSize(keys.size());
        destination.fillWithNullValue(0, keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, destination, keys);
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    private static void throwUnsupported() {
        throw new UnsupportedOperationException("NullValueColumnSource cannot contain non-null values");
    }

    @Override
    public void set(final long key, final T value) {
        if (value != null) {
            throwUnsupported();
        }
    }

    @Override
    public void set(final long key, final byte value) {
        if (value != NULL_BYTE) {
            throwUnsupported();
        }
    }

    @Override
    public void set(final long key, final char value) {
        if (value != NULL_CHAR) {
            throwUnsupported();
        }
    }

    @Override
    public void set(final long key, final double value) {
        if (value != NULL_DOUBLE) {
            throwUnsupported();
        }
    }

    @Override
    public void set(final long key, final float value) {
        if (value != NULL_FLOAT) {
            throwUnsupported();
        }
    }

    @Override
    public void set(final long key, final int value) {
        if (value != NULL_INT) {
            throwUnsupported();
        }
    }

    @Override
    public void set(final long key, final long value) {
        if (value != NULL_LONG) {
            throwUnsupported();
        }
    }

    @Override
    public void set(final long key, final short value) {
        if (value != NULL_SHORT) {
            throwUnsupported();
        }
    }

    @Override
    public void setNull(final long key) {}

    @Override
    public void setNull(@NotNull final RowSequence orderedKeys) {}

    @Override
    public void ensureCapacity(final long capacity, final boolean nullFilled) {}

    @Override
    public FillFromContext makeFillFromContext(int chunkCapacity) {
        return DEFAULT_FILL_FROM_INSTANCE;
    }

    @Override
    public void fillFromChunk(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        // Assume all values in src are null, which will be true for any correct usage.
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys) {
        // Assume all values in src are null, which will be true for any correct usage.
    }
}
