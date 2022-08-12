/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.string.cache.CharSequenceUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ChunkMatchFilterFactory;
import io.deephaven.time.DateTime;
import io.deephaven.vector.*;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractColumnSource<T> implements
        ColumnSource<T>,
        DefaultChunkSource.WithPrev<Values> {

    /**
     * Minimum average run length in an {@link RowSequence} that should trigger {@link Chunk}-filling by key ranges
     * instead of individual keys.
     */
    public static final long USE_RANGES_AVERAGE_RUN_LENGTH = 5;

    protected final Class<T> type;
    protected final Class<?> componentType;

    protected volatile Map<T, RowSet> groupToRange;
    protected volatile List<ColumnSource> rowSetIndexerKey;

    protected AbstractColumnSource(@NotNull final Class<T> type) {
        this(type, Object.class);
    }

    public AbstractColumnSource(@NotNull final Class<T> type, @Nullable final Class<?> elementType) {
        if (type == boolean.class) {
            // noinspection unchecked
            this.type = (Class<T>) Boolean.class;
        } else if (type == Boolean.class) {
            this.type = type;
        } else {
            // noinspection rawtypes
            final Class unboxedType = TypeUtils.getUnboxedType(type);
            // noinspection unchecked
            this.type = unboxedType != null ? unboxedType : type;
        }
        if (type.isArray()) {
            componentType = type.getComponentType();
        } else if (Vector.class.isAssignableFrom(type)) {
            // noinspection deprecation
            if (BooleanVector.class.isAssignableFrom(type)) {
                componentType = Boolean.class;
            } else if (ByteVector.class.isAssignableFrom(type)) {
                componentType = byte.class;
            } else if (CharVector.class.isAssignableFrom(type)) {
                componentType = char.class;
            } else if (DoubleVector.class.isAssignableFrom(type)) {
                componentType = double.class;
            } else if (FloatVector.class.isAssignableFrom(type)) {
                componentType = float.class;
            } else if (IntVector.class.isAssignableFrom(type)) {
                componentType = int.class;
            } else if (LongVector.class.isAssignableFrom(type)) {
                componentType = long.class;
            } else if (ShortVector.class.isAssignableFrom(type)) {
                componentType = short.class;
            } else {
                componentType = elementType;
            }
        } else {
            componentType = null;
        }
    }

    @Override
    public Class<T> getType() {
        return type;
    }

    @Override
    public Class<?> getComponentType() {
        return componentType;
    }

    @Override
    public ColumnSource<T> getPrevSource() {
        return new PrevColumnSource<>(this);
    }

    @Override
    public List<ColumnSource> getColumnSources() {
        List<ColumnSource> localRowSetIndexerKey;
        if ((localRowSetIndexerKey = rowSetIndexerKey) == null) {
            synchronized (this) {
                if ((localRowSetIndexerKey = rowSetIndexerKey) == null) {
                    rowSetIndexerKey = localRowSetIndexerKey = Collections.singletonList(this);
                }
            }
        }
        return localRowSetIndexerKey;
    }

    @Override
    public Map<T, RowSet> getGroupToRange() {
        return groupToRange;
    }

    @Override
    public Map<T, RowSet> getGroupToRange(RowSet rowSet) {
        return groupToRange;
    }

    public final void setGroupToRange(@Nullable Map<T, RowSet> groupToRange) {
        this.groupToRange = groupToRange;
    }

    @Override
    public WritableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, RowSet mapper,
            final Object... keys) {
        final Map<T, RowSet> groupToRange = (isImmutable() || !usePrev) ? getGroupToRange(mapper) : null;
        if (groupToRange != null) {
            RowSetBuilderRandom allInMatchingGroups = RowSetFactory.builderRandom();

            if (caseInsensitive && (type == String.class)) {
                KeyedObjectHashSet keySet = new KeyedObjectHashSet<>(new CIStringKey());
                Collections.addAll(keySet, keys);

                for (Map.Entry<T, RowSet> ent : groupToRange.entrySet()) {
                    if (keySet.containsKey(ent.getKey())) {
                        allInMatchingGroups.addRowSet(ent.getValue());
                    }
                }
            } else {
                for (Object key : keys) {
                    RowSet range = groupToRange.get(key);
                    if (range != null) {
                        allInMatchingGroups.addRowSet(range);
                    }
                }
            }

            final WritableRowSet matchingValues;
            try (final RowSet matchingGroups = allInMatchingGroups.build()) {
                if (invertMatch) {
                    matchingValues = mapper.minus(matchingGroups);
                } else {
                    matchingValues = mapper.intersect(matchingGroups);
                }
            }
            return matchingValues;
        } else {
            return ChunkFilter.applyChunkFilter(mapper, this, usePrev,
                    ChunkMatchFilterFactory.getChunkFilter(type, caseInsensitive, invertMatch, keys));
        }
    }

    private static final class CIStringKey implements KeyedObjectKey<String, String> {
        @Override
        public String getKey(String s) {
            return s;
        }

        @Override
        public int hashKey(String s) {
            return (s == null) ? 0 : CharSequenceUtils.caseInsensitiveHashCode(s);
        }

        @Override
        public boolean equalKey(String s, String s2) {
            return (s == null) ? s2 == null : s.equalsIgnoreCase(s2);
        }
    }

    @Override
    public Map<T, RowSet> getValuesMapping(RowSet subRange) {
        Map<T, RowSet> result = new LinkedHashMap<>();
        final Map<T, RowSet> groupToRange = getGroupToRange();

        // if we have a grouping we can use it to avoid iterating the entire subRange. The issue is that our grouping
        // could be bigger than the RowSet we care about, by a very large margin. In this case we could be spinning
        // on RowSet intersect operations that are actually useless. This check says that if our subRange is smaller
        // than the number of keys in our grouping, we should just fetch the keys instead and generate the grouping
        // from scratch.
        boolean useGroupToRange = (groupToRange != null) && (groupToRange.size() < subRange.size());
        if (useGroupToRange) {
            for (Map.Entry<T, RowSet> typeEntry : groupToRange.entrySet()) {
                RowSet mapping = subRange.intersect(typeEntry.getValue());
                if (mapping.size() > 0) {
                    result.put(typeEntry.getKey(), mapping);
                }
            }
        } else {
            Map<T, RowSetBuilderSequential> valueToIndexSet = new LinkedHashMap<>();

            for (RowSet.Iterator it = subRange.iterator(); it.hasNext();) {
                long key = it.nextLong();
                T value = get(key);
                RowSetBuilderSequential indexes = valueToIndexSet.get(value);
                if (indexes == null) {
                    indexes = RowSetFactory.builderSequential();
                }
                indexes.appendKey(key);
                valueToIndexSet.put(value, indexes);
            }
            for (Map.Entry<T, RowSetBuilderSequential> entry : valueToIndexSet.entrySet()) {
                result.put(entry.getKey(), entry.getValue().build());
            }
        }
        return result;
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        defaultFillChunk(context, destination, rowSequence);
    }

    @VisibleForTesting
    public final void defaultFillChunk(@SuppressWarnings("unused") @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ChunkFiller filler = ChunkFiller.forChunkType(destination.getChunkType());
        if (rowSequence.getAverageRunLengthEstimate() >= USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillByRanges(this, rowSequence, destination);
        } else {
            filler.fillByIndices(this, rowSequence, destination);
        }
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        defaultFillPrevChunk(context, destination, rowSequence);
    }

    protected final void defaultFillPrevChunk(@SuppressWarnings("unused") @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ChunkFiller filler = ChunkFiller.forChunkType(destination.getChunkType());
        if (rowSequence.getAverageRunLengthEstimate() >= USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillPrevByRanges(this, rowSequence, destination);
        } else {
            filler.fillPrevByIndices(this, rowSequence, destination);
        }
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return false;
    }

    @Override
    public final <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> reinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        if (!allowsReinterpret(alternateDataType)) {
            throw new IllegalArgumentException("Unsupported reinterpret for " + getClass().getSimpleName()
                    + ": type=" + getType()
                    + ", alternateDataType=" + alternateDataType);
        }
        return doReinterpret(alternateDataType);
    }

    /**
     * Supply allowed reinterpret results. The default implementation handles the most common case to avoid code
     * duplication.
     *
     * @param alternateDataType The alternate data type
     * @return The resulting {@link ColumnSource}
     */
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        Assert.eq(getType(), "getType()", DateTime.class);
        Assert.eq(alternateDataType, "alternateDataType", long.class);
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) new UnboxedDateTimeWritableSource(
                (WritableColumnSource<DateTime>) this);
    }

    public static abstract class DefaultedMutable<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE>
            implements MutableColumnSourceGetDefaults.ForObject<DATA_TYPE> {

        protected DefaultedMutable(@NotNull final Class<DATA_TYPE> type) {
            super(type);
        }

        protected DefaultedMutable(@NotNull final Class<DATA_TYPE> type, @Nullable final Class<?> elementType) {
            super(type, elementType);
        }
    }

    public static abstract class DefaultedImmutable<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE>
            implements ImmutableColumnSourceGetDefaults.ForObject<DATA_TYPE> {

        protected DefaultedImmutable(@NotNull final Class<DATA_TYPE> type) {
            super(type);
        }

        protected DefaultedImmutable(@NotNull final Class<DATA_TYPE> type, @Nullable final Class<?> elementType) {
            super(type, elementType);
        }
    }
}
