/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources;

import io.deephaven.base.Pair;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.utils.*;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.string.cache.CharSequenceUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.tables.dbarrays.*;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.select.ChunkFilter;
import io.deephaven.engine.v2.select.chunkfilters.ChunkMatchFilterFactory;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;
import io.deephaven.engine.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.engine.v2.sources.ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH;

public abstract class AbstractColumnSource<T> implements ColumnSource<T>, Serializable {

    private static final long serialVersionUID = 8003280177657671273L;

    protected final Class<T> type;
    protected final Class<?> componentType;

    protected volatile Map<T, TrackingMutableRowSet> groupToRange;

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
        } else if (DbArrayBase.class.isAssignableFrom(type)) {
            // noinspection deprecation
            if (DbBooleanArray.class.isAssignableFrom(type)) {
                componentType = Boolean.class;
            } else if (DbByteArray.class.isAssignableFrom(type)) {
                componentType = byte.class;
            } else if (DbCharArray.class.isAssignableFrom(type)) {
                componentType = char.class;
            } else if (DbDoubleArray.class.isAssignableFrom(type)) {
                componentType = double.class;
            } else if (DbFloatArray.class.isAssignableFrom(type)) {
                componentType = float.class;
            } else if (DbIntArray.class.isAssignableFrom(type)) {
                componentType = int.class;
            } else if (DbLongArray.class.isAssignableFrom(type)) {
                componentType = long.class;
            } else if (DbShortArray.class.isAssignableFrom(type)) {
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
    public Map<T, TrackingMutableRowSet> getGroupToRange() {
        return groupToRange;
    }

    @Override
    public Map<T, TrackingMutableRowSet> getGroupToRange(TrackingMutableRowSet rowSet) {
        return groupToRange;
    }

    public final void setGroupToRange(@Nullable Map<T, TrackingMutableRowSet> groupToRange) {
        this.groupToRange = groupToRange;
    }

    @Override
    public TrackingMutableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, TrackingMutableRowSet mapper,
                                       final Object... keys) {
        final Map<T, TrackingMutableRowSet> groupToRange = (isImmutable() || !usePrev) ? getGroupToRange(mapper) : null;
        if (groupToRange != null) {
            RowSetBuilderRandom allInMatchingGroups = RowSetFactoryImpl.INSTANCE.getRandomBuilder();

            if (caseInsensitive && (type == String.class)) {
                KeyedObjectHashSet keySet = new KeyedObjectHashSet<>(new CIStringKey());
                Collections.addAll(keySet, keys);

                for (Map.Entry<T, TrackingMutableRowSet> ent : groupToRange.entrySet()) {
                    if (keySet.containsKey(ent.getKey())) {
                        allInMatchingGroups.addRowSet(ent.getValue());
                    }
                }
            } else {
                for (Object key : keys) {
                    TrackingMutableRowSet range = groupToRange.get(key);
                    if (range != null) {
                        allInMatchingGroups.addRowSet(range);
                    }
                }
            }

            final TrackingMutableRowSet matchingValues;
            if (invertMatch) {
                matchingValues = mapper.minus(allInMatchingGroups.build());
            } else {
                matchingValues = mapper.intersect(allInMatchingGroups.build());
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
    public Map<T, TrackingMutableRowSet> getValuesMapping(TrackingMutableRowSet subRange) {
        Map<T, TrackingMutableRowSet> result = new LinkedHashMap<>();
        final Map<T, TrackingMutableRowSet> groupToRange = getGroupToRange();

        // if we have a grouping we can use it to avoid iterating the entire subRange. The issue is that our grouping
        // could be bigger than the rowSet we care about, by a very large margin. In this case we could be spinning
        // on TrackingMutableRowSet intersect operations that are actually useless. This check says that if our subRange is smaller
        // than the number of keys in our grouping, we should just fetch the keys instead and generate the grouping
        // from scratch.
        boolean useGroupToRange = (groupToRange != null) && (groupToRange.size() < subRange.size());
        if (useGroupToRange) {
            for (Map.Entry<T, TrackingMutableRowSet> typeEntry : groupToRange.entrySet()) {
                TrackingMutableRowSet mapping = subRange.intersect(typeEntry.getValue());
                if (mapping.size() > 0) {
                    result.put(typeEntry.getKey(), mapping);
                }
            }
        } else {
            Map<T, RowSetBuilderSequential> valueToIndexSet = new LinkedHashMap<>();

            for (TrackingMutableRowSet.Iterator it = subRange.iterator(); it.hasNext();) {
                long key = it.nextLong();
                T value = get(key);
                RowSetBuilderSequential indexes = valueToIndexSet.get(value);
                if (indexes == null) {
                    indexes = RowSetFactoryImpl.INSTANCE.getSequentialBuilder();
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

    /**
     * We have a fair bit of internal state that must be serialized, but not all of our descendants in the class
     * hierarchy should actually be sent over the wire. If you are implementing a class that should allow this to be
     * serialized, then you must annotate it with an IsSerializable annotation, containing a value of true.
     */
    @Retention(RetentionPolicy.RUNTIME)
    public @interface IsSerializable {
        boolean value() default false;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        if (isSerializable())
            oos.defaultWriteObject();
        else
            throw new UnsupportedOperationException(
                    "AbstractColumnSources are not all serializable, you may be missing a select() call.");
    }

    /**
     * Finds the most derived class that has an IsSerializable annotation, and returns its value. If no annotation is
     * found, then returns false.
     */
    private boolean isSerializable() {
        for (Class<?> clazz = getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            IsSerializable isSerializable = clazz.getAnnotation(IsSerializable.class);
            if (isSerializable != null) {
                return isSerializable.value();
            }
        }
        return false;
    }

    /**
     * Get a map from unique, boxed values in this column to a long[2] range of keys.
     *
     * @param rowSet The rowSet that defines the column along with the column source
     * @param columnSource The column source that defines the column along with the rowSet
     * @return A new value to range map (i.e. grouping metadata)
     */
    public static <TYPE> Map<TYPE, long[]> getValueToRangeMap(@NotNull final TrackingMutableRowSet rowSet,
            @Nullable final ColumnSource<TYPE> columnSource) {
        final long size = rowSet.size();
        if (columnSource == null) {
            return Collections.singletonMap(null, new long[] {0, size});
        }
        // noinspection unchecked
        return ((Map<TYPE, TrackingMutableRowSet>) rowSet.getGrouping(columnSource)).entrySet().stream()
                .sorted(java.util.Comparator.comparingLong(e -> e.getValue().firstRowKey())).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        new Function<Map.Entry<TYPE, TrackingMutableRowSet>, long[]>() {
                            private long prevLastKey = -1L;
                            private long currentSize = 0;

                            @Override
                            public long[] apply(@NotNull final Map.Entry<TYPE, TrackingMutableRowSet> entry) {
                                final TrackingMutableRowSet rowSet = entry.getValue();
                                Assert.instanceOf(rowSet, "rowSet", GroupingRowSetHelper.class);
                                Assert.gt(rowSet.firstRowKey(), "rowSet.firstRowKey()", prevLastKey, "prevLastKey");
                                prevLastKey = rowSet.lastRowKey();
                                return new long[] {currentSize, currentSize += rowSet.size()};
                            }
                        },
                        Assert::neverInvoked,
                        LinkedHashMap::new));
    }

    /**
     * Consume all groups in a group-to-rowSet map.
     *
     * @param groupToIndex The group-to-rowSet map to consume
     * @param groupConsumer Consumer for responsive groups
     */
    public static <TYPE> void forEachGroup(@NotNull final Map<TYPE, TrackingMutableRowSet> groupToIndex,
            @NotNull final BiConsumer<TYPE, TrackingMutableRowSet> groupConsumer) {
        groupToIndex.entrySet().stream()
                .filter(kie -> kie.getValue().nonempty())
                .sorted(java.util.Comparator.comparingLong(kie -> kie.getValue().firstRowKey()))
                .forEachOrdered(kie -> groupConsumer.accept(kie.getKey(), kie.getValue()));
    }

    /**
     * Convert a group-to-rowSet map to a pair of flat in-memory column sources, one for the keys and one for the
     * indexes.
     *
     * @param originalKeyColumnSource The key column source whose contents are reflected by the group-to-rowSet map (used
     *        for typing, only)
     * @param groupToIndex The group-to-rowSet map to convert
     * @return A pair of a flat key column source and a flat rowSet column source
     */
    @SuppressWarnings("unused")
    public static <TYPE> Pair<ArrayBackedColumnSource<TYPE>, ObjectArraySource<TrackingMutableRowSet>> groupingToFlatSources(
            @NotNull final ColumnSource<TYPE> originalKeyColumnSource, @NotNull final Map<TYPE, TrackingMutableRowSet> groupToIndex) {
        final int numGroups = groupToIndex.size();
        final ArrayBackedColumnSource<TYPE> resultKeyColumnSource = ArrayBackedColumnSource.getMemoryColumnSource(
                numGroups, originalKeyColumnSource.getType(), originalKeyColumnSource.getComponentType());
        final ObjectArraySource<TrackingMutableRowSet> resultIndexColumnSource = new ObjectArraySource<>(TrackingMutableRowSet.class);
        resultIndexColumnSource.ensureCapacity(numGroups);

        final MutableInt processedGroupCount = new MutableInt(0);
        forEachGroup(groupToIndex, (final TYPE key, final TrackingMutableRowSet rowSet) -> {
            final long groupIndex = processedGroupCount.longValue();
            resultKeyColumnSource.set(groupIndex, key);
            resultIndexColumnSource.set(groupIndex, rowSet);
            processedGroupCount.increment();
        });
        Assert.eq(processedGroupCount.intValue(), "processedGroupCount.intValue()", numGroups, "numGroups");
        return new Pair<>(resultKeyColumnSource, resultIndexColumnSource);
    }

    /**
     * Consume all responsive groups in a group-to-rowSet map.
     *
     * @param groupToIndex The group-to-rowSet map to consume
     * @param intersect Limit indices to values contained within intersect, eliminating empty result groups
     * @param groupConsumer Consumer for responsive groups
     */
    public static <TYPE> void forEachResponsiveGroup(@NotNull final Map<TYPE, TrackingMutableRowSet> groupToIndex,
            @NotNull final TrackingMutableRowSet intersect,
            @NotNull final BiConsumer<TYPE, TrackingMutableRowSet> groupConsumer) {
        groupToIndex.entrySet().stream()
                .map(kie -> new Pair<>(kie.getKey(), kie.getValue().intersect(intersect)))
                .filter(kip -> kip.getSecond().nonempty())
                .sorted(java.util.Comparator.comparingLong(kip -> kip.getSecond().firstRowKey()))
                .forEachOrdered(kip -> groupConsumer.accept(kip.getFirst(), kip.getSecond()));
    }

    /**
     * Convert a group-to-rowSet map to a pair of flat in-memory column sources, one for the keys and one for the
     * indexes.
     *
     * @param originalKeyColumnSource The key column source whose contents are reflected by the group-to-rowSet map (used
     *        for typing, only)
     * @param groupToIndex The group-to-rowSet map to convert
     * @param intersect Limit returned indices to values contained within intersect
     * @param responsiveGroups Set to the number of responsive groups on exit
     * @return A pair of a flat key column source and a flat rowSet column source
     */
    public static <TYPE> Pair<ArrayBackedColumnSource<TYPE>, ObjectArraySource<TrackingMutableRowSet>> groupingToFlatSources(
            @NotNull final ColumnSource<TYPE> originalKeyColumnSource,
            @NotNull final Map<TYPE, TrackingMutableRowSet> groupToIndex,
            @NotNull final TrackingMutableRowSet intersect,
            @NotNull final MutableInt responsiveGroups) {
        final int numGroups = groupToIndex.size();
        final ArrayBackedColumnSource<TYPE> resultKeyColumnSource = ArrayBackedColumnSource.getMemoryColumnSource(
                numGroups, originalKeyColumnSource.getType(), originalKeyColumnSource.getComponentType());
        final ObjectArraySource<TrackingMutableRowSet> resultIndexColumnSource = new ObjectArraySource<>(TrackingMutableRowSet.class);
        resultIndexColumnSource.ensureCapacity(numGroups);

        responsiveGroups.setValue(0);
        forEachResponsiveGroup(groupToIndex, intersect, (final TYPE key, final TrackingMutableRowSet rowSet) -> {
            final long groupIndex = responsiveGroups.longValue();
            resultKeyColumnSource.set(groupIndex, key);
            resultIndexColumnSource.set(groupIndex, rowSet);
            responsiveGroups.increment();
        });

        return new Pair<>(resultKeyColumnSource, resultIndexColumnSource);
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
        final ChunkFiller filler = destination.getChunkFiller();
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

    final void defaultFillPrevChunk(@SuppressWarnings("unused") @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ChunkFiller filler = destination.getChunkFiller();
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
        Assert.eq(getType(), "getType()", DBDateTime.class);
        Assert.eq(alternateDataType, "alternateDataType", long.class);
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) new UnboxedDateTimeWritableSource((WritableSource<DBDateTime>) this);
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
