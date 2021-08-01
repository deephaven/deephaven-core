package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.DictionaryKeys;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Column region interface for regions that support fetching objects.
 */
public interface ColumnRegionObject<DATA_TYPE, ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single object from this region.
     *
     * @param elementIndex Element (object) index in the table's address space
     * @return The object value at the specified element (object) index
     */
    DATA_TYPE getObject(long elementIndex);

    /**
     * Get a single object from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (object) index in the table's address space
     * @return The object value at the specified element (object) index
     */
    default DATA_TYPE getObject(@NotNull final FillContext context, final long elementIndex) {
        return getObject(elementIndex);
    }

    /**
     * Check if this region can expose an alternate form as paired regions of {@code long} keys and {@code DATA_TYPE}
     * values covering all of its index keys in {@code remainingKeys}.
     *
     * <p>Both alternate regions must use the same or smaller index key space as this one. Keys fetched from the
     * keys region must represent valid element indices in the values region.
     *
     * <p>Use {@link #getDictionaryKeysRegion()} to access the region of keys and {@link #getDictionaryValuesRegion()}
     * to access the region of values.
     *
     * @param remainingKeys Iterator positioned at the first relevant index key belonging to this region.
     *                      Will be advanced to <em>after</em> this region if {@code failFast == false} or {@code true}
     *                      is returned.
     *                      No guarantee is made if {@code failFast == true} and {@code false} is returned.
     * @param failFast      Whether this is part of an iteration that should short-circuit on the first {@code false}
     *                      result
     * @return Whether this region can supply a dictionary format covering all of its keys in {@code remainingKeys}
     */
    default boolean supportsDictionaryFormat(@NotNull final OrderedKeys.Iterator remainingKeys, final boolean failFast) {
        if (!failFast) {
            advanceToNextPage(remainingKeys);
        }
        return false;
    }

    /**
     * @return A dictionary keys region as specified by {@link #supportsDictionaryFormat(OrderedKeys.Iterator, boolean)}
     * @throws UnsupportedOperationException If this region does not support dictionary format
     * @implNote Implementations should cache the result
     */
    default ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return A dictionary values region as specified by {@link #supportsDictionaryFormat(OrderedKeys.Iterator, boolean)}
     * @throws UnsupportedOperationException If this region does not support dictionary format
     * @implNote Implementations should cache the result
     */
    default ColumnRegionObject<DATA_TYPE, ATTR> getDictionaryValuesRegion() {
        throw new UnsupportedOperationException();
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Object;
    }

    static ColumnRegionLong<DictionaryKeys> createSingletonDictionaryKeysRegion(final long pageMask) {
        return pageMask == SingletonDictionaryRegion.DEFAULT_SINGLETON_DICTIONARY_KEYS_REGION.mask()
                ? SingletonDictionaryRegion.DEFAULT_SINGLETON_DICTIONARY_KEYS_REGION
                : new ColumnRegionLong.Constant<>(pageMask, 0L);
    }

    interface SingletonDictionaryRegion<DATA_TYPE, ATTR extends Any> extends ColumnRegionObject<DATA_TYPE, ATTR> {

        ColumnRegionLong<DictionaryKeys> DEFAULT_SINGLETON_DICTIONARY_KEYS_REGION = new ColumnRegionLong.Constant<>(RegionedColumnSourceBase.PARAMETERS.regionMask, 0L);

        @Override
        @FinalDefault
        default boolean supportsDictionaryFormat(@NotNull final OrderedKeys.Iterator remainingKeys, final boolean failFast) {
            advanceToNextPage(remainingKeys);
            return true;
        }

        @Override
        @FinalDefault
        default ColumnRegionObject<DATA_TYPE, ATTR> getDictionaryValuesRegion() {
            return this;
        }
    }

    static <DATA_TYPE, ATTR extends Any> ColumnRegionObject<DATA_TYPE, ATTR> createNull(final long pageMask) {
        //noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<DATA_TYPE, ATTR>(pageMask);
    }

    final class Null<DATA_TYPE, ATTR extends Any> extends ColumnRegion.Null<ATTR> implements SingletonDictionaryRegion<DATA_TYPE, ATTR> {

        @SuppressWarnings("rawtypes")
        private static final ColumnRegionObject DEFAULT_INSTANCE = new ColumnRegionObject.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        private ColumnRegionLong<DictionaryKeys> dictionaryKeysRegion;

        private Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public DATA_TYPE getObject(final long elementIndex) {
            return null;
        }

        @Override
        public ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
            return dictionaryKeysRegion == null ? dictionaryKeysRegion = createSingletonDictionaryKeysRegion(mask()) : dictionaryKeysRegion;
        }
    }

    final class Constant<DATA_TYPE, ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements SingletonDictionaryRegion<DATA_TYPE, ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final DATA_TYPE value;

        private ColumnRegionLong<DictionaryKeys> dictionaryKeysRegion;

        public Constant(final long pageMask, final DATA_TYPE value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public DATA_TYPE getObject(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableObjectChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }

        @Override
        public ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
            return dictionaryKeysRegion == null ? dictionaryKeysRegion = createSingletonDictionaryKeysRegion(mask()) : dictionaryKeysRegion;
        }
    }

    final class StaticPageStore<DATA_TYPE, ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionObject<DATA_TYPE, ATTR>>
            implements ColumnRegionObject<DATA_TYPE, ATTR> {

        private ColumnRegionLong<DictionaryKeys> dictionaryKeysRegion;
        private ColumnRegionObject<DATA_TYPE, ATTR> dictionaryValuesRegion;

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionObject<DATA_TYPE, ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public DATA_TYPE getObject(final long elementIndex) {
            return lookupRegion(elementIndex).getObject(elementIndex);
        }

        @Override
        public DATA_TYPE getObject(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getObject(context, elementIndex);
        }

        @Override
        public boolean supportsDictionaryFormat(@NotNull final OrderedKeys.Iterator remainingKeys, final boolean failFast) {
            boolean result = true;
            try (final OrderedKeys regionKeys = remainingKeys.getNextOrderedKeysThrough(maxRow(remainingKeys.peekNextKey()));
                 final OrderedKeys.Iterator regionRemainingKeys = regionKeys.getOrderedKeysIterator()) {
                while (regionRemainingKeys.hasMore()) {
                    if (!lookupRegion(regionRemainingKeys.peekNextKey()).supportsDictionaryFormat(regionRemainingKeys, failFast)) {
                        result = false;
                        if (failFast) {
                            break;
                        }
                    }
                }
            }
            return result;
        }

        @Override
        public ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
            return dictionaryKeysRegion == null
                    ? dictionaryKeysRegion = new ColumnRegionLong.StaticPageStore<>(parameters(), mapRegionsToDictionaryKeys())
                    : dictionaryKeysRegion;
        }

        @Override
        public ColumnRegionObject<DATA_TYPE, ATTR> getDictionaryValuesRegion() {
            return dictionaryValuesRegion == null
                    ? dictionaryValuesRegion = new ColumnRegionObject.StaticPageStore<>(parameters(), mapRegionsToDictionaryValues())
                    : dictionaryValuesRegion;
        }

        private ColumnRegionLong<DictionaryKeys>[] mapRegionsToDictionaryKeys() {
            //noinspection unchecked
            return IntStream.range(0, getRegionCount())
                    .mapToObj(ri -> DictionaryKeysWrapper.create(parameters(), ri, getRegion(ri)))
                    .toArray(ColumnRegionLong[]::new);
        }

        private ColumnRegionObject<DATA_TYPE, ATTR>[] mapRegionsToDictionaryValues() {
            //noinspection unchecked
            return IntStream.range(0, getRegionCount())
                    .mapToObj(ri -> getRegion(ri).getDictionaryValuesRegion())
                    .toArray(ColumnRegionObject[]::new);
        }
    }

    final class DictionaryKeysWrapper implements ColumnRegionLong<DictionaryKeys>, Page.WithDefaults<DictionaryKeys> {

        static ColumnRegionLong<DictionaryKeys> create(@NotNull final RegionedPageStore.Parameters parameters,
                                                       final int regionIndex,
                                                       @NotNull final ColumnRegionObject<?, ?> sourceRegion) {
            return new DictionaryKeysWrapper((long) regionIndex << parameters.regionMaskNumBits, sourceRegion.getDictionaryKeysRegion());
        }

        private final long prefixBits;
        private final ColumnRegionLong<DictionaryKeys> wrapped;

        private DictionaryKeysWrapper(final long prefixBits, @NotNull final ColumnRegionLong<DictionaryKeys> wrapped) {
            this.prefixBits = prefixBits;
            this.wrapped = wrapped;
        }

        @Override
        public long mask() {
            return wrapped.mask();
        }

        @Override
        public long getLong(final long elementIndex) {
            final long dictionaryKey = wrapped.getLong(elementIndex);
            return dictionaryKey == NULL_LONG ? NULL_LONG : prefixBits | dictionaryKey ;
        }

        @Override
        public long getLong(@NotNull final FillContext context, final long elementIndex) {
            final long dictionaryKey = wrapped.getLong(context, elementIndex);
            return dictionaryKey == NULL_LONG ? NULL_LONG : prefixBits | dictionaryKey ;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super DictionaryKeys> destination, @NotNull final OrderedKeys orderedKeys) {
            final WritableLongChunk<? super DictionaryKeys> typed = destination.asWritableLongChunk();
            final int firstOffsetInclusive = destination.size();
            try (final OrderedKeys.Iterator oki = orderedKeys.getOrderedKeysIterator()) {
                wrapped.fillChunkAppend(context, destination, oki);
            }
            final int lastOffsetExclusive = destination.size();
            for (int dki = firstOffsetInclusive; dki < lastOffsetExclusive; ++dki) {
                final long dictionaryKey = typed.get(dki);
                if (dictionaryKey != NULL_LONG) {
                    typed.set(dki, prefixBits | dictionaryKey);
                }
            }
        }
    }
}
