package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.DictionaryKeys;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

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
     * Check if this region can expose an alternate form as paired regions of {@code int} keys and {@code DATA_TYPE}
     * values covering all of its index keys in {@code remainingKeys}. Both alternate regions must use the same or
     * smaller index key space as this one.
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

    interface SingletonDictionaryRegion<DATA_TYPE, ATTR extends Any> extends ColumnRegionObject<DATA_TYPE, ATTR> {

        ColumnRegionLong<DictionaryKeys> SINGLETON_DICTIONARY_KEYS_REGION = new ColumnRegionLong.Constant<>(RegionedColumnSourceBase.PARAMETERS.regionMask, 0L);

        @Override
        @FinalDefault
        default boolean supportsDictionaryFormat(@NotNull final OrderedKeys.Iterator remainingKeys, final boolean failFast) {
            advanceToNextPage(remainingKeys);
            return true;
        }

        @Override
        @FinalDefault
        default ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
            return SINGLETON_DICTIONARY_KEYS_REGION;
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

        private Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public DATA_TYPE getObject(final long elementIndex) {
            return null;
        }
    }

    final class Constant<DATA_TYPE, ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements SingletonDictionaryRegion<DATA_TYPE, ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final DATA_TYPE value;

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
    }

    final class StaticPageStore<DATA_TYPE, ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionObject<DATA_TYPE, ATTR>>
            implements ColumnRegionObject<DATA_TYPE, ATTR> {

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
    }
}
