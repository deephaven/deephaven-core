package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.page.Page;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
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
     * @param elementIndex Element row key in the table's address space
     * @return The object value at the specified element row key
     */
    DATA_TYPE getObject(long elementIndex);

    /**
     * Get a single object from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element row key in the table's address space
     * @return The object value at the specified element row key
     */
    default DATA_TYPE getObject(@NotNull final FillContext context, final long elementIndex) {
        return getObject(elementIndex);
    }

    /**
     * Check if this region can expose an alternate form as paired regions of {@code long} keys and {@code DATA_TYPE}
     * values covering all of its row keys in {@code keysToVisit}.
     *
     * <p>Both alternate regions must use the same or smaller row key space as this one. Indices fetched from the
     * keys region must represent valid element indices in the values region. Values regions must support
     * {@link #gatherDictionaryValuesRowSet(RowSet.SearchIterator, RowSequence.Iterator, RowSetBuilderSequential)}.
     *
     * <p>Use {@link #getDictionaryKeysRegion()} to access the region of keys and {@link #getDictionaryValuesRegion()}
     * to access the region of values.
     *
     * @param keysToVisit Iterator positioned at the first relevant row key belonging to this region.
     *                    Will be advanced to <em>after</em> this region if {@code true} is returned.
     *                    No guarantee is made if {@code false} is returned.
     * @return A {@link RegionVisitResult} specifying {@code FAILED} if this region cannot supply a dictionary,
     * {@code CONTINUE} if it can and {@code keysToVisit} is <em>not</em> exhausted, and {@code COMPLETE} if it can and
     * {@code keysToVisit} is exhausted
     */
    default RegionVisitResult supportsDictionaryFormat(@NotNull final RowSet.SearchIterator keysToVisit) {
        return RegionVisitResult.FAILED;
    }

    /**
     * Optional method that should only be used on regions returned by {@link #getDictionaryValuesRegion()}.
     * Gathers
     *
     * @param keysToVisit       A search iterator over the enclosing table address space (which must have the same
     *                          regions at the same masks), positioned at a row key in this region. Used to
     *                          identify regions to visit. Should be advanced to after this region as a side-effect.
     * @param knownKeys         An iterator over the previously-known row keys, positioned at the first known key in
     *                          this region, or after the region's maximum key if no keys are known. Should be advanced
     *                          to after this region as a side effect.
     * @param sequentialBuilder Output builder; implementations should append ranges for row keys not found in
     *                          {@code knownKeys}
     * @throws UnsupportedOperationException If this region is incapable of gathering its dictionary values RowSet
     * @return Whether {@code keysToVisit} has been exhausted
     */
    default boolean gatherDictionaryValuesRowSet(@NotNull final RowSet.SearchIterator keysToVisit,
                                                 @NotNull final RowSequence.Iterator knownKeys,
                                                 @NotNull final RowSetBuilderSequential sequentialBuilder) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return A dictionary keys region as specified by {@link #supportsDictionaryFormat(RowSet.SearchIterator)}
     * @throws UnsupportedOperationException If this region does not support dictionary format
     * @implNote Implementations should cache the result
     */
    default ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return A dictionary values region as specified by {@link #supportsDictionaryFormat(RowSet.SearchIterator)}
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

    interface SelfDictionaryRegion<DATA_TYPE, ATTR extends Any> extends ColumnRegionObject<DATA_TYPE, ATTR> {

        @Override
        @FinalDefault
        default RegionVisitResult supportsDictionaryFormat(@NotNull final RowSet.SearchIterator keysToVisit) {
            return advanceToNextPage(keysToVisit) ? RegionVisitResult.CONTINUE : RegionVisitResult.COMPLETE;
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

    final class Null<DATA_TYPE, ATTR extends Any> extends ColumnRegion.Null<ATTR> implements SelfDictionaryRegion<DATA_TYPE, ATTR> {

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
        public boolean gatherDictionaryValuesRowSet(@NotNull final RowSet.SearchIterator keysToVisit,
                                                    @NotNull final RowSequence.Iterator knownKeys,
                                                    @NotNull final RowSetBuilderSequential sequentialBuilder) {
            // Nothing to be gathered, we don't include null regions in dictionary values.
            advanceToNextPage(knownKeys);
            return advanceToNextPage(keysToVisit);
        }

        @Override
        public ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
            return dictionaryKeysRegion == null ? dictionaryKeysRegion = ColumnRegionLong.createNull(mask()) : dictionaryKeysRegion;
        }
    }

    static ColumnRegionLong<DictionaryKeys> createConstantDictionaryKeysRegion(final long pageMask) {
        return pageMask == Constant.DEFAULT_SINGLETON_DICTIONARY_KEYS_REGION.mask()
                ? Constant.DEFAULT_SINGLETON_DICTIONARY_KEYS_REGION
                : new ColumnRegionLong.Constant<>(pageMask, 0L);
    }

    final class Constant<DATA_TYPE, ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements SelfDictionaryRegion<DATA_TYPE, ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private static final ColumnRegionLong<DictionaryKeys> DEFAULT_SINGLETON_DICTIONARY_KEYS_REGION = new ColumnRegionLong.Constant<>(RegionedColumnSourceBase.PARAMETERS.regionMask, 0L);

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
        public boolean gatherDictionaryValuesRowSet(@NotNull final RowSet.SearchIterator keysToVisit,
                                                    @NotNull final RowSequence.Iterator knownKeys,
                                                    @NotNull final RowSetBuilderSequential sequentialBuilder) {
            final long pageOnlyKey = firstRow(keysToVisit.currentValue());
            if (knownKeys.peekNextKey() != pageOnlyKey) {
                sequentialBuilder.appendKey(pageOnlyKey);
            }
            advanceToNextPage(knownKeys);
            return advanceToNextPage(keysToVisit);
        }

        @Override
        public ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
            return dictionaryKeysRegion == null ? dictionaryKeysRegion = createConstantDictionaryKeysRegion(mask()) : dictionaryKeysRegion;
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
        public RegionVisitResult supportsDictionaryFormat(@NotNull final RowSet.SearchIterator keysToVisit) {
            final long pageMaxKey = maxRow(keysToVisit.currentValue());
            RegionVisitResult result;
            do {
                result = lookupRegion(keysToVisit.currentValue()).supportsDictionaryFormat(keysToVisit);
            } while (result == RegionVisitResult.CONTINUE && keysToVisit.currentValue() <= pageMaxKey);
            return result;
        }

        @Override
        public boolean gatherDictionaryValuesRowSet(@NotNull final RowSet.SearchIterator keysToVisit,
                                                    @NotNull final RowSequence.Iterator knownKeys,
                                                    @NotNull final RowSetBuilderSequential sequentialBuilder) {
            final long pageMaxKey = maxRow(keysToVisit.currentValue());
            boolean moreKeysToVisit;
            do {
                moreKeysToVisit = lookupRegion(keysToVisit.currentValue()).gatherDictionaryValuesRowSet(keysToVisit, knownKeys, sequentialBuilder);
            } while (moreKeysToVisit && keysToVisit.currentValue() <= pageMaxKey);
            return moreKeysToVisit;
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

        public static ColumnRegionLong<DictionaryKeys> create(@NotNull final RegionedPageStore.Parameters parameters,
                                                              final int regionIndex,
                                                              @NotNull final ColumnRegionObject<?, ?> sourceRegion) {
            final ColumnRegionLong<DictionaryKeys> sourceDictKeys = sourceRegion.getDictionaryKeysRegion();
            if (sourceDictKeys instanceof ColumnRegionLong.Null) {
                return sourceDictKeys;
            }
            return new DictionaryKeysWrapper((long) regionIndex << parameters.regionMaskNumBits, sourceDictKeys);
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
            return dictionaryKey == NULL_LONG ? NULL_LONG : prefixBits | dictionaryKey;
        }

        @Override
        public long getLong(@NotNull final FillContext context, final long elementIndex) {
            final long dictionaryKey = wrapped.getLong(context, elementIndex);
            return dictionaryKey == NULL_LONG ? NULL_LONG : prefixBits | dictionaryKey;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super DictionaryKeys> destination, @NotNull final RowSequence rowSequence) {
            final WritableLongChunk<? super DictionaryKeys> typed = destination.asWritableLongChunk();
            final int firstOffsetInclusive = destination.size();
            try (final RowSequence.Iterator rsi = rowSequence.getRowSequenceIterator()) {
                wrapped.fillChunkAppend(context, destination, rsi);
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
