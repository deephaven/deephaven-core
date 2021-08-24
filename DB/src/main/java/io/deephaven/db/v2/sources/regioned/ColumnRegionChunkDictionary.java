package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.string.cache.StringCache;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * {@link ColumnRegionObject} implementation for regions that support fetching symbols from a
 * dictionary represented as an {@link ObjectChunk}.
 */
public class ColumnRegionChunkDictionary<DICT_TYPE, DATA_TYPE, ATTR extends Any>
    extends GenericColumnRegionBase<ATTR>
    implements ColumnRegionObject<DATA_TYPE, ATTR>, Page.WithDefaults<ATTR>,
    DefaultChunkSource.SupportsContiguousGet<ATTR> {

    private final ObjectChunk<DICT_TYPE, ATTR> dictionary;
    private final Function<DICT_TYPE, DATA_TYPE> conversion;

    public static <DATA_TYPE, ATTR extends Any> ColumnRegionObject<DATA_TYPE, ATTR> create(
        final long pageMask,
        @NotNull final Class<DATA_TYPE> dataType,
        @NotNull final Chunk<ATTR> dictionary) {
        if (CharSequence.class.isAssignableFrom(dataType)) {
            // noinspection unchecked
            final StringCache<?> stringCache =
                StringUtils.getStringCache((Class<? extends CharSequence>) dataType);
            // noinspection unchecked
            final Function<String, DATA_TYPE> conversion =
                (final String dictValue) -> (DATA_TYPE) stringCache.getCachedString(dictValue);
            return new ColumnRegionChunkDictionary<>(pageMask, dictionary.asObjectChunk(),
                conversion);
        }
        return new ColumnRegionChunkDictionary<>(pageMask, dictionary.asObjectChunk(),
            Function.identity());
    }

    private ColumnRegionChunkDictionary(final long pageMask,
        @NotNull final ObjectChunk<DICT_TYPE, ATTR> dictionary,
        @NotNull final Function<DICT_TYPE, DATA_TYPE> conversion) {
        super(pageMask);
        this.dictionary = dictionary;
        this.conversion = conversion;
    }

    @Override
    public DATA_TYPE getObject(final long elementIndex) {
        return conversion.apply(dictionary.get((int) getRowOffset(elementIndex)));
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, final long firstKey,
        final long lastKey) {
        return dictionary.slice(Math.toIntExact(getRowOffset(firstKey)),
            Math.toIntExact(lastKey - firstKey + 1));
    }

    @Override
    public void fillChunkAppend(@NotNull final FillContext context,
        @NotNull final WritableChunk<? super ATTR> destination,
        @NotNull final OrderedKeys orderedKeys) {
        final WritableObjectChunk<DATA_TYPE, ? super ATTR> objectDestination =
            destination.asWritableObjectChunk();
        orderedKeys.forAllLongs((final long key) -> objectDestination.add(getObject(key)));
    }

    @Override
    public boolean gatherDictionaryValuesIndex(
        @NotNull final ReadOnlyIndex.SearchIterator keysToVisit,
        @NotNull final OrderedKeys.Iterator knownKeys,
        @NotNull final Index.SequentialBuilder sequentialBuilder) {
        final long pageFirstKey = firstRow(keysToVisit.currentValue());
        final long pageLastKey = pageFirstKey + dictionary.size() - 1;
        if (knownKeys.peekNextKey() != pageFirstKey) {
            // We need to add the entire page
            sequentialBuilder.appendRange(pageFirstKey, pageLastKey);
            advanceToNextPage(knownKeys);
        } else {
            final long knownSize = advanceToNextPageAndGetPositionDistance(knownKeys);
            if (knownSize != dictionary.size()) {
                sequentialBuilder.appendRange(pageFirstKey + knownSize, pageLastKey);
            }
        }
        return advanceToNextPage(keysToVisit);
    }
}
