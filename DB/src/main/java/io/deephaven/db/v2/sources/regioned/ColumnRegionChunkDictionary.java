package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.string.cache.StringCache;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnRegionObject} implementation for regions that support fetching symbols from a dictionary represented as
 * an {@link ObjectChunk}.
 */
public class ColumnRegionChunkDictionary<ATTR extends Any, STRING_LIKE_TYPE extends CharSequence>
        extends GenericColumnRegionBase<ATTR>
        implements ColumnRegionObject<STRING_LIKE_TYPE, ATTR>, Page.WithDefaults<ATTR>, DefaultChunkSource.SupportsContiguousGet<ATTR> {

    private final ObjectChunk<String, ATTR> dictionary;
    private final StringCache<STRING_LIKE_TYPE> stringCache;

    public static <STRING_LIKE_TYPE, ATTR extends Any> ColumnRegionObject<STRING_LIKE_TYPE, ATTR> create(
            final long pageMask,
            @NotNull final Class<STRING_LIKE_TYPE> dataType,
            @NotNull final Chunk<ATTR> dictionary) {
        Require.eqTrue(CharSequence.class.isAssignableFrom(dataType), "Dictionary result is not a string like type.");
        Require.neqNull(dictionary, "dictionary");
        //noinspection unchecked
        return new ColumnRegionChunkDictionary(pageMask, dataType, dictionary.asObjectChunk());
    }

    private ColumnRegionChunkDictionary(final long pageMask,
                                        @NotNull final Class<STRING_LIKE_TYPE> dataType,
                                        @NotNull final ObjectChunk<String, ATTR> dictionary) {
        super(pageMask);
        this.dictionary = dictionary;
        this.stringCache = StringUtils.getStringCache(dataType);
    }

    @Override
    public STRING_LIKE_TYPE getObject(final long elementIndex) {
        return stringCache.getCachedString(dictionary.get((int) getRowOffset(elementIndex)));
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, final long firstKey, final long lastKey) {
        return dictionary.slice(Math.toIntExact(getRowOffset(firstKey)), Math.toIntExact(lastKey - firstKey + 1));
    }

    @Override
    public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, @NotNull final OrderedKeys orderedKeys) {
        final WritableObjectChunk<STRING_LIKE_TYPE, ? super ATTR> objectDestination = destination.asWritableObjectChunk();
        orderedKeys.forAllLongs((final long key) -> objectDestination.add(getObject(key)));
    }
}
