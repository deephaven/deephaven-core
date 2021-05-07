package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.string.cache.StringCache;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnRegionObject} implementation for regions that support fetching symbols from a dictionary from a
 * {@link ObjectChunk}.
 */
public class ParquetColumnRegionSymbolTable<ATTR extends Attributes.Any, STRING_LIKE_TYPE extends CharSequence>
        implements ColumnRegionObject<STRING_LIKE_TYPE, ATTR>, Page.WithDefaults<ATTR>, DefaultChunkSource.SupportsContiguousGet<ATTR> {

    private final Class<STRING_LIKE_TYPE> nativeType;
    private final ObjectChunk<String, ATTR> dictionary;
    private final StringCache<STRING_LIKE_TYPE> stringCache;

    static <T, ATTR extends Attributes.Any> ColumnRegionObject<T, ATTR> create(Class<T> nativeType, final Chunk<ATTR> dictionary) {
        Require.eqTrue(CharSequence.class.isAssignableFrom(nativeType), "Dictionary result is not a string like type.");
        Require.neqNull(dictionary, "dictionary");
        //noinspection unchecked,rawtypes
        return (ColumnRegionObject<T, ATTR>) new ParquetColumnRegionSymbolTable(nativeType, dictionary.asObjectChunk());
    }

    private ParquetColumnRegionSymbolTable(@NotNull Class<STRING_LIKE_TYPE> nativeType, @NotNull final ObjectChunk<String, ATTR> dictionary) {
        this.nativeType = nativeType;
        this.dictionary = dictionary;
        this.stringCache = StringUtils.getStringCache(nativeType);
    }

    @Override
    public STRING_LIKE_TYPE getObject(long elementIndex) {
        return stringCache.getCachedString(dictionary.get((int)getRowOffset(elementIndex)));
    }

    @Override
    public long length() {
        return dictionary.size();
    }

    @Override
    public Class<?> getNativeType() {
        return nativeType;
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return dictionary.slice(Math.toIntExact(getRowOffset(firstKey)), Math.toIntExact(lastKey - firstKey + 1));
    }

    @Override
    public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        WritableObjectChunk<STRING_LIKE_TYPE, ? super ATTR> objectDestination = destination.asWritableObjectChunk();

        orderedKeys.forAllLongs((final long key) -> objectDestination.add(getObject(key)));
    }
}
