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
    private final ObjectChunk<String, ATTR>[] dictionaries;
    private final StringCache<STRING_LIKE_TYPE> stringCache;
    private final long[] accumLengths;
    private final int nDicts;

    public static <T, ATTR extends Attributes.Any> ColumnRegionObject<T, ATTR> create(Class<T> nativeType, final Chunk<ATTR>[] dictionaries) {
        Require.eqTrue(CharSequence.class.isAssignableFrom(nativeType), "Dictionary result is not a string like type.");
        Require.neqNull(dictionaries, "dictionaries");
        //noinspection unchecked,rawtypes
        return (ColumnRegionObject<T, ATTR>) new ParquetColumnRegionSymbolTable(nativeType, (ObjectChunk[]) dictionaries);
    }

    private ParquetColumnRegionSymbolTable(@NotNull Class<STRING_LIKE_TYPE> nativeType, @NotNull final ObjectChunk<String, ATTR>[] dictionaries) {
        this.nativeType = nativeType;
        this.dictionaries = dictionaries;
        this.stringCache = StringUtils.getStringCache(nativeType);
        nDicts = dictionaries.length;
        accumLengths = new long[nDicts];
        long len = 0;
        for (int i = 0; i < nDicts; ++i) {
            len += dictionaries[i].size();
            accumLengths[i] = len;
        }
    }

    @Override
    public STRING_LIKE_TYPE getObject(long elementIndex) {
        if (nDicts == 1) {
            return stringCache.getCachedString(dictionaries[0].get((int) getRowOffset(elementIndex)));
        }
        return null;  // TODO MISSING
    }

    @Override
    public long length() {
        return accumLengths[nDicts - 1];
    }

    @Override
    public Class<?> getNativeType() {
        return nativeType;
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        if (nDicts == 1) {
            return dictionaries[0].slice(Math.toIntExact(getRowOffset(firstKey)), Math.toIntExact(lastKey - firstKey + 1));
        }
        return null;  // TODO MISSING
    }

    @Override
    public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        WritableObjectChunk<STRING_LIKE_TYPE, ? super ATTR> objectDestination = destination.asWritableObjectChunk();
        orderedKeys.forAllLongs((final long key) -> objectDestination.add(getObject(key)));
    }
}
