package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.string.cache.StringCache;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * {@link ColumnRegionObject} implementation for regions that support fetching symbols from a dictionary from a
 * {@link ObjectChunk}.
 */
public class ParquetColumnRegionSymbolTable<ATTR extends Attributes.Any, STRING_LIKE_TYPE extends CharSequence>
        implements ColumnRegionObject<STRING_LIKE_TYPE, ATTR>, Page.WithDefaults<ATTR>, DefaultChunkSource.SupportsContiguousGet<ATTR> {

    private final Class<STRING_LIKE_TYPE> nativeType;
    private final ObjectChunk<String, ATTR>[] dictionaries;

    private final int dictionaryCount;
    private final long[] dictionaryLastIndices;
    private final StringCache<STRING_LIKE_TYPE> stringCache;

    public static <T, ATTR extends Attributes.Any> ColumnRegionObject<T, ATTR> create(Class<T> nativeType, final Chunk<ATTR>[] dictionaries) {
        Require.eqTrue(CharSequence.class.isAssignableFrom(nativeType), "Dictionary result is not a string like type.");
        Require.elementsNeqNull(dictionaries, "dictionaries");
        Require.gtZero(dictionaries.length, "dictionaries.length");
        //noinspection unchecked,rawtypes
        return (ColumnRegionObject<T, ATTR>) new ParquetColumnRegionSymbolTable(nativeType, (ObjectChunk[]) dictionaries);
    }

    private ParquetColumnRegionSymbolTable(@NotNull Class<STRING_LIKE_TYPE> nativeType, @NotNull final ObjectChunk<String, ATTR>[] dictionaries) {
        this.nativeType = nativeType;
        this.dictionaries = dictionaries;
        dictionaryCount = dictionaries.length;
        dictionaryLastIndices = new long[dictionaryCount];
        long lastIndex = -1L;
        for (int di = 0; di < dictionaryCount; ++di) {
            dictionaryLastIndices[di] = lastIndex += dictionaries[di].size();
        }
        stringCache = StringUtils.getStringCache(nativeType);
    }

    @Override
    public STRING_LIKE_TYPE getObject(final long elementIndex) {
        final long rowIndex = getRowOffset(elementIndex);
        final int dictionaryIndex;
        final int rowIndexInDictionary;
        if (dictionaryCount == 1) {
            dictionaryIndex = 0;
            rowIndexInDictionary = (int) rowIndex;
        } else {
            final int rawDictionaryIndex = Arrays.binarySearch(dictionaryLastIndices, rowIndex);
            dictionaryIndex = rawDictionaryIndex < 0 ? ~rawDictionaryIndex : rawDictionaryIndex;
            rowIndexInDictionary = (int) (dictionaryIndex == 0 ? rowIndex : rowIndex - dictionaryLastIndices[dictionaryIndex - 1] - 1);
        }
        return stringCache.getCachedString(dictionaries[dictionaryIndex].get(rowIndexInDictionary);
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
    public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, @NotNull final OrderedKeys orderedKeys) {
        // TODO-RWC: This is no good. Also, we need to delegate to a dictionary page, I think.
        final WritableObjectChunk<STRING_LIKE_TYPE, ? super ATTR> objectDestination = destination.asWritableObjectChunk();
        orderedKeys.forAllLongs((final long key) -> objectDestination.add(getObject(key)));
    }
}
