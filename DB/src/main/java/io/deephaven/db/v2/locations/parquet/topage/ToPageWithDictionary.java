package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.v2.sources.StringSetImpl;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.DictionaryKeys;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.parquet.ColumnPageReader;
import io.deephaven.parquet.DataWithOffsets;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.IntBuffer;
import java.util.function.Function;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class ToPageWithDictionary<DATA_TYPE, ATTR extends Any> implements ToPage<ATTR, DATA_TYPE[]> {

    private final Class<DATA_TYPE> nativeType;
    private final Dictionary<DATA_TYPE, ATTR> dictionary;
    private final Function<Object, DATA_TYPE[]> convertResultFallbackFun;

    ToPageWithDictionary(
            final Class<DATA_TYPE> nativeType,
            final Dictionary<DATA_TYPE, ATTR> dictionary,
            final Function<Object, DATA_TYPE[]> convertResultFallbackFun) {
        this.nativeType = nativeType;
        this.dictionary = dictionary;
        this.convertResultFallbackFun = convertResultFallbackFun;
    }

    @Override
    @NotNull
    public final Class<DATA_TYPE> getNativeType() {
        return nativeType;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final Object getResult(final ColumnPageReader columnPageReader) throws IOException {
        if (columnPageReader.getDictionary() == null) {
            return ToPage.super.getResult(columnPageReader);
        }

        final int[] keys = new int[columnPageReader.numValues()];
        final IntBuffer offsets = columnPageReader.readKeyValues(IntBuffer.wrap(keys), NULL_INT);

        return offsets == null ? keys : new DataWithOffsets(offsets, keys);
    }

    @Override
    @NotNull
    public final DATA_TYPE[] convertResult(final Object result) {
        if (!(result instanceof int[])) {
            return convertResultFallbackFun.apply(result);
        }

        final int[] from = (int[]) result;
        //noinspection unchecked
        final DATA_TYPE[] to = (DATA_TYPE[]) Array.newInstance(nativeType, from.length);

        for (int fi = 0; fi < from.length; ++fi) {
            to[fi] = dictionary.get(from[fi]);
        }

        return to;
    }

    @Override
    @NotNull
    public final ObjectChunk<DATA_TYPE, ATTR> getDictionary() {
        return dictionary.getChunk();
    }

    @NotNull
    public final StringSetImpl.ReversibleLookup<DATA_TYPE> getReversibleLookup() {
        return dictionary;
    }

    @Override
    @NotNull
    public final ToPage<DictionaryKeys, int[]> getDictionaryKeysToPage() {
        return new ToPage<DictionaryKeys, int[]>() {

            @Override
            @NotNull
            public Class<?> getNativeType() {
                return int.class;
            }

            @Override
            @NotNull
            public ChunkType getChunkType() {
                return ChunkType.Int;
            }

            @Override
            public Object nullValue() {
                return NULL_INT;
            }

            @Override
            public Object getResult(final ColumnPageReader columnPageReader) throws IOException {
                return ToPageWithDictionary.this.getResult(columnPageReader);
            }
        };
    }
}
