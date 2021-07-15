package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.v2.sources.StringSetImpl;
import io.deephaven.db.v2.sources.chunk.Attributes;
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

public class ToPageWithDictionary<T, ATTR extends Attributes.Any> implements ToPage<ATTR, T[]> {

    private final Class<T> nativeType;
    private final Dictionary<T, ATTR> dictionary;
    private final Function<Object, T[]> convertResultFallbackFun;

    ToPageWithDictionary(
            final Class<T> nativeType,
            final Dictionary<T, ATTR> dictionary,
            final Function<Object, T[]> convertResultFallbackFun) {
        this.nativeType = nativeType;
        this.dictionary = dictionary;
        this.convertResultFallbackFun = convertResultFallbackFun;
    }

    @Override
    @NotNull
    public final Class<T> getNativeType() {
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
        int[] keys = new int [columnPageReader.numValues()];
        IntBuffer offsets = columnPageReader.readKeyValues(IntBuffer.wrap(keys), NULL_INT);

        return offsets == null ? keys : new DataWithOffsets(offsets, keys);
    }

    @Override
    @NotNull
    public final T[] convertResult(final Object result) {
        if (!(result instanceof int[])) {
            return convertResultFallbackFun.apply(result);
        }
        int[] from = (int []) result;
        //noinspection unchecked
        T[] to = (T[])Array.newInstance(nativeType, from.length);

        for (int i = 0; i < from.length; ++i) {
            to[i] = dictionary.get(from[i]);
        }

        return to;
    }

    @Override
    @NotNull
    public final ObjectChunk<T, ATTR> getDictionary() {
        return dictionary.getChunk();
    }

    @NotNull
    public final StringSetImpl.ReversibleLookup<T> getReversibleLookup() {
        return dictionary;
    }

    @Override
    @NotNull
    public final ToPage<Attributes.DictionaryKeys, int[]> getDictionaryKeysToPage() {
        return new ToPage<Attributes.DictionaryKeys, int[]>() {

            @NotNull
            @Override
            public Class<?> getNativeType() {
                return int.class;
            }

            @NotNull
            @Override
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
