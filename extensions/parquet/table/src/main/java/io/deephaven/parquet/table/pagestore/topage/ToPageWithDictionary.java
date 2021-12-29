package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.stringset.LongBitmapStringSet;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.parquet.base.ColumnChunkReader;
import io.deephaven.parquet.base.ColumnPageReader;
import io.deephaven.parquet.base.DataWithOffsets;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.IntBuffer;
import java.util.function.Function;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class ToPageWithDictionary<DATA_TYPE, ATTR extends Any>
        implements ToPage<ATTR, DATA_TYPE[]> {

    private final Class<DATA_TYPE> nativeType;
    private final ChunkDictionary<DATA_TYPE, ATTR> chunkDictionary;
    private final Function<Object, DATA_TYPE[]> convertResultFallbackFun;

    ToPageWithDictionary(
            @NotNull final Class<DATA_TYPE> nativeType,
            @NotNull final ChunkDictionary<DATA_TYPE, ATTR> chunkDictionary,
            @NotNull final Function<Object, DATA_TYPE[]> convertResultFallbackFun) {
        this.nativeType = nativeType;
        this.chunkDictionary = chunkDictionary;
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
    public final Object getResult(@NotNull final ColumnPageReader columnPageReader)
            throws IOException {
        if (columnPageReader.getDictionary() == ColumnChunkReader.NULL_DICTIONARY) {
            return ToPage.super.getResult(columnPageReader);
        }

        final int[] keys = new int[columnPageReader.numValues()];
        final IntBuffer offsets = columnPageReader.readKeyValues(IntBuffer.wrap(keys), NULL_INT);

        return offsets == null ? keys : new DataWithOffsets(offsets, keys);
    }

    @Override
    @NotNull
    public final DATA_TYPE[] convertResult(@NotNull final Object result) {
        if (!(result instanceof int[])) {
            return convertResultFallbackFun.apply(result);
        }

        final int[] from = (int[]) result;
        // noinspection unchecked
        final DATA_TYPE[] to = (DATA_TYPE[]) Array.newInstance(nativeType, from.length);

        for (int ii = 0; ii < from.length; ++ii) {
            to[ii] = chunkDictionary.get(from[ii]);
        }

        return to;
    }

    @Override
    @NotNull
    public final ObjectChunk<DATA_TYPE, ATTR> getDictionaryChunk() {
        return chunkDictionary.getChunk();
    }

    @NotNull
    public final LongBitmapStringSet.ReversibleLookup<DATA_TYPE> getReversibleLookup() {
        return chunkDictionary;
    }

    @Override
    @NotNull
    public final ToPage<DictionaryKeys, long[]> getDictionaryKeysToPage() {
        return new ToPage<DictionaryKeys, long[]>() {

            @Override
            @NotNull
            public Class<?> getNativeType() {
                return long.class;
            }

            @Override
            @NotNull
            public ChunkType getChunkType() {
                return ChunkType.Long;
            }

            @Override
            public Object nullValue() {
                return NULL_INT;
            }

            @Override
            public Object getResult(@NotNull final ColumnPageReader columnPageReader)
                    throws IOException {
                return ToPageWithDictionary.this.getResult(columnPageReader);
            }

            @Override
            public long[] convertResult(@NotNull final Object result) {
                final int[] from = (int[]) result;
                final long[] to = new long[from.length];

                for (int ii = 0; ii < from.length; ++ii) {
                    final int intKey = from[ii];
                    to[ii] = intKey == NULL_INT ? NULL_LONG : intKey;
                }

                return to;
            }
        };
    }
}
