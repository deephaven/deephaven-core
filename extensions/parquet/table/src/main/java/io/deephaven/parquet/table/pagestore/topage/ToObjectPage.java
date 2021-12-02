package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.ChunkType;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.function.Supplier;

public class ToObjectPage<T, ATTR extends Any> implements ToPage<ATTR, T[]> {

    private final Class<T> nativeType;
    private final ObjectCodec<T> codec;

    public static <T, ATTR extends Any>
    ToPage<ATTR, T[]> create(final Class<T> nativeType,
                             final @NotNull ObjectCodec<T> codec,
                             final Supplier<Dictionary> dictionarySupplier) {
        if (!nativeType.isPrimitive()) {
            return dictionarySupplier == null ? new ToObjectPage<>(nativeType, codec) :
                    new ToPageWithDictionary<>(
                            nativeType,
                            new ChunkDictionary<>(
                                    (dictionary, key) -> {
                                        final byte[] bytes = dictionary.decodeToBinary(key).getBytes();
                                        return codec.decode(bytes, 0, bytes.length);
                                    },
                                    dictionarySupplier),
                            (final Object result) -> convertResult(nativeType, codec, result));
        }

        throw new IllegalArgumentException("The native type for a Object column is " + nativeType.getCanonicalName());
    }

    private ToObjectPage(Class<T> nativeType, ObjectCodec<T> codec) {
        this.nativeType = nativeType;
        this.codec = codec;
    }

    @Override
    @NotNull
    public final Class<?> getNativeType() {
        return nativeType;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final T[] convertResult(Object result) {
        return convertResult(nativeType, codec, result);
    }

    private static <T2> T2[] convertResult(final Class<T2> nativeType, final ObjectCodec<T2> codec, final Object result) {
        Binary[] from = (Binary[]) result;
        //noinspection unchecked
        T2[] to = (T2[]) Array.newInstance(nativeType, from.length);

        for (int ri = 0; ri < to.length; ++ri) {
            if (from[ri] != null) {
                byte[] bytes = from[ri].getBytes();
                to[ri] = codec.decode(bytes, 0, bytes.length);
            }
        }

        return to;
    }
}
