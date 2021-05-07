package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

public class ToObjectPage<T, ATTR extends Attributes.Any> implements ToPage<ATTR, T[]> {

    private final Class<T> nativeType;
    private final ObjectCodec<T> codec;

    public static <T, ATTR extends Attributes.Any>
    ToPage<ATTR, T[]> create(Class<T> nativeType, @NotNull ObjectCodec<T> codec,
                        org.apache.parquet.column.Dictionary dictionary) {
        if (!nativeType.isPrimitive()) {
            return dictionary == null ? new ToObjectPage<>(nativeType, codec) :
                    new ToPageWithDictionary<>(nativeType, new Dictionary<>(i -> {
                        byte[] bytes = dictionary.decodeToBinary(i).getBytes();
                        return codec.decode(bytes, 0, bytes.length);
                    }, dictionary.getMaxId() + 1));
        }

        throw new IllegalArgumentException("The native type for a Object column is " + nativeType.getCanonicalName());
    }

    private ToObjectPage(Class<T> nativeType, ObjectCodec<T> codec) {
        this.nativeType = nativeType;
        this.codec = codec;
    }

    @Override
    @NotNull
    public final Class getNativeType() {
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
        Binary [] from = (Binary[]) result;
        //noinspection unchecked
        T[] to = (T[])Array.newInstance(nativeType, from.length);

        for (int i = 0; i < to.length; ++i) {
            if (from[i] != null) {
                byte [] bytes = from[i].getBytes();
                to[i] = codec.decode(bytes, 0, bytes.length);
            }
        }

        return to;
    }
}
