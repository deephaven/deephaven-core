//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.ChunkType;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.ObjectMaterializer;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public class ToObjectPage<T, ATTR extends Any> implements ToPage<ATTR, T[]> {

    private final PageMaterializerFactory pageMaterializerFactory;
    private final Class<T> nativeType;

    public static <T, ATTR extends Any> ToPage<ATTR, T[]> create(
            final Class<T> nativeType,
            @NotNull final ObjectCodec<T> codec,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier) {
        if (!nativeType.isPrimitive()) {
            if (dictionarySupplier == null) {
                return new ToObjectPage<>(nativeType, codec);
            }
            // Note that dictionary supplier is never null, even if it points to a NULL_DICTIONARY.
            // So we always use the following dictionary version of ToPage but internally, we check if the dictionary is
            // NULL and fall back to the default implementation.
            // noinspection unchecked
            return new ToPageWithDictionary<>(
                    nativeType,
                    new ChunkDictionary<>(
                            (dictionary, key) -> codec.decode(dictionary.decodeToBinary(key).toByteBuffer()),
                            dictionarySupplier),
                    (final Object result) -> (T[]) result,
                    new ObjectMaterializer.Factory<>(codec, nativeType));
        }

        throw new IllegalArgumentException("The native type for a Object column is " + nativeType.getCanonicalName());
    }

    private ToObjectPage(Class<T> nativeType, ObjectCodec<T> codec) {
        this.nativeType = nativeType;
        this.pageMaterializerFactory = new ObjectMaterializer.Factory<>(codec, nativeType);
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
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }
}
