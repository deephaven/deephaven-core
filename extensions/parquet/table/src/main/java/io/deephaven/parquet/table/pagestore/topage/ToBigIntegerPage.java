//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.BigIntegerMaterializer;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.function.Function;

public class ToBigIntegerPage<ATTR extends Any> implements ToPage<ATTR, BigInteger[]> {

    private final PageMaterializerFactory pageMaterializerFactory;

    public static <ATTR extends Any> ToPage<ATTR, BigInteger[]> create(
            final Class<?> nativeType,
            @NotNull final ObjectCodec<BigInteger> codec,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier) {
        if (nativeType == null || BigInteger.class.equals(nativeType)) {
            if (dictionarySupplier == null) {
                return new ToBigIntegerPage<>(codec);
            }
            // Note that dictionary supplier is never null, even if it points to a NULL_DICTIONARY.
            // So we always use the following dictionary version of ToPage but internally, we check if the dictionary is
            // NULL and fall back to the default implementation.
            return new ToPageWithDictionary<>(
                    BigInteger.class,
                    new ChunkDictionary<>(
                            (dictionary, key) -> codec.decode(dictionary.decodeToBinary(key).toByteBuffer()),
                            dictionarySupplier),
                    (final Object result) -> (BigInteger[]) result,
                    new BigIntegerMaterializer.Factory(codec));
        }
        throw new IllegalArgumentException(
                "The native type for a BigInteger column is " + nativeType.getCanonicalName());
    }

    private ToBigIntegerPage(@NotNull final ObjectCodec<BigInteger> codec) {
        pageMaterializerFactory = new BigIntegerMaterializer.Factory(codec);
    }

    @Override
    @NotNull
    public final Class<?> getNativeType() {
        return BigInteger.class;
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
