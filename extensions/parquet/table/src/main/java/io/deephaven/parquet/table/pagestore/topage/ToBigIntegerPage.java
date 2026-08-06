//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.BigIntegerFromUnsignedLongMaterializer;
import io.deephaven.parquet.base.materializers.BigIntegerMaterializer;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.function.Function;

public class ToBigIntegerPage<ATTR extends Any> implements ToPage<ATTR, BigInteger[]> {
    @SuppressWarnings("rawtypes")
    private static final ToBigIntegerPage FROM_UNSIGNED_LONG =
            new ToBigIntegerPage<>(BigIntegerFromUnsignedLongMaterializer.FACTORY);

    private final PageMaterializerFactory pageMaterializerFactory;

    public static <ATTR extends Any> ToPage<ATTR, BigInteger[]> create(
            final Class<?> nativeType,
            @NotNull final ObjectCodec<BigInteger> codec,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier) {
        verifyNativeType(nativeType);
        if (dictionarySupplier == null) {
            return new ToBigIntegerPage<>(new BigIntegerMaterializer.Factory(codec));
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

    /**
     * Create a {@link ToPage} for a parquet {@code INT64} column annotated with an unsigned 64-bit int logical type.
     * Such values do not fit in any Java primitive, so they are promoted to {@link BigInteger}.
     */
    public static <ATTR extends Any> ToPage<ATTR, BigInteger[]> createFromUnsignedLong(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_LONG;
    }


    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || BigInteger.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException(
                "The native type for a BigInteger column is " + nativeType.getCanonicalName());
    }

    private ToBigIntegerPage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
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
