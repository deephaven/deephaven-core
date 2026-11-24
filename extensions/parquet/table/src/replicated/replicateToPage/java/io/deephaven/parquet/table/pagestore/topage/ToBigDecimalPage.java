//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ToBigIntegerPage and run "./gradlew replicateToPage" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.BigDecimalFromUnsignedLongMaterializer;
import io.deephaven.parquet.base.materializers.BigDecimalFromBytesMaterializer;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.function.Function;

public class ToBigDecimalPage<ATTR extends Any> implements ToPage<ATTR, BigDecimal[]> {
    @SuppressWarnings("rawtypes")
    private static final ToBigDecimalPage FROM_UNSIGNED_LONG =
            new ToBigDecimalPage<>(BigDecimalFromUnsignedLongMaterializer.FACTORY);

    private final PageMaterializerFactory pageMaterializerFactory;

    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> create(
            final Class<?> nativeType,
            @NotNull final ObjectCodec<BigDecimal> codec,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier) {
        verifyNativeType(nativeType);
        if (dictionarySupplier == null) {
            return new ToBigDecimalPage<>(new BigDecimalFromBytesMaterializer.Factory(codec));
        }
        // Note that dictionary supplier is never null, even if it points to a NULL_DICTIONARY.
        // So we always use the following dictionary version of ToPage but internally, we check if the dictionary is
        // NULL and fall back to the default implementation.
        return new ToPageWithDictionary<>(
                BigDecimal.class,
                new ChunkDictionary<>(
                        (dictionary, key) -> codec.decode(dictionary.decodeToBinary(key).toByteBuffer()),
                        dictionarySupplier),
                (final Object result) -> (BigDecimal[]) result,
                new BigDecimalFromBytesMaterializer.Factory(codec));
    }

    /**
     * Needs no codec, unlike {@link #create}, because the values are fixed-width {@code INT64}.
     */
    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> createFromUnsignedLong(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_LONG;
    }


    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || BigDecimal.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException(
                "The native type for a BigDecimal column is " + nativeType.getCanonicalName());
    }

    private ToBigDecimalPage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @Override
    @NotNull
    public final Class<?> getNativeType() {
        return BigDecimal.class;
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
