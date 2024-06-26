//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ToBigIntegerPage and run "./gradlew replicateToPage" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.BigDecimalFromBytesMaterializer;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.function.Function;

public class ToBigDecimalPage<ATTR extends Any> implements ToPage<ATTR, BigDecimal[]> {

    private final PageMaterializerFactory pageMaterializerFactory;

    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> create(
            final Class<?> nativeType,
            @NotNull final ObjectCodec<BigDecimal> codec,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier) {
        if (nativeType == null || BigDecimal.class.equals(nativeType)) {
            if (dictionarySupplier == null) {
                return new ToBigDecimalPage<>(codec);
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
        throw new IllegalArgumentException(
                "The native type for a BigDecimal column is " + nativeType.getCanonicalName());
    }

    private ToBigDecimalPage(@NotNull final ObjectCodec<BigDecimal> codec) {
        pageMaterializerFactory = new BigDecimalFromBytesMaterializer.Factory(codec);
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
