//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.BigDecimalParquetBytesCodec;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.function.Function;

public class ToBigDecimalPage<ATTR extends Any> implements ToPage<ATTR, BigDecimal[]> {

    private static final ToBigDecimalPage<? extends Any> INSTANCE = new ToBigDecimalPage<>();

    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> create(
            final Class<?> nativeType,
            final int precision,
            final int scale,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier,
            final int encodedSizeInBytes) {
        if (BigDecimal.class.equals(nativeType)) {
            if (dictionarySupplier == null) {
                // noinspection unchecked
                return (ToPage<ATTR, BigDecimal[]>) INSTANCE;
            }
            // Codec is only used for dictionary encoding
            // TODO Currently codec is always made, even if dictionary is NONE
            final ObjectCodec<BigDecimal> codec = new BigDecimalParquetBytesCodec(precision, scale, encodedSizeInBytes);
            return new ToPageWithDictionary<>(
                    BigDecimal.class,
                    new ChunkDictionary<>(
                            (dictionary, key) -> {
                                final byte[] bytes = dictionary.decodeToBinary(key).getBytes();
                                return codec.decode(bytes, 0, bytes.length);
                            },
                            dictionarySupplier),
                    INSTANCE::convertResult);
        }
        throw new IllegalArgumentException(
                "The native type for a BigDecimal column is " + nativeType.getCanonicalName());
    }

    private ToBigDecimalPage() {}

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
}
