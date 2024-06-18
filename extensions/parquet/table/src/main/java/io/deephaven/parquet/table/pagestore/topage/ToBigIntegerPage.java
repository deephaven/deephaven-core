//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.BigIntegerParquetBytesCodec;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.function.Function;

public class ToBigIntegerPage<ATTR extends Any> implements ToPage<ATTR, BigInteger[]> {

    private static final ToBigIntegerPage<? extends Any> INSTANCE = new ToBigIntegerPage<>();

    public static <ATTR extends Any> ToPage<ATTR, BigInteger[]> create(
            final Class<?> nativeType,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier,
            final int encodedSizeInBytes) {
        if (nativeType == null || BigInteger.class.equals(nativeType)) {
            if (dictionarySupplier == null) {
                // noinspection unchecked
                return (ToPage<ATTR, BigInteger[]>) INSTANCE;
            }
            // Codec is only used for dictionary encoding
            final ObjectCodec<BigInteger> codec = new BigIntegerParquetBytesCodec(encodedSizeInBytes);
            return new ToPageWithDictionary<>(
                    BigInteger.class,
                    new ChunkDictionary<>(
                            (dictionary, key) -> {
                                final byte[] bytes = dictionary.decodeToBinary(key).getBytes();
                                return codec.decode(bytes, 0, bytes.length);
                            },
                            dictionarySupplier),
                    INSTANCE::convertResult);
        }
        throw new IllegalArgumentException(
                "The native type for a BigInteger column is " + nativeType.getCanonicalName());
    }

    private ToBigIntegerPage() {}

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
}
