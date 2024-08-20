//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.ChunkType;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.StringMaterializer;
import io.deephaven.util.channel.SeekableChannelContext;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public class ToStringPage<ATTR extends Any> implements ToPage<ATTR, String[]> {

    private static final ToStringPage<? extends Any> INSTANCE = new ToStringPage<>();

    public static <ATTR extends Any> ToPage<ATTR, String[]> create(
            final Class<?> nativeType,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier) {
        if (nativeType == null || String.class.equals(nativeType)) {
            // noinspection unchecked
            return dictionarySupplier == null ? (ToPage<ATTR, String[]>) INSTANCE
                    : new ToPageWithDictionary<>(
                            String.class,
                            new ChunkDictionary<>(
                                    (dictionary, key) -> dictionary.decodeToBinary(key).toStringUsingUTF8(),
                                    dictionarySupplier),
                            INSTANCE::convertResult,
                            INSTANCE.getPageMaterializerFactory());
        }

        throw new IllegalArgumentException(
                "The native type for a String column is " + nativeType.getCanonicalName());
    }

    @Override
    @NotNull
    public final Class<?> getNativeType() {
        return String.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return StringMaterializer.FACTORY;
    }
}
