/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.ChunkType;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

public class ToStringPage<ATTR extends Any> implements ToPage<ATTR, String[]> {

    static final ToStringPage<? extends Any> INSTANCE = new ToStringPage<>();

    public static <ATTR extends Any> ToPage<ATTR, String[]> create(
            final Class<?> nativeType,
            final Supplier<Dictionary> dictionarySupplier) {
        if (nativeType == null || String.class.equals(nativeType)) {
            // noinspection unchecked
            return dictionarySupplier == null ? (ToPage<ATTR, String[]>) INSTANCE
                    : new ToPageWithDictionary<>(
                            String.class,
                            new ChunkDictionary<>(
                                    (dictionary, key) -> dictionary.decodeToBinary(key).toStringUsingUTF8(),
                                    dictionarySupplier),
                            INSTANCE::convertResult);
        }

        throw new IllegalArgumentException(
                "The native type for a String column is " + nativeType.getCanonicalName());
    }

    private ToStringPage() {}

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
    public final String[] convertResult(final Object result) {
        final Binary[] from = (Binary[]) result;
        final String[] to = new String[from.length];
        for (int ri = 0; ri < to.length; ++ri) {
            if (from[ri] != null) {
                to[ri] = from[ri].toStringUsingUTF8();
            }
        }
        return to;
    }
}
