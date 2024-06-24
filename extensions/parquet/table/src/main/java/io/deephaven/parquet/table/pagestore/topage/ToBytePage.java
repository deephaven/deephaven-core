//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ToIntPage and run "./gradlew replicateToPage" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_BYTE_BOXED;

public class ToBytePage<ATTR extends Any> implements ToPage<ATTR, byte[]> {

    private static final ToBytePage INSTANCE = new ToBytePage<>();

    public static <ATTR extends Any> ToBytePage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || byte.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Byte column is " + nativeType.getCanonicalName());
    }

    private ToBytePage() {}

    @Override
    @NotNull
    public final Class<Byte> getNativeType() {
        return byte.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Byte;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_BYTE_BOXED;
    }
}
