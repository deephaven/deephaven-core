//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ToLocalDatePage and run "./gradlew replicateToPage" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;

public class ToLocalDateTimePage<ATTR extends Any> implements ToPage<ATTR, LocalDateTime[]> {

    private static final ToLocalDateTimePage INSTANCE = new ToLocalDateTimePage<>();

    public static <ATTR extends Any> ToLocalDateTimePage<ATTR> create(final Class<?> nativeType) {
        if (nativeType == null || LocalDateTime.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }
        throw new IllegalArgumentException(
                "The native type for a LocalDateTime column is " + nativeType.getCanonicalName());
    }

    private ToLocalDateTimePage() {}

    @Override
    @NotNull
    public final Class<LocalDateTime> getNativeType() {
        return LocalDateTime.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }
}
