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

import java.time.LocalTime;

public class ToLocalTimePage<ATTR extends Any> implements ToPage<ATTR, LocalTime[]> {

    private static final ToLocalTimePage INSTANCE = new ToLocalTimePage<>();

    public static <ATTR extends Any> ToLocalTimePage<ATTR> create(final Class<?> nativeType) {
        if (nativeType == null || LocalTime.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }
        throw new IllegalArgumentException(
                "The native type for a LocalTime column is " + nativeType.getCanonicalName());
    }

    private ToLocalTimePage() {}

    @Override
    @NotNull
    public final Class<LocalTime> getNativeType() {
        return LocalTime.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }
}
