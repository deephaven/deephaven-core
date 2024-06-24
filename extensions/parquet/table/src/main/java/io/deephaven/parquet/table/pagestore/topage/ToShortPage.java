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

import static io.deephaven.util.QueryConstants.NULL_SHORT_BOXED;

public class ToShortPage<ATTR extends Any> implements ToPage<ATTR, short[]> {

    private static final ToShortPage INSTANCE = new ToShortPage<>();

    public static <ATTR extends Any> ToShortPage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || short.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Short column is " + nativeType.getCanonicalName());
    }

    private ToShortPage() {}

    @Override
    @NotNull
    public final Class<Short> getNativeType() {
        return short.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Short;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_SHORT_BOXED;
    }
}
