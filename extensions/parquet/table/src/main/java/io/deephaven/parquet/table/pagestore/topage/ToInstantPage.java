//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

import static io.deephaven.util.QueryConstants.NULL_LONG_BOXED;

public class ToInstantPage<ATTR extends Any> implements ToPage<ATTR, long[]> {

    private static final ToInstantPage INSTANCE = new ToInstantPage<>();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, Instant[]> create(final Class<?> nativeType) {
        if (nativeType == null || Instant.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException(
                "The native type for an Instant column is " + nativeType.getCanonicalName());
    }

    private ToInstantPage() {}

    @Override
    @NotNull
    public final Class<Long> getNativeType() {
        return long.class;
    }

    @Override
    @NotNull
    public final Class<Instant> getNativeComponentType() {
        return Instant.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Long;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_LONG_BOXED;
    }

    @Override
    @NotNull
    public ObjectVector<Instant> makeVector(long[] result) {
        Instant[] to = new Instant[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = DateTimeUtils.epochNanosToInstant(result[i]);
        }
        return new ObjectVectorDirect<>(to);
    }
}
