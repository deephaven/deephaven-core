//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_INT_BOXED;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class ToLongPageFromUnsignedInt<ATTR extends Any> implements ToPage<ATTR, long[]> {

    private static final ToLongPageFromUnsignedInt INSTANCE = new ToLongPageFromUnsignedInt<>();

    public static <ATTR extends Any> ToLongPageFromUnsignedInt<ATTR> create(final Class<?> nativeType) {
        if (nativeType == null || long.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }
        throw new IllegalArgumentException("The native type for a Long column is " + nativeType.getCanonicalName());
    }

    private ToLongPageFromUnsignedInt() {}

    @Override
    @NotNull
    public final Class<Long> getNativeType() {
        return long.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Long;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_INT_BOXED;
    }

    @Override
    public final long[] convertResult(final Object result) {
        final int[] from = (int[]) result;
        final long[] to = new long[from.length];
        for (int i = 0; i < from.length; ++i) {
            final int fromValue = from[i];
            to[i] = fromValue == NULL_INT ? NULL_LONG : Integer.toUnsignedLong(fromValue);
        }
        return to;
    }
}
