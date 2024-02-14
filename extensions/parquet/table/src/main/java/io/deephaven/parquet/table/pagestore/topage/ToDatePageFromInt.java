/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;

import static io.deephaven.util.QueryConstants.NULL_INT_BOXED;

public class ToDatePageFromInt<ATTR extends Any> implements ToPage<ATTR, LocalDate[]> {

    private static final ToDatePageFromInt INSTANCE = new ToDatePageFromInt<>();

    public static <ATTR extends Any> ToDatePageFromInt<ATTR> create(final Class<?> nativeType) {
        if (nativeType == null || LocalDate.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Date column is " + nativeType.getCanonicalName());
    }

    private ToDatePageFromInt() {}

    @Override
    @NotNull
    public final Class<LocalDate> getNativeType() {
        return LocalDate.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_INT_BOXED;
    }

    @Override
    @NotNull
    public final LocalDate[] convertResult(final Object result) {
        final int[] from = (int[]) result;
        final LocalDate[] to = new LocalDate[from.length];

        for (int i = 0; i < from.length; ++i) {
            to[i] = DateTimeUtils.epochDaysAsIntToLocalDate(from[i]);
        }
        return to;
    }
}
