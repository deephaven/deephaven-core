//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.LongFromUnsignedIntMaterializer;
import io.deephaven.parquet.base.materializers.LongMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG_BOXED;

public abstract class ToLongPage<ATTR extends Any> implements ToPage<ATTR, long[]> {

    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_LONG_INSTANCE = new FromLong<>();

    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_UNSIGNED_INT_INSTANCE = new FromUnsignedInt();

    public static <ATTR extends Any> ToLongPage<ATTR> create(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_LONG_INSTANCE;
    }

    public static <ATTR extends Any> ToLongPage<ATTR> createFromUnsignedInt(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_INT_INSTANCE;
    }

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || long.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException("The native type for a Long column is " + nativeType.getCanonicalName());
    }

    private static final class FromLong<ATTR extends Any> extends ToLongPage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return LongMaterializer.Factory;
        }
    }

    private static final class FromUnsignedInt<ATTR extends Any> extends ToLongPage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return LongFromUnsignedIntMaterializer.Factory;
        }
    }

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
    public Object nullValue() {
        return NULL_LONG_BOXED;
    }
}
