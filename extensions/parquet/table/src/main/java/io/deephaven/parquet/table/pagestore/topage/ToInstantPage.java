//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.InstantNanosFromInt96Materializer;
import io.deephaven.parquet.base.materializers.InstantNanosFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.InstantNanosFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LongMaterializer;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

import static io.deephaven.util.QueryConstants.NULL_LONG_BOXED;

public class ToInstantPage<ATTR extends Any> implements ToPage<ATTR, long[]> {

    public static <ATTR extends Any> ToPage<ATTR, Instant[]> createFromMillis(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_MILLIS;
    }

    public static <ATTR extends Any> ToPage<ATTR, Instant[]> createFromMicros(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_MICROS;
    }

    public static <ATTR extends Any> ToPage<ATTR, Instant[]> createFromNanos(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_NANOS;
    }

    public static <ATTR extends Any> ToPage<ATTR, Instant[]> createFromInt96(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_INT96;
    }

    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MILLIS = new ToInstantPage<>(InstantNanosFromMillisMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MICROS = new ToInstantPage<>(InstantNanosFromMicrosMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_NANOS = new ToInstantPage<>(LongMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_INT96 = new ToInstantPage<>(InstantNanosFromInt96Materializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType != null && !Instant.class.equals(nativeType)) {
            throw new IllegalArgumentException(
                    "The native type for an Instant column is " + nativeType.getCanonicalName());
        }
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToInstantPage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

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
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }

    @Override
    @NotNull
    public ObjectVector<Instant> makeVector(long[] result) {
        final Instant[] to = new Instant[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = DateTimeUtils.epochNanosToInstant(result[i]);
        }
        return new ObjectVectorDirect<>(to);
    }
}
