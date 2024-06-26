//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.InstantFromInt96Materializer;
import io.deephaven.parquet.base.materializers.InstantNanosFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.InstantNanosFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LongMaterializer;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

import static io.deephaven.util.QueryConstants.NULL_LONG_BOXED;

public class ToInstantPage<ATTR extends Any> implements ToPage<ATTR, long[]> {

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, Instant[]> create(
            final Class<?> nativeType,
            final LogicalTypeAnnotation.TimeUnit unit) {
        if (nativeType == null || Instant.class.equals(nativeType)) {
            switch (unit) {
                case MILLIS:
                    return FROM_MILLIS;
                case MICROS:
                    return FROM_MICROS;
                case NANOS:
                    return FROM_NANOS;
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + unit);
            }
        }
        throw new IllegalArgumentException(
                "The native type for an Instant column is " + nativeType.getCanonicalName());
    }

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, Instant[]> create(final Class<?> nativeType) {
        if (nativeType == null || Instant.class.equals(nativeType)) {
            return FROM_INT96;
        }
        throw new IllegalArgumentException(
                "The native type for an Instant column is " + nativeType.getCanonicalName());
    }

    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MILLIS = new ToInstantPage<>(InstantNanosFromMillisMaterializer.Factory);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MICROS = new ToInstantPage<>(InstantNanosFromMicrosMaterializer.Factory);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_NANOS = new ToInstantPage<>(LongMaterializer.Factory);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_INT96 = new ToInstantPage<>(InstantFromInt96Materializer.Factory);

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
