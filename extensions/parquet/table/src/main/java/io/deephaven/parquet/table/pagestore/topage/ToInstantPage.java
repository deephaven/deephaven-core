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

public abstract class ToInstantPage<ATTR extends Any> implements ToPage<ATTR, long[]> {

    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MILLIS_INSTANCE = new FromMillis();
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MICROS_INSTANCE = new FromMicros();
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_NANOS_INSTANCE = new FromNanos();
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_INT96_INSTANCE = new FromInt96();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, Instant[]> create(
            final Class<?> nativeType,
            final LogicalTypeAnnotation.TimeUnit unit) {
        if (nativeType == null || Instant.class.equals(nativeType)) {
            switch (unit) {
                case MILLIS:
                    return FROM_MILLIS_INSTANCE;
                case MICROS:
                    return FROM_MICROS_INSTANCE;
                case NANOS:
                    return FROM_NANOS_INSTANCE;
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
            return FROM_INT96_INSTANCE;
        }
        throw new IllegalArgumentException(
                "The native type for an Instant column is " + nativeType.getCanonicalName());
    }

    private static final class FromMillis<ATTR extends Any> extends ToInstantPage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return InstantNanosFromMillisMaterializer.Factory;
        }
    }

    private static final class FromMicros<ATTR extends Any> extends ToInstantPage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return InstantNanosFromMicrosMaterializer.Factory;
        }
    }

    private static final class FromNanos<ATTR extends Any> extends ToInstantPage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return LongMaterializer.Factory;
        }
    }

    private static final class FromInt96<ATTR extends Any> extends ToInstantPage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return InstantFromInt96Materializer.Factory;
        }
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
    public ObjectVector<Instant> makeVector(long[] result) {
        final Instant[] to = new Instant[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = DateTimeUtils.epochNanosToInstant(result[i]);
        }
        return new ObjectVectorDirect<>(to);
    }
}
