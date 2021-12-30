/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ToIntPage and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_FLOAT_BOXED;

public class ToFloatPage<ATTR extends Any> implements ToPage<ATTR, float[]> {

    private static final ToFloatPage INSTANCE = new ToFloatPage<>();

    public static <ATTR extends Any> ToFloatPage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || float.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Float column is " + nativeType.getCanonicalName());
    }

    @SuppressWarnings("WeakerAccess")
    ToFloatPage() {}

    @Override
    @NotNull
    public final Class<Float> getNativeType() {
        return float.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Float;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_FLOAT_BOXED;
    }
}
