/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ToIntPage and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG_BOXED;

public class ToLongPage<ATTR extends Attributes.Any> implements ToPage<ATTR, long[]> {

    private static final ToLongPage INSTANCE = new ToLongPage<>();

    public static <ATTR extends Attributes.Any> ToLongPage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || long.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Long column is " + nativeType.getCanonicalName());
    }

    @SuppressWarnings("WeakerAccess")
    ToLongPage() {}

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
        return NULL_LONG_BOXED;
    }
}
