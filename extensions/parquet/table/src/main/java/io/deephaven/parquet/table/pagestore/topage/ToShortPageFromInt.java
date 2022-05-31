/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ToCharPageFromInt and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ToShortPageFromInt<ATTR extends Any> implements ToPage<ATTR, short[]> {

    private static final ToShortPageFromInt INSTANCE = new ToShortPageFromInt<>();

    private static final Integer NULL_SHORT_AS_INT = (int) NULL_SHORT;

    public static <ATTR extends Any> ToShortPageFromInt<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || short.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Short column is " + nativeType.getCanonicalName());
    }

    private ToShortPageFromInt() {}

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
        return NULL_SHORT_AS_INT;
    }

    @Override
    @NotNull
    public final short[] convertResult(Object result) {
        int [] from = (int []) result;
        short [] to = new short [from.length];

        for (int i = 0; i < from.length; ++i) {
            to[i] = (short) from[i];
        }

        return to;
    }
}
