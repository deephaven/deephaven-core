/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ToCharPageFromInt and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.locations.parquet.topage;

import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ToBytePageFromInt<ATTR extends Attributes.Any> implements ToPage<ATTR, byte[]> {

    private static final ToBytePageFromInt INSTANCE = new ToBytePageFromInt<>();

    private static final Integer NULL_BYTE_AS_INT = (int) NULL_BYTE;

    public static <ATTR extends Attributes.Any> ToBytePageFromInt<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || byte.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Byte column is " + nativeType.getCanonicalName());
    }

    private ToBytePageFromInt() {}

    @Override
    @NotNull
    public final Class<Byte> getNativeType() {
        return byte.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Byte;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_BYTE_AS_INT;
    }

    @Override
    @NotNull
    public final byte[] convertResult(Object result) {
        int [] from = (int []) result;
        byte [] to = new byte [from.length];

        for (int i = 0; i < from.length; ++i) {
            to[i] = (byte) from[i];
        }

        return to;
    }
}
