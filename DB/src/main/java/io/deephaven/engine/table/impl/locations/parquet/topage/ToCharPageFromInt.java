package io.deephaven.engine.table.impl.locations.parquet.topage;

import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class ToCharPageFromInt<ATTR extends Attributes.Any> implements ToPage<ATTR, char[]> {

    private static final ToCharPageFromInt INSTANCE = new ToCharPageFromInt<>();

    private static final Integer NULL_CHAR_AS_INT = (int) NULL_CHAR;

    public static <ATTR extends Attributes.Any> ToCharPageFromInt<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || char.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Char column is " + nativeType.getCanonicalName());
    }

    private ToCharPageFromInt() {}

    @Override
    @NotNull
    public final Class<Character> getNativeType() {
        return char.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Char;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_CHAR_AS_INT;
    }

    @Override
    @NotNull
    public final char[] convertResult(Object result) {
        int [] from = (int []) result;
        char [] to = new char [from.length];

        for (int i = 0; i < from.length; ++i) {
            to[i] = (char) from[i];
        }

        return to;
    }
}
