package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class ToCharPageFromInt<ATTR extends Any> implements ToPage<ATTR, char[]> {

    private static final ToCharPageFromInt INSTANCE = new ToCharPageFromInt<>();

    private static final Integer NULL_CHAR_AS_INT = (int) NULL_CHAR;

    public static <ATTR extends Any> ToCharPageFromInt<ATTR> create(Class<?> nativeType) {
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
