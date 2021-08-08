package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

public class ToStringPage<ATTR extends Attributes.Any> implements ToPage<ATTR, String[]> {

    static final ToStringPage INSTANCE = new ToStringPage();

    public static <ATTR extends Attributes.Any>
    ToPage<ATTR, String[]> create(final Class<?> nativeType, final org.apache.parquet.column.Dictionary dictionary) {
        if (nativeType == null || String.class.equals(nativeType)) {
            //noinspection unchecked
            return dictionary == null
                    ? INSTANCE
                    : new ToPageWithDictionary<>(
                            String.class,
                            new Dictionary<>(
                                    dictionaryKey -> dictionary.decodeToBinary(dictionaryKey).toStringUsingUTF8(),
                                    dictionary.getMaxId() + 1),
                            INSTANCE::convertResult);
        }

        throw new IllegalArgumentException("The native type for a String column is " + nativeType.getCanonicalName());
    }

    private ToStringPage() {
    }

    @Override
    @NotNull
    public final Class getNativeType() {
        return String.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final String[] convertResult(final Object result) {
        final Binary[] from = (Binary[]) result;
        final String[] to = new String[from.length];
        for (int i = 0; i < to.length; ++i) {
            if (from[i] != null) {
                to[i] = from[i].toStringUsingUTF8();
            }
        }
        return to;
    }
}
