package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

public class ToStringPage<ATTR extends Attributes.Any> implements ToPage<ATTR, String[]> {

    private static final ToStringPage INSTANCE = new ToStringPage();

    public static <ATTR extends Attributes.Any>
    ToPage<ATTR, String[]> create(Class<?> nativeType, org.apache.parquet.column.Dictionary dictionary) {
        if (nativeType == null || String.class.equals(nativeType)) {
            //noinspection unchecked
            return dictionary == null ? INSTANCE :
                    new ToPageWithDictionary<>(String.class, new Dictionary<>( i ->
                            dictionary.decodeToBinary(i).toStringUsingUTF8(),
                            dictionary.getMaxId()+1));
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
    public final String[] convertResult(Object result) {
        Binary [] from = (Binary[]) result;
        String[] to = ObjectChunk.makeArray(from.length);

        for (int i = 0; i < to.length; ++i) {
            if (from[i] != null) {
                to[i] = from[i].toStringUsingUTF8();
            }
        }

        return to;
    }
}
