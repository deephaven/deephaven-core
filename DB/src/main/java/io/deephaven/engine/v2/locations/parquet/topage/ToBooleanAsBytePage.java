package io.deephaven.engine.v2.locations.parquet.topage;

import io.deephaven.engine.tables.dbarrays.ObjectVector;
import io.deephaven.engine.tables.dbarrays.ObjectVectorDirect;
import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE_BOXED;

public class ToBooleanAsBytePage<ATTR extends Attributes.Any> implements ToPage<ATTR, byte[]> {

    private static final ToBooleanAsBytePage INSTANCE = new ToBooleanAsBytePage<>();

    public static <ATTR extends Attributes.Any> ToBooleanAsBytePage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || Boolean.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a BooleanAsByte column is " + nativeType.getCanonicalName());
    }

    private ToBooleanAsBytePage()
    {}

    @Override
    @NotNull
    public final Class<Byte> getNativeType() {
        return byte.class;
    }

    @Override
    @NotNull
    public final Class<Boolean> getNativeComponentType() {
        return Boolean.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Byte;
    }

    @Override
    public final Object nullValue() {
        return NULL_BOOLEAN_AS_BYTE_BOXED;
    }

    @Override
    @NotNull
    public ObjectVector<Boolean> makeDbArray(byte[] result) {
        Boolean[] to = new Boolean[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = BooleanUtils.byteAsBoolean(result[i]);
        }

        return new ObjectVectorDirect<>(to);
    }
}
