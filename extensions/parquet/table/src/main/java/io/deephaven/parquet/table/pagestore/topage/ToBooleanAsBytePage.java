package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.util.BooleanUtils;
import io.deephaven.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE_BOXED;

public class ToBooleanAsBytePage<ATTR extends Any> implements ToPage<ATTR, byte[]> {

    private static final ToBooleanAsBytePage INSTANCE = new ToBooleanAsBytePage<>();

    public static <ATTR extends Any> ToBooleanAsBytePage<ATTR> create(Class<?> nativeType) {
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
    public ObjectVector<Boolean> makeVector(byte[] result) {
        Boolean[] to = new Boolean[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = BooleanUtils.byteAsBoolean(result[i]);
        }

        return new ObjectVectorDirect<>(to);
    }
}
