/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ToIntPage and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE_BOXED;

public class ToDoublePage<ATTR extends Attributes.Any> implements ToPage<ATTR, double[]> {

    private static final ToDoublePage INSTANCE = new ToDoublePage<>();

    public static <ATTR extends Attributes.Any> ToDoublePage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || double.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Double column is " + nativeType.getCanonicalName());
    }

    @SuppressWarnings("WeakerAccess")
    ToDoublePage() {}

    @Override
    @NotNull
    public final Class<Double> getNativeType() {
        return double.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Double;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_DOUBLE_BOXED;
    }

    @Override
    public double[] convertResult(Object result) {
        //noinspection unchecked
        throw new IllegalStateException("failed!");
    }
}
