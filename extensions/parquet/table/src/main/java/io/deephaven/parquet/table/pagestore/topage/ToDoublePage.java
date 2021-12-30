/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ToIntPage and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE_BOXED;

public class ToDoublePage<ATTR extends Any> implements ToPage<ATTR, double[]> {

    private static final ToDoublePage INSTANCE = new ToDoublePage<>();

    public static <ATTR extends Any> ToDoublePage<ATTR> create(Class<?> nativeType) {
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
}
