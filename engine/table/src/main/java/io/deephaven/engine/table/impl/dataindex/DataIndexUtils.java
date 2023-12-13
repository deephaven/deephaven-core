package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.PrimaryDataIndex;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Tools for working with {@link PrimaryDataIndex data indices}.
 */
public class DataIndexUtils {

    /**
     * Make a {@link ChunkSource} that produces data index {@link PrimaryDataIndex.RowKeyLookup lookup} keys from
     * {@code keySources}.
     * 
     * @param keySources The individual key sources
     * @return The boxed key source
     */
    public static ChunkSource.WithPrev<Values> makeBoxedKeySource(final ColumnSource<?>... keySources) {
        switch (keySources.length) {
            case 0:
                throw new IllegalArgumentException("Data index must have at least one key column");
            case 1:
                return new SingleDataIndexBoxedKeySource(keySources[0]);
            default:
                return new CompoundDataIndexBoxedKeySource(keySources);
        }
    }

    /** Return true if the two keys are equal. */
    public static boolean keysEqual(@Nullable final Object key1, @NotNull final Object key2) {
        if (key1 instanceof Object[] && key2 instanceof Object[]) {
            return Arrays.equals((Object[]) key1, (Object[]) key2);
        }
        return Objects.equals(key1, key2);
    }
}
