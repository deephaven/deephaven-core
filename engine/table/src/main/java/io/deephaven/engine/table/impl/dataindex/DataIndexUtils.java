package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;

/**
 * Tools for working with {@link io.deephaven.engine.table.DataIndex data indices}.
 */
public class DataIndexUtils {

    /**
     * Make a {@link ChunkSource} that produces data index {@link io.deephaven.engine.table.DataIndex.RowSetLookup row
     * set lookup} keys from {@code keySources}.
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
}
