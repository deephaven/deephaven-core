package io.deephaven.engine.table.impl.updateby.hashing;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

/**
 * The basis for operators that participate in an updateBy operation.
 */
public interface ChunkedUpdateByStateManager {
    void add(final SafeCloseable bc,
             final RowSequence orderedKeys,
             final ColumnSource<?>[] sources,
             final MutableInt nextOutputPosition,
             final WritableIntChunk<RowKeys> outputPositions);

    default void remove(@NotNull final SafeCloseable pc,
                        @NotNull final RowSequence indexToRemove,
                        @NotNull final ColumnSource<?>[] sources,
                        @NotNull final WritableIntChunk<RowKeys> outputPositions)  {
        throw new UnsupportedOperationException("Remove is not supported");
    }

    default void findModifications(@NotNull final SafeCloseable pc,
                                   @NotNull final RowSequence modifiedIndex,
                                   @NotNull final ColumnSource<?> [] keySources,
                                   @NotNull final WritableIntChunk<RowKeys> outputPositions)  {
        throw new UnsupportedOperationException("Find is not supported");
    }

    SafeCloseable makeUpdateByBuildContext(ColumnSource<?>[] keySources, long updateSize);

    default SafeCloseable makeUpdateByProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        throw new UnsupportedOperationException("Cannot make a probe context.");
    }
}
