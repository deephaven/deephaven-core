//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.hashing;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The basis for operators that participate in an updateBy operation.
 */
public abstract class UpdateByStateManager {

    protected final ColumnSource<?>[] keySourcesForErrorMessages;

    protected UpdateByStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        this.keySourcesForErrorMessages = keySourcesForErrorMessages;
    }

    // produce a pretty key for error messages
    protected String extractKeyStringFromSourceTable(long leftKey) {
        if (keySourcesForErrorMessages.length == 1) {
            return Objects.toString(keySourcesForErrorMessages[0].get(leftKey));
        }
        return "[" + Arrays.stream(keySourcesForErrorMessages).map(ls -> Objects.toString(ls.get(leftKey)))
                .collect(Collectors.joining(", ")) + "]";
    }

    public abstract void add(final boolean initialBuild,
            final SafeCloseable bc,
            final RowSequence orderedKeys,
            final ColumnSource<?>[] sources,
            final MutableInt nextOutputPosition,
            final WritableIntChunk<RowKeys> outputPositions);

    public void remove(@NotNull final SafeCloseable pc,
            @NotNull final RowSequence indexToRemove,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        throw new UnsupportedOperationException("Remove is not supported");
    }

    public void findModifications(@NotNull final SafeCloseable pc,
            @NotNull final RowSequence modifiedIndex,
            @NotNull final ColumnSource<?>[] keySources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        throw new UnsupportedOperationException("Find is not supported");
    }

    public abstract SafeCloseable makeUpdateByBuildContext(ColumnSource<?>[] keySources, long updateSize);

    public SafeCloseable makeUpdateByProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        throw new UnsupportedOperationException("Cannot make a probe context.");
    }
}
