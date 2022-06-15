/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue.Dependency;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

/**
 * A ConstituentDependency is used to establish a {@link Dependency dependency} relationship between a table and the
 * constituent tables (or table-like things) that fill its columns.
 */
public class ConstituentDependency implements Dependency {

    /**
     * If {@code result} has any columns of {@link Dependency dependencies}, construct and install a
     * ConstituentDependency as a {@link Table#addParentReference(Object) parent}.
     *
     * @param result The result {@link Table} that may contain columns of constituent dependencies
     * @param resultUpdatedDependency A parent {@link Dependency dependency} used to determine if the result table is
     *        done updating for this cycle. This is typically the {@link InstrumentedTableListenerBase} or
     *        {@link MergedListener} responsible for maintaining the result table, which will be {@link #satisfied(long)
     *        satisfied} if it has processed its notification or if it will never fire on this cycle.
     */
    public static void install(
            @NotNull final Table result,
            @NotNull final Dependency resultUpdatedDependency) {
        // noinspection unchecked
        final ColumnSource<? extends Dependency>[] dependencyColumns = result.getColumnSources().stream()
                .filter(cs -> Dependency.class.isAssignableFrom(cs.getType()))
                .toArray(ColumnSource[]::new);
        if (dependencyColumns.length == 0) {
            return;
        }
        result.addParentReference(
                new ConstituentDependency(resultUpdatedDependency, result.getRowSet(), dependencyColumns));
    }

    /**
     * A {@link Dependency dependency} used to determine if the result table is done updating for this cycle. See
     * {@link #install(Table, Dependency)} for more information.
     */
    private final Dependency resultUpdatedDependency;
    private final RowSet resultRows;
    private final ColumnSource<? extends Dependency>[] dependencyColumns;

    private volatile long lastSatisfiedStep;

    private ConstituentDependency(
            @NotNull final Dependency resultUpdatedDependency,
            @NotNull final RowSet resultRows,
            @NotNull final ColumnSource<? extends Dependency>[] dependencyColumns) {
        this.resultUpdatedDependency = resultUpdatedDependency;
        this.resultRows = resultRows;
        this.dependencyColumns = dependencyColumns;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("ConstituentDependency-").append(System.identityHashCode(this));
    }

    @Override
    public boolean satisfied(final long step) {
        if (lastSatisfiedStep == step) {
            return true;
        }
        if (!resultUpdatedDependency.satisfied(step)) {
            UpdateGraphProcessor.DEFAULT.logDependencies()
                    .append("Result updated dependency not satisfied for ").append(this)
                    .append(", dependency=").append(resultUpdatedDependency)
                    .endl();
            return false;
        }
        // Now that we know the result is updated (or won't be), it's safe to look at current contents.
        if (resultRows.isEmpty()) {
            lastSatisfiedStep = step;
            return true;
        }
        final int chunkSize = Math.toIntExact(Math.min(ColumnIterator.DEFAULT_CHUNK_SIZE, resultRows.size()));
        final int numColumns = dependencyColumns.length;
        final ChunkSource.GetContext[] contexts = new ChunkSource.GetContext[numColumns];
        try (final SharedContext sharedContext = numColumns > 1 ? SharedContext.makeSharedContext() : null;
                final SafeCloseable ignored = new SafeCloseableArray<>(contexts);
                final RowSequence.Iterator rows = resultRows.getRowSequenceIterator()) {
            for (int ci = 0; ci < numColumns; ++ci) {
                contexts[ci] = dependencyColumns[ci].makeGetContext(chunkSize, sharedContext);
            }
            while (rows.hasMore()) {
                final RowSequence sliceRows = rows.getNextRowSequenceWithLength(chunkSize);
                final int numConstituents = sliceRows.intSize();
                for (int ci = 0; ci < numColumns; ++ci) {
                    final ObjectChunk<? extends Dependency, ? extends Values> dependencies =
                            dependencyColumns[ci].getChunk(contexts[ci], sliceRows).asObjectChunk();
                    for (int di = 0; di < numConstituents; ++di) {
                        final Dependency constituent = dependencies.get(di);
                        if (constituent != null && !constituent.satisfied(step)) {
                            UpdateGraphProcessor.DEFAULT.logDependencies()
                                    .append("Constituent dependencies not satisfied for ")
                                    .append(this).append(", constituent=").append(constituent)
                                    .endl();
                            return false;
                        }
                    }
                }
                if (sharedContext != null) {
                    sharedContext.reset();
                }
            }
        }
        UpdateGraphProcessor.DEFAULT.logDependencies()
                .append("All constituent dependencies satisfied for ").append(this)
                .endl();
        lastSatisfiedStep = step;
        return true;
    }
}
