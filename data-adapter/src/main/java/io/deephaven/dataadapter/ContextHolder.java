//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;

/**
 * An AutoCloseable holder for a shared context and related GetContexts.
 */
public class ContextHolder implements AutoCloseable {
    private final SharedContext sharedContext;
    private final ChunkSource.GetContext[] getContexts;

    public ContextHolder(final int chunkSize, final ColumnSource<?>... columnSources) {
        sharedContext = SharedContext.makeSharedContext();
        getContexts = new ChunkSource.GetContext[columnSources.length];
        for (int i = 0; i < columnSources.length; i++) {
            getContexts[i] = columnSources[i].makeGetContext(chunkSize, sharedContext);
        }
    }

    /**
     * Gets the GetContext for the {@code nth} column source.
     *
     * @param n The index of the column source
     * @return The GetContext for the {@code nth} column source
     */
    public ChunkSource.GetContext getGetContext(int n) {
        return getContexts[n];
    }

    @Override
    public void close() throws Exception {
        for (ChunkSource.GetContext getContext : getContexts) {
            getContext.close();
        }
        sharedContext.close();
    }
}
