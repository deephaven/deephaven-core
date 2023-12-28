/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.Nullable;

public class RegionContextHolder implements ChunkSource.FillContext {
    private final int chunkCapacity;
    private final SharedContext sharedContext;
    private Context innerContext;

    public RegionContextHolder(final int chunkCapacity, @Nullable final SharedContext sharedContext) {

        this.chunkCapacity = chunkCapacity;
        this.sharedContext = sharedContext;
    }

    @Override
    public boolean supportsUnboundedFill() {
        return true;
    }

    /**
     * Set the inner wrapped context object for use by downstream regions.
     * 
     * @param contextObject The context object
     */
    public void setInnerContext(@Nullable final Context contextObject) {
        this.innerContext = contextObject;
    }

    /**
     * Get the chunk capacity this holder was created with.
     * 
     * @return The chunk capacity
     */
    public int getChunkCapacity() {
        return chunkCapacity;
    }

    /**
     * Get the {@link SharedContext} this holder was created with.
     * 
     * @return The {@link SharedContext}
     */
    public SharedContext getSharedContext() {
        return sharedContext;
    }

    /**
     * Get the inner context value set by {@link #setInnerContext(Context)} and cast it to the templated type.
     * 
     * @return The inner context value.
     * @param <T> The desired result type
     */
    public <T extends Context> T getInnerContext() {
        // noinspection unchecked
        return (T) innerContext;
    }

    @Override
    public void close() {
        if (innerContext != null) {
            innerContext.close();
            innerContext = null;
        }
    }
}
