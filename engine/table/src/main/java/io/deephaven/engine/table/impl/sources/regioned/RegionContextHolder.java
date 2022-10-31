/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.Nullable;

public class RegionContextHolder implements ChunkSource.FillContext {
    private final int chunkCapacity;
    private final SharedContext sharedContext;
    private Object innerContext;

    public RegionContextHolder(int chunkCapacity, SharedContext sharedContext) {
        this.chunkCapacity = chunkCapacity;
        this.sharedContext = sharedContext;
    }

    @Override
    public boolean supportsUnboundedFill() {
        return true;
    }

    public void setInnerContext(@Nullable final Object contextObject) {
        this.innerContext = contextObject;
    }

    public int getChunkCapacity() {
        return chunkCapacity;
    }

    public SharedContext getSharedContext() {
        return sharedContext;
    }

    public <T> T getInnerContext() {
        //noinspection unchecked
        return (T)innerContext;
    }
}
