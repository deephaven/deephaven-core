//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;

public interface ExposesChunkFilter {
    /**
     * Get the chunk filter for this filter.
     */
    ChunkFilter chunkFilter();
}
