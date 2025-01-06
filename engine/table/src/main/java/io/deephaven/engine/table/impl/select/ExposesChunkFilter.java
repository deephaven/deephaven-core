//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;

import java.util.Optional;

public interface ExposesChunkFilter {
    /**
     * Retrieve the underlying chunk filter for this filter if available.
     *
     * @return If available, returns the underlying chunk filter. Otherwise returns {@link Optional#empty()}.
     */
    Optional<ChunkFilter> chunkFilter();
}
