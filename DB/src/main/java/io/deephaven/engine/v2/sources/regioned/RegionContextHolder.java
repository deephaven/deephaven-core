package io.deephaven.engine.v2.sources.regioned;

import io.deephaven.engine.rftable.ChunkSource;

public class RegionContextHolder implements ChunkSource.FillContext {
    // Currently mo column regions use a non-default context.
    // If that changes, we'll need to add indirection and/or caching here, switching out contexts on region boundaries.
}
