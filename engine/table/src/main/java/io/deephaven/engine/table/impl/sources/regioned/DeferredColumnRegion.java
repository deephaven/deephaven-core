/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;

/**
 * Adds region factory for deferred column regions implementations. Deferred regions serve as placeholders, constructing
 * (and usually swapping themselves for) the "real" region on first access.
 */
interface DeferredColumnRegion<ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>>
        extends ColumnRegion<ATTR> {

    /**
     * Get (and possibly construct) the "real" region whose construction was deferred.
     *
     * @return The "real" region whose construction was deferred
     */
    REGION_TYPE getResultRegion();

    static <ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>, INNER_REGION_TYPE extends REGION_TYPE> REGION_TYPE materialize(
            INNER_REGION_TYPE region) {
        // noinspection unchecked
        return region instanceof DeferredColumnRegion
                ? ((DeferredColumnRegion<ATTR, REGION_TYPE>) region).getResultRegion()
                : region;
    }
}
