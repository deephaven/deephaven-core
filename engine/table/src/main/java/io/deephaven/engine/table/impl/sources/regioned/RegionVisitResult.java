package io.deephaven.engine.table.impl.sources.regioned;

/**
 * Result enum for region-visit patterns.
 */
public enum RegionVisitResult {

    /**
     * Returned if the visit operation failed for the current region.
     */
    FAILED,

    /**
     * Returned if the visit operation succeeded for the current region
     */
    CONTINUE,

    /**
     * Returned if the visit operation succeeded for the last region.
     */
    COMPLETE
}
