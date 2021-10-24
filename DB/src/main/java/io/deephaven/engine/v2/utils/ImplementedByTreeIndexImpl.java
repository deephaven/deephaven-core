package io.deephaven.engine.v2.utils;

interface ImplementedByTreeIndexImpl {
    /**
     * DO NOT USE!
     *
     * This method exists for use by internal rowSet implementations when it is known that the TrackingMutableRowSet type must own a
     * {@link TreeIndexImpl}.
     *
     * @return the backing TreeIndexImpl
     */
    TreeIndexImpl getImpl();

    /**
     * Override to improve rowSet debug-tracing messages.
     */
    default String strid() {
        return "";
    }
}
