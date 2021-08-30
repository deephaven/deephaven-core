package io.deephaven.db.v2.utils;

interface ImplementedByTreeIndexImpl {
    /**
     * DO NOT USE!
     *
     * This method exists for use by internal index implementations when it is known that the Index
     * type must own a {@link TreeIndexImpl}.
     *
     * @return the backing TreeIndexImpl
     */
    TreeIndexImpl getImpl();

    /**
     * Override to improve index debug-tracing messages.
     */
    default String strid() {
        return "";
    }
}
