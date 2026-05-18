//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

/**
 * Private helper methods for binary search kernels.
 */
class BinarySearchKernelHelper {
    /**
     * Private constructor to prevent instantiation.
     */
    private BinarySearchKernelHelper() {}

    /**
     * Helper to convert array index to insertion index (and back again).
     */
    static int insertionPoint(final int index) {
        return -index - 1;
    }
}
