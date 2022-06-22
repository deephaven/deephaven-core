/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.function;

/**
 * Algorithm used to resolve ties when performing a binary search.
 */
public enum BinSearchAlgo {
    /**
     * Binary search algorithm returns any matching index.
     */
    BS_ANY,

    /**
     * Binary search algorithm returns the highest matching index.
     */
    BS_HIGHEST,

    /**
     * Binary search algorithm returns the lowest matching index.
     */
    BS_LOWEST
}
