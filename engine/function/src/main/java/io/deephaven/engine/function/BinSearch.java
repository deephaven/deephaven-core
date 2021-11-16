/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.function;

/**
 * Algorithm used to resolve ties when performing a binary search.
 */
public enum BinSearch {
    /**
     * Binary search algorithm returns any matching rowSet.
     */
    BS_ANY,

    /**
     * Binary search algorithm returns the highest matching rowSet.
     */
    BS_HIGHEST,

    /**
     * Binary search algorithm returns the lowest matching rowSet.
     */
    BS_LOWEST
}
