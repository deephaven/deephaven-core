//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.value.iterator;

import io.deephaven.engine.primitive.iterator.CloseableIterator;

public interface ValueIterator<TYPE> extends CloseableIterator<TYPE> {
    /**
     * @return The number of elements remaining in this ColumnIterator
     */
    long remaining();

}
