/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.tablelogger;

import java.io.IOException;

/**
 * A container for a Row that supports the concept of release, as well as access to the row itself.
 */
public interface WritableRowContainer<R extends Row> {
    /**
     * @return The row of this container
     */
    R getRow();

    /**
     * Write the underlying row to the storage.
     * 
     * @throws IOException If a problem occurs during the write.
     */
    void writeRow() throws IOException;

    /**
     * Indicate that the underlying row has been written and should be reclaimed. This may be a
     * no-op for many storage types, however it enables additional layers of buffering where it may
     * be beneficial.
     */
    void release();
}
