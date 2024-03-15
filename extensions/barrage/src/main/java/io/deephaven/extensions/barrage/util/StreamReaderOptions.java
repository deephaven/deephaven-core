//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.extensions.barrage.ColumnConversionMode;
import io.deephaven.util.QueryConstants;

public interface StreamReaderOptions {
    /**
     * @return whether we encode the validity buffer to express null values or {@link QueryConstants}'s NULL values.
     */
    boolean useDeephavenNulls();

    /**
     * @return the conversion mode to use for object columns
     */
    ColumnConversionMode columnConversionMode();

    /**
     * @return the ideal number of records to send per record batch
     */
    int batchSize();

    /**
     * @return the maximum number of bytes that should be sent in a single message.
     */
    int maxMessageSize();

    /**
     * Some Flight clients cannot handle modifications that have irregular column counts. These clients request that the
     * server wrap all columns in a list to enable each column having a variable length.
     *
     * @return true if the columns should be wrapped in a list
     */
    default boolean columnsAsList() {
        return false;
    }
}
