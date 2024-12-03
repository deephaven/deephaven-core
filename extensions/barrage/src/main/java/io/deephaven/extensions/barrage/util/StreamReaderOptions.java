//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.extensions.barrage.ColumnConversionMode;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;

public interface StreamReaderOptions {
    /**
     * @return whether we encode the validity buffer to express null values or {@link QueryConstants}'s NULL values.
     */
    boolean useDeephavenNulls();

    /**
     * @deprecated Since 0.37.0 and is marked for removal. (Note, GWT does not support encoding this context via
     *             annotation values.)
     * @return the conversion mode to use for object columns
     */
    @FinalDefault
    @Deprecated
    default ColumnConversionMode columnConversionMode() {
        return ColumnConversionMode.Stringify;
    }

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

    /**
     * The maximum length of any list / array to encode.
     * <ul>
     * <li>If zero, list lengths will not be limited.</li>
     * <li>If non-zero, the server will limit the length of any encoded list / array to the absolute value of the
     * returned length.</li>
     * <li>If less than zero, the server will encode elements from the end of the list / array, rather than rom the
     * beginning.</li>
     * </ul>
     * <p>
     * Note: The server is unable to indicate when truncation occurs. To detect truncation request one more element than
     * the maximum number you wish to display.
     *
     * @return the maximum length of any list / array to encode; zero means no limit; negative values indicate to treat
     *         the limit as a tail instead of a head
     */
    default long previewListLengthLimit() {
        return 0;
    }
}
