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
     * Deprecated since 0.37.0 and is marked for removal. (our GWT artifacts do not yet support the attributes)
     *
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
     * <li>If non-zero, the server will limit the length of any encoded list / array to n elements, where n is the
     * absolute value of the specified value.</li>
     * <li>If the column value has length less than zero, the server will encode the last n elements of the list /
     * array.</li>
     * </ul>
     * <p>
     * Note that the server will append an arbitrary value to indicate truncation; this value may not be the actual last
     * value in the list / array.
     *
     * @return the maximum length of any list / array to encode; zero means no limit; negative values indicate to treat
     *         the limit as a tail instead of a head
     */
    default int previewListLengthLimit() {
        return 0;
    }
}
