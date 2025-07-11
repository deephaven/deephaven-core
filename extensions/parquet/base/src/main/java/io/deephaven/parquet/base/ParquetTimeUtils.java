//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Internal library with utility methods for converting time data between Deephaven and Parquet.
 */
@InternalUseOnly
public class ParquetTimeUtils {
    /**
     * Returns nanoseconds from the Epoch for a {@link LocalDateTime} value in UTC timezone.
     *
     * @param localDateTime the local date time to compute the Epoch offset for
     * @return nanoseconds since Epoch, or a NULL_LONG value if the local date time is null
     */
    public static long epochNanosUTC(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return QueryConstants.NULL_LONG;
        }
        return DateTimeUtils.secondsToNanos(localDateTime.toEpochSecond(ZoneOffset.UTC))
                + localDateTime.toLocalTime().getNano();
    }
}
