//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

class TransferUtils {
    /**
     * Returns nanoseconds from the Epoch for a {@link LocalDateTime} value in UTC timezone.
     *
     * @param localDateTime the local date time to compute the Epoch offset for
     * @return nanoseconds since Epoch, or a NULL_LONG value if the local date time is null
     */
    static long epochNanosUTC(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return QueryConstants.NULL_LONG;
        }
        return DateTimeUtils.secondsToNanos(localDateTime.toEpochSecond(ZoneOffset.UTC))
                + localDateTime.toLocalTime().getNano();
    }
}
