/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Array-backed {@link ColumnSource} for DateTimes. Allows reinterpretation to long and {@link java.time.Instant}.
 */
public class DateTimeArraySource extends NanosBasedTimeArraySource<DateTime>
        implements MutableColumnSourceGetDefaults.ForLongAsDateTime, ConvertableTimeSource {
    public DateTimeArraySource() {
        super(DateTime.class);
    }

    public DateTimeArraySource(final @NotNull LongArraySource nanoSource) {
        super(DateTime.class, nanoSource);
    }

    @Override
    protected DateTime makeValue(long nanos) {
        return DateTimeUtils.nanosToTime(nanos);
    }

    @Override
    protected long toNanos(DateTime value) {
        return DateTimeUtils.nanos(value);
    }

    @Override
    public ColumnSource<DateTime> toDateTime() {
        return this;
    }
}
