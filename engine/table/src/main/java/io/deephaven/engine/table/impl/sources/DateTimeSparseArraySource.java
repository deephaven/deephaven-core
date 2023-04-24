/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTime;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Array-backed ColumnSource for DateTimes. Allows reinterpret as long.
 */
public class DateTimeSparseArraySource extends NanosBasedTimeSparseArraySource<DateTime>
        implements MutableColumnSourceGetDefaults.ForLongAsDateTime, DefaultChunkSource<Values>, ConvertableTimeSource {

    public DateTimeSparseArraySource() {
        super(DateTime.class);
    }

    public DateTimeSparseArraySource(final @NotNull LongSparseArraySource nanoSource) {
        super(DateTime.class, nanoSource);
    }

    @Override
    protected DateTime makeValue(long nanos) {
        return DateTimeUtils.nanosToDateTime(nanos);
    }

    @Override
    protected long toNanos(DateTime value) {
        return DateTimeUtils.nanos(value);
    }
}
