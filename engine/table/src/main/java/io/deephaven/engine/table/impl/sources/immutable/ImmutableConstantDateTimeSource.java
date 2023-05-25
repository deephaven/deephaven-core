/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Constant ImmutableColumnSource for {@link DateTime}s. Allows reinterpretation as long.
 */
public class ImmutableConstantDateTimeSource extends ImmutableConstantNanosBasedTimeSource<DateTime>
        implements ImmutableColumnSourceGetDefaults.ForLongAsDateTime {

    public ImmutableConstantDateTimeSource(final long nanos) {
        super(DateTime.class, new ImmutableConstantLongSource(nanos));
    }

    public ImmutableConstantDateTimeSource(final @NotNull ImmutableConstantLongSource nanoSource) {
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
