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
 * ImmutableArraySource for {@link DateTime}s. Allows reinterpretation as long.
 */
public class ImmutableDateTimeArraySource extends ImmutableNanosBasedTimeArraySource<DateTime>
        implements ImmutableColumnSourceGetDefaults.ForLongAsDateTime {

    public ImmutableDateTimeArraySource() {
        super(DateTime.class);
    }

    public ImmutableDateTimeArraySource(final @NotNull long[] nanos) {
        super(DateTime.class, new ImmutableLongArraySource(nanos));
    }

    public ImmutableDateTimeArraySource(final @NotNull ImmutableLongArraySource nanoSource) {
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

    @Override
    public ColumnSource<DateTime> toDateTime() {
        return this;
    }
}
