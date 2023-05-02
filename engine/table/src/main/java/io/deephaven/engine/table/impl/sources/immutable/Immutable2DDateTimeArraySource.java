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
 * Immutable2DArraySource for {@link DateTime}s. Allows reinterpretation as long.
 */
public class Immutable2DDateTimeArraySource extends Immutable2DNanosBasedTimeArraySource<DateTime>
        implements ImmutableColumnSourceGetDefaults.ForLongAsDateTime {

    public Immutable2DDateTimeArraySource() {
        super(DateTime.class);
    }

    public Immutable2DDateTimeArraySource(final @NotNull Immutable2DLongArraySource nanoSource) {
        super(DateTime.class, nanoSource);
    }

    @Override
    protected DateTime makeValue(long nanos) {
        return DateTimeUtils.epochNanosToDateTime(nanos);
    }

    @Override
    protected long toNanos(DateTime value) {
        return DateTimeUtils.epochNanos(value);
    }

    @Override
    public ColumnSource<DateTime> toDateTime() {
        return this;
    }
}

