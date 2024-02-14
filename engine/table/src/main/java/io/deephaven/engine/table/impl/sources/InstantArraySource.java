/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * Array-backed ColumnSource for {@link Instant}s. Allows reinterpretation as long.
 */
public class InstantArraySource extends NanosBasedTimeArraySource<Instant>
        implements MutableColumnSourceGetDefaults.ForLongAsInstant {
    public InstantArraySource() {
        super(Instant.class);
    }

    public InstantArraySource(@NotNull final LongArraySource nanoSource) {
        super(Instant.class, nanoSource);
    }

    @Override
    protected Instant makeValue(long nanos) {
        return DateTimeUtils.epochNanosToInstant(nanos);
    }

    @Override
    protected long toNanos(Instant value) {
        return DateTimeUtils.epochNanos(value);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return this;
    }
}
