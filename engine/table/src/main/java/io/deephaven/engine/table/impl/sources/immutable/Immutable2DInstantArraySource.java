/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * Immutable2DArraySource for {@link Instant}s. Allows reinterpretation as long.
 */
public class Immutable2DInstantArraySource extends Immutable2DNanosBasedTimeArraySource<Instant>
        implements ImmutableColumnSourceGetDefaults.ForLongAsInstant {

    public Immutable2DInstantArraySource() {
        super(Instant.class);
    }

    public Immutable2DInstantArraySource(@NotNull final Immutable2DLongArraySource nanoSource) {
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
