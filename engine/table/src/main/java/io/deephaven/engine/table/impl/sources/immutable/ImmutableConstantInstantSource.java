//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * Constant ImmutableColumnSource for {@link Instant}s. Allows reinterpretation as long.
 */
public class ImmutableConstantInstantSource extends ImmutableConstantNanosBasedTimeSource<Instant>
        implements ImmutableColumnSourceGetDefaults.ForLongAsInstant {

    public ImmutableConstantInstantSource(final long nanos) {
        super(Instant.class, new ImmutableConstantLongSource(nanos));
    }

    public ImmutableConstantInstantSource(@NotNull final ImmutableConstantLongSource nanoSource) {
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
