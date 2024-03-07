//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.time.DateTimeUtils;

import java.time.Instant;
import java.util.Random;

public class UnsortedInstantGenerator extends AbstractGenerator<Instant> {

    private final Instant minTime;
    private final Instant maxTime;
    private final double nullFrac;

    public UnsortedInstantGenerator(Instant minTime, Instant maxTime) {
        this(minTime, maxTime, 0);
    }

    public UnsortedInstantGenerator(Instant minTime, Instant maxTime, double nullFrac) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.nullFrac = nullFrac;
    }

    @Override
    public Class<Instant> getType() {
        return Instant.class;
    }

    @Override
    public Instant nextValue(Random random) {
        if (nullFrac > 0 && random.nextDouble() < nullFrac) {
            return null;
        }
        final long longFloor = DateTimeUtils.epochNanos(minTime);
        final long longCeiling = DateTimeUtils.epochNanos(maxTime);

        final long range = longCeiling - longFloor + 1L;
        final long nextLong = Math.abs(random.nextLong()) % range;

        return DateTimeUtils.epochNanosToInstant(longFloor + nextLong);
    }
}
