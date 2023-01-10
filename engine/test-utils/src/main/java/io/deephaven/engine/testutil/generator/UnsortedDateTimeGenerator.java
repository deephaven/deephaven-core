package io.deephaven.engine.testutil.generator;

import io.deephaven.time.DateTime;

import java.util.Random;

public class UnsortedDateTimeGenerator extends AbstractGenerator<DateTime> {
    private final DateTime minTime;
    private final DateTime maxTime;
    private final double nullFrac;

    public UnsortedDateTimeGenerator(DateTime minTime, DateTime maxTime) {
        this(minTime, maxTime, 0);
    }

    public UnsortedDateTimeGenerator(DateTime minTime, DateTime maxTime, double nullFrac) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.nullFrac = nullFrac;
    }

    @Override
    public Class<DateTime> getType() {
        return DateTime.class;
    }

    @Override
    public DateTime nextValue(Random random) {
        if (nullFrac > 0 && random.nextDouble() < nullFrac) {
            return null;
        }
        final long longFloor = minTime.getNanos();
        final long longCeiling = maxTime.getNanos();

        final long range = longCeiling - longFloor + 1L;
        final long nextLong = Math.abs(random.nextLong()) % range;

        return new DateTime(longFloor + (nextLong % range));
    }
}
