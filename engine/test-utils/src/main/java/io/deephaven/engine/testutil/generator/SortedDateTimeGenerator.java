package io.deephaven.engine.testutil.generator;

import io.deephaven.time.DateTime;

import java.util.Random;

public class SortedDateTimeGenerator extends AbstractSortedGenerator<DateTime> {
    private final DateTime minTime;
    private final DateTime maxTime;

    public SortedDateTimeGenerator(DateTime minTime, DateTime maxTime) {
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    DateTime maxValue() {
        return maxTime;
    }

    DateTime minValue() {
        return minTime;
    }

    DateTime makeValue(DateTime floor, DateTime ceiling, Random random) {
        final long longFloor = floor.getNanos();
        final long longCeiling = ceiling.getNanos();

        final long range = longCeiling - longFloor + 1L;
        final long nextLong = Math.abs(random.nextLong()) % range;

        return new DateTime(longFloor + (nextLong % range));
    }

    @Override
    public Class<DateTime> getType() {
        return DateTime.class;
    }
}
