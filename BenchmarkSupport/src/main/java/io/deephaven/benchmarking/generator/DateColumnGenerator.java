package io.deephaven.benchmarking.generator;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;

public class DateColumnGenerator implements ColumnGenerator<DateTime> {
    private NumGenerator gen;
    private final ColumnDefinition<DateTime> def;
    private final long min;
    private final long max;

    public DateColumnGenerator(String name) {
        this(name, 0, Long.MAX_VALUE);
    }

    public DateColumnGenerator(String name, DateTime min, DateTime max) {
        this(name, min.getNanos(), max.getNanos());
    }

    private DateColumnGenerator(String name, long min, long max) {
        def = ColumnDefinition.ofTime(name);
        this.min = min;
        this.max = max;
    }

    @Override
    public ColumnDefinition<DateTime> getDefinition() {
        return def;
    }

    @Override
    public String getUpdateString(String varName) {
        return def.getName() + "=(DateTime)" + varName + ".get()";
    }

    @Override
    public String getName() {
        return def.getName();
    }

    @Override
    public void init(ExtendedRandom random) {
        gen = new NumGenerator(min, max, random);
    }

    public DateTime get() {
        return DateTimeUtils.nanosToTime(gen.getLong());
    }
}
