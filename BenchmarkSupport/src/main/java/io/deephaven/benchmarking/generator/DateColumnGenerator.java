package io.deephaven.benchmarking.generator;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;

public class DateColumnGenerator implements ColumnGenerator<DBDateTime> {
    private NumGenerator gen;
    private final ColumnDefinition<DBDateTime> def;
    private final long min;
    private final long max;

    public DateColumnGenerator(String name) {
        this(name, 0, Long.MAX_VALUE);
    }

    public DateColumnGenerator(String name, DBDateTime min, DBDateTime max) {
        this(name, min.getNanos(), max.getNanos());
    }

    private DateColumnGenerator(String name, long min, long max) {
        def = ColumnDefinition.ofTime(name);
        this.min = min;
        this.max = max;
    }

    @Override
    public ColumnDefinition<DBDateTime> getDefinition() {
        return def;
    }

    @Override
    public String getUpdateString(String varName) {
        return def.getName() + "=(DBDateTime)" + varName + ".get()";
    }

    @Override
    public String getName() {
        return def.getName();
    }

    @Override
    public void init(ExtendedRandom random) {
        gen = new NumGenerator(min, max, random);
    }

    public DBDateTime get() {
        return DBTimeUtils.nanosToTime(gen.getLong());
    }
}
