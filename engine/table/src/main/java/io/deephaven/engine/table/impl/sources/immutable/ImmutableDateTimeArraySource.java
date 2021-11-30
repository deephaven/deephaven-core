package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;

public class ImmutableDateTimeArraySource extends AbstractColumnSource<DateTime>
        implements ImmutableColumnSourceGetDefaults.ForObject<DateTime> {
    private final long[] data;

    public ImmutableDateTimeArraySource(long[] source) {
        super(DateTime.class);
        this.data = source;
    }

    @Override
    public DateTime get(long index) {
        if (index < 0 || index >= data.length) {
            return null;
        }

        return DateTimeUtils.nanosToTime(data[(int) index]);
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
