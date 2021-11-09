package io.deephaven.engine.v2.sources.immutable;

import io.deephaven.engine.tables.utils.DateTime;
import io.deephaven.engine.tables.utils.DateTimeUtils;
import io.deephaven.engine.v2.sources.AbstractColumnSource;
import io.deephaven.engine.v2.sources.ImmutableColumnSourceGetDefaults;

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
