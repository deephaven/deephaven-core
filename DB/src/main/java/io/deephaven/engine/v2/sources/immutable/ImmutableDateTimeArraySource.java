package io.deephaven.engine.v2.sources.immutable;

import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import io.deephaven.engine.v2.sources.AbstractColumnSource;
import io.deephaven.engine.v2.sources.ImmutableColumnSourceGetDefaults;

public class ImmutableDateTimeArraySource extends AbstractColumnSource<DBDateTime>
        implements ImmutableColumnSourceGetDefaults.ForObject<DBDateTime> {
    private final long[] data;

    public ImmutableDateTimeArraySource(long[] source) {
        super(DBDateTime.class);
        this.data = source;
    }

    @Override
    public DBDateTime get(long index) {
        if (index < 0 || index >= data.length) {
            return null;
        }

        return DBTimeUtils.nanosToTime(data[(int) index]);
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
