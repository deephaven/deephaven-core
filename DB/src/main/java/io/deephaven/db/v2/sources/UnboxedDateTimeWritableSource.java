package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.DBDateTime;

public class UnboxedDateTimeWritableSource extends UnboxedDateTimeColumnSource
    implements WritableSource<Long> {
    private final WritableSource<DBDateTime> alternateWritableSource;

    public UnboxedDateTimeWritableSource(WritableSource<DBDateTime> alternateWritableSource) {
        super(alternateWritableSource);
        this.alternateWritableSource = alternateWritableSource;
    }

    @Override
    public void copy(ColumnSource<Long> sourceColumn, long sourceKey, long destKey) {
        // We assume that the alternate source has a getLong method, so that we can avoid boxing.
        set(destKey, alternateWritableSource.getLong(sourceKey));
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        alternateWritableSource.ensureCapacity(capacity, nullFill);
    }

    @Override
    public void set(long key, Long value) {
        alternateWritableSource.set(key, value);
    }

    @Override
    public void set(long key, long value) {
        alternateWritableSource.set(key, value);
    }
}
