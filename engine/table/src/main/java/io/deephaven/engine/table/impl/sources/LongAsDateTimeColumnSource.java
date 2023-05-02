/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code long} to {@link DateTime} values.
 */
public class LongAsDateTimeColumnSource extends BoxedLongAsTimeSource<DateTime> {
    public LongAsDateTimeColumnSource(ColumnSource<Long> alternateColumnSource) {
        super(DateTime.class, alternateColumnSource);
    }

    @Override
    protected DateTime makeValue(long val) {
        return DateTimeUtils.epochNanosToDateTime(val);
    }
}