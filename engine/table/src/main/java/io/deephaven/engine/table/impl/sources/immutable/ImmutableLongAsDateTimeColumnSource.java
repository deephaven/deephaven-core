/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.impl.sources.LongAsDateTimeColumnSource;

/**
 * Convenience wrapper for Python conversions from long[] to immutable DateTime source.
 */
public class ImmutableLongAsDateTimeColumnSource extends LongAsDateTimeColumnSource {
    public ImmutableLongAsDateTimeColumnSource(long [] dateTimes) {
        super(new ImmutableLongArraySource(dateTimes));
    }
}
