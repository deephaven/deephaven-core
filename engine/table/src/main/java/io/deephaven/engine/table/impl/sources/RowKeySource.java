/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;

/**
 * A column source that returns the row key as a long.
 */
public class RowKeySource extends AbstractColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong {
    public static final RowKeySource INSTANCE = new RowKeySource();

    public RowKeySource() {
        super(Long.class);
    }

    @Override
    public long getLong(long rowKey) {
        return rowKey;
    }
}
