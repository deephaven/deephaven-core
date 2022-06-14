/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;

public class RowIdSource extends AbstractColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong {
    public static final RowIdSource INSTANCE = new RowIdSource();

    public RowIdSource() {
        super(Long.class);
    }

    @Override
    public long getLong(long rowKey) {
        return rowKey;
    }
}
