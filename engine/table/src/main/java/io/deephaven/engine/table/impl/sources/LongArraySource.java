/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.type.TypeUtils.box;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Simple array source for Long.
 */
public class LongArraySource extends AbstractLongArraySource<Long> {

    public LongArraySource() {
        super(long.class);
    }

    @Override
    public void setNull(long key) {
        set(key, NULL_LONG);
    }

    @Override
    public void set(long key, Long value) {
        set(key, unbox(value));
    }

    @Override
    public Long get(long rowKey) { return box(getLong(rowKey)); }

    @Override
    public Long getPrev(long rowKey) {
        return box(getPrevLong(rowKey));
    }
}
