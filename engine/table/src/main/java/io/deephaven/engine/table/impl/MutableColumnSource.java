package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;

/**
 * Sub-interface of {@link ColumnSource} for implementations that always use return {@code false} from
 * {@link #isImmutable()}.
 */
public interface MutableColumnSource<DATA_TYPE> extends ColumnSource<DATA_TYPE> {

    @Override
    default boolean isImmutable() {
        return false;
    }
}
