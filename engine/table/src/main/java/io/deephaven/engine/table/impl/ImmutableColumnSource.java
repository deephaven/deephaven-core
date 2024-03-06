//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;

/**
 * Sub-interface of {@link ColumnSource} for implementations that always use return {@code true} from
 * {@link #isImmutable()} and delegate all {@code getPrev*} methods to their current (non-previous) equivalents.
 */
public interface ImmutableColumnSource<DATA_TYPE> extends ColumnSource<DATA_TYPE> {

    @Override
    default DATA_TYPE getPrev(final long rowKey) {
        return get(rowKey);
    }

    @Override
    default Boolean getPrevBoolean(final long rowKey) {
        return getBoolean(rowKey);
    }

    @Override
    default byte getPrevByte(final long rowKey) {
        return getByte(rowKey);
    }

    @Override
    default char getPrevChar(final long rowKey) {
        return getChar(rowKey);
    }

    @Override
    default double getPrevDouble(final long rowKey) {
        return getDouble(rowKey);
    }

    @Override
    default float getPrevFloat(final long rowKey) {
        return getFloat(rowKey);
    }

    @Override
    default int getPrevInt(final long rowKey) {
        return getInt(rowKey);
    }

    @Override
    default long getPrevLong(final long rowKey) {
        return getLong(rowKey);
    }

    @Override
    default short getPrevShort(final long rowKey) {
        return getShort(rowKey);
    }

    @Override
    default boolean isImmutable() {
        return true;
    }
}
