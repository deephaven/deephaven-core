package io.deephaven.db.v2.sources;

/**
 * Sub-interface of {@link ColumnSource} for implementations that always use return {@code true}
 * from {@link #isImmutable()} and delegate all {@code getPrev*} methods to their current
 * (non-previous) equivalents.
 */
public interface ImmutableColumnSource<DATA_TYPE> extends ColumnSource<DATA_TYPE> {

    @Override
    default DATA_TYPE getPrev(final long elementIndex) {
        return get(elementIndex);
    }

    @Override
    default Boolean getPrevBoolean(final long elementIndex) {
        return getBoolean(elementIndex);
    }

    @Override
    default byte getPrevByte(final long elementIndex) {
        return getByte(elementIndex);
    }

    @Override
    default char getPrevChar(final long elementIndex) {
        return getChar(elementIndex);
    }

    @Override
    default double getPrevDouble(final long elementIndex) {
        return getDouble(elementIndex);
    }

    @Override
    default float getPrevFloat(final long elementIndex) {
        return getFloat(elementIndex);
    }

    @Override
    default int getPrevInt(final long elementIndex) {
        return getInt(elementIndex);
    }

    @Override
    default long getPrevLong(final long elementIndex) {
        return getLong(elementIndex);
    }

    @Override
    default short getPrevShort(final long elementIndex) {
        return getShort(elementIndex);
    }

    @Override
    default boolean isImmutable() {
        return true;
    }
}
