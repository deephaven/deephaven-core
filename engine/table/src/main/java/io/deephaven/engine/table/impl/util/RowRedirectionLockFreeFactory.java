//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.util.datastructures.hash.NullableLong2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;

class RowRedirectionLockFreeFactory implements WritableRowRedirection.Factory {
    @Override
    public NullableLong2LongMap createUnderlyingMapWithCapacity(int initialCapacity) {
        return WritableRowRedirectionLockFree.createMapWithCapacity(initialCapacity);
    }

    @Override
    public WritableRowRedirectionLockFree createRowRedirection(int initialCapacity) {
        return createRowRedirection(createUnderlyingMapWithCapacity(initialCapacity));
    }

    @Override
    public WritableRowRedirectionLockFree createRowRedirection(NullableLong2LongMap map) {
        return new WritableRowRedirectionLockFree(map);
    }
}
