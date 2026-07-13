//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.util.datastructures.hash.NullableLongLongMap;

class RowRedirectionLockFreeFactory implements WritableRowRedirection.Factory {
    @Override
    public NullableLongLongMap createUnderlyingMapWithCapacity(int initialCapacity) {
        return WritableRowRedirectionLockFree.createMapWithCapacity(initialCapacity);
    }

    @Override
    public WritableRowRedirectionLockFree createRowRedirection(int initialCapacity) {
        return createRowRedirection(createUnderlyingMapWithCapacity(initialCapacity));
    }

    @Override
    public WritableRowRedirectionLockFree createRowRedirection(NullableLongLongMap map) {
        return new WritableRowRedirectionLockFree(map);
    }
}
