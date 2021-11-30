package io.deephaven.engine.table.impl.util;

import io.deephaven.util.datastructures.hash.TNullableLongLongMap;
import gnu.trove.map.TLongLongMap;

class RowRedirectionLockFreeFactory implements WritableRowRedirection.Factory {
    @Override
    public TLongLongMap createUnderlyingMapWithCapacity(int initialCapacity) {
        return WritableRowRedirectionLockFree.createMapWithCapacity(initialCapacity);
    }

    @Override
    public WritableRowRedirectionLockFree createRowRedirection(int initialCapacity) {
        return createRowRedirection(createUnderlyingMapWithCapacity(initialCapacity));
    }

    @Override
    public WritableRowRedirectionLockFree createRowRedirection(TLongLongMap map) {
        return new WritableRowRedirectionLockFree((TNullableLongLongMap) map);
    }
}
