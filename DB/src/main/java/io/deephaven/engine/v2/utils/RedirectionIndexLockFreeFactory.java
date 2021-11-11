package io.deephaven.engine.v2.utils;

import io.deephaven.util.datastructures.hash.TNullableLongLongMap;
import gnu.trove.map.TLongLongMap;

class RowRedirectionLockFreeFactory implements MutableRowRedirection.Factory {
    @Override
    public TLongLongMap createUnderlyingMapWithCapacity(int initialCapacity) {
        return MutableRowRedirectionLockFree.createMapWithCapacity(initialCapacity);
    }

    @Override
    public MutableRowRedirectionLockFree createRowRedirection(int initialCapacity) {
        return createRowRedirection(createUnderlyingMapWithCapacity(initialCapacity));
    }

    @Override
    public MutableRowRedirectionLockFree createRowRedirection(TLongLongMap map) {
        return new MutableRowRedirectionLockFree((TNullableLongLongMap) map);
    }
}
