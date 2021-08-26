package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.hashing.TNullableLongLongMap;
import gnu.trove.map.TLongLongMap;

class RedirectionIndexLockFreeFactory implements RedirectionIndex.Factory {
    @Override
    public TLongLongMap createUnderlyingMapWithCapacity(int initialCapacity) {
        return RedirectionIndexLockFreeImpl.createMapWithCapacity(initialCapacity);
    }

    @Override
    public RedirectionIndexLockFreeImpl createRedirectionIndex(int initialCapacity) {
        return createRedirectionIndex(createUnderlyingMapWithCapacity(initialCapacity));
    }

    @Override
    public RedirectionIndexLockFreeImpl createRedirectionIndex(TLongLongMap map) {
        return new RedirectionIndexLockFreeImpl((TNullableLongLongMap) map);
    }
}
