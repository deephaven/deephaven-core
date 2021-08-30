package io.deephaven.engine.structures.rowredirection;

import io.deephaven.engine.structures.rowredirection.map.TNullableLongLongMap;
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
