//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import gnu.trove.map.TLongLongMap;

public interface TNullableLongLongMap extends TLongLongMap {
    void resetToNull();

    int capacity();
}
