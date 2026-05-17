//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import it.unimi.dsi.fastutil.longs.Long2LongMap;

public interface NullableLong2LongMap extends Long2LongMap {
    void resetToNull();

    int capacity();
}
