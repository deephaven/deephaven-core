/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.structures.rowredirection.map;

import gnu.trove.map.TLongLongMap;

public interface TNullableLongLongMap extends TLongLongMap {
    void resetToNull();
    int capacity();
}