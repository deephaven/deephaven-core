//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.data;

import elemental2.core.JsArray;
import io.deephaven.chunk.Chunk;
import jsinterop.base.Any;

import java.util.PrimitiveIterator;

/**
 * Holds data from or intended for web clients, normalizing over nulls, with helpers to handle typed chunks.
 */
public abstract class WebColumnData {
    protected final JsArray<Any> arr = new JsArray<>();

    public abstract void fillChunk(Chunk<?> data, PrimitiveIterator.OfLong destIterator);

    public void ensureCapacity(long size) {
        // Current impl does nothing, js arrays don't behave better when told the size up front
    }

    public Any get(long position) {
        return arr.getAt((int) position);
    }
}
