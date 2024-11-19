//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.data;

import elemental2.core.JsArray;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.web.shared.data.RangeSet;
import jsinterop.base.Any;

import java.util.List;
import java.util.PrimitiveIterator;

/**
 * Holds data from or intended for web clients, normalizing over nulls, with helpers to handle typed chunks.
 */
public abstract class WebColumnData {
    protected int length = 0;
    protected JsArray<Any> arr = new JsArray<>();

    public abstract void fillChunk(Chunk<?> data, PrimitiveIterator.OfLong destIterator);

    /**
     * Apply a viewport update directly to this column data.
     *
     * @param data the data source for added rows
     * @param added rows that are new in a post-update position-space
     * @param removed rows that no longer exist in a pre-update position-space
     */
    public abstract void applyUpdate(List<Chunk<Values>> data, RangeSet added, RangeSet removed);

    public void ensureCapacity(long size) {
        // Current impl does nothing, js arrays don't behave better when told the size up front
    }

    public Any get(long position) {
        return arr.getAt((int) position);
    }
}
