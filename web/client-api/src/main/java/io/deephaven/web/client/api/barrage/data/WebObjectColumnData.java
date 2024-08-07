//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.data;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import jsinterop.base.Js;

import java.util.PrimitiveIterator;

public class WebObjectColumnData extends WebColumnData {
    @Override
    public void fillChunk(Chunk<?> data, PrimitiveIterator.OfLong destIterator) {
        ObjectChunk<?, ?> objectChunk = data.asObjectChunk();
        int i = 0;
        while (destIterator.hasNext()) {
            Object value = objectChunk.get(i++);
            arr.setAt((int) destIterator.nextLong(), Js.asAny(value));
        }
    }
}
