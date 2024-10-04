//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.data;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.util.QueryConstants;
import jsinterop.base.Js;

import java.util.PrimitiveIterator;

public class WebCharColumnData extends WebColumnData {
    @Override
    public void fillChunk(Chunk<?> data, PrimitiveIterator.OfLong destIterator) {
        CharChunk<?> charChunk = data.asCharChunk();
        int i = 0;
        while (destIterator.hasNext()) {
            char value = charChunk.get(i++);
            arr.setAt((int) destIterator.nextLong(), value == QueryConstants.NULL_CHAR ? null : Js.asAny(value));
        }
    }
}
