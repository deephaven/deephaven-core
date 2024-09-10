//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit WebCharColumnData and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.web.client.api.barrage.data;

import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.util.QueryConstants;
import jsinterop.base.Js;

import java.util.PrimitiveIterator;

public class WebShortColumnData extends WebColumnData {
    @Override
    public void fillChunk(Chunk<?> data, PrimitiveIterator.OfLong destIterator) {
        ShortChunk<?> shortChunk = data.asShortChunk();
        int i = 0;
        while (destIterator.hasNext()) {
            short value = shortChunk.get(i++);
            arr.setAt((int) destIterator.nextLong(), value == QueryConstants.NULL_SHORT ? null : Js.asAny(value));
        }
    }
}
