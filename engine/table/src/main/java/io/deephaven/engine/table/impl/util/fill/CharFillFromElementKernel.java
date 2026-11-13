//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.fill;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;

public class CharFillFromElementKernel implements FillFromElementKernel {
    public static final CharFillFromElementKernel INSTANCE = new CharFillFromElementKernel();

    private CharFillFromElementKernel() {} // use through the instance

    @Override
    public void fillFromElement(WritableChunk<? extends Any> dest, int destOffset, int length,
            Chunk<? extends Any> src, int srcOffset) {
        dest.asWritableCharChunk().fillWithValue(destOffset, length, src.asCharChunk().get(srcOffset));
    }
}
