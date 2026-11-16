//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharFillFromElementKernel and run "./gradlew replicateFillFromElementKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.fill;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;

public class FloatFillFromElementKernel implements FillFromElementKernel {
    public static final FloatFillFromElementKernel INSTANCE = new FloatFillFromElementKernel();

    private FloatFillFromElementKernel() {} // use through the instance

    @Override
    public void fillFromElement(WritableChunk<? extends Any> dest, int destOffset, int length,
            Chunk<? extends Any> src, int srcOffset) {
        dest.asWritableFloatChunk().fillWithValue(destOffset, length, src.asFloatChunk().get(srcOffset));
    }
}
