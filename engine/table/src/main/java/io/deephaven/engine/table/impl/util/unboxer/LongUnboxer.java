/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharUnboxer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.util.unboxer;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;

class LongUnboxer implements ChunkUnboxer.UnboxerKernel {
    private final WritableLongChunk<Values> primitiveChunk;

    LongUnboxer(int capacity) {
        primitiveChunk = WritableLongChunk.makeWritableChunk(capacity);
    }

    @Override
    public void close() {
        primitiveChunk.close();
    }

    @Override
    public LongChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
        unboxTo(boxed, primitiveChunk, 0, 0);
        primitiveChunk.setSize(boxed.size());
        return primitiveChunk;
    }

    @Override
    public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
        unboxTo(boxed, primitives.asWritableLongChunk(), sourceOffset, destOffset);
    }

    public static void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableLongChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
        final ObjectChunk<Long, ? extends Values> longChunk = boxed.asObjectChunk();
        for (int ii = 0; ii < boxed.size(); ++ii) {
            primitives.set(ii + destOffset, TypeUtils.unbox(longChunk.get(ii + sourceOffset)));
        }
    }
}
