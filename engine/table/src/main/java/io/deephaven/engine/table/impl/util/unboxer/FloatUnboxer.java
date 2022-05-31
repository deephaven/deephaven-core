/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharUnboxer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.util.unboxer;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;

class FloatUnboxer implements ChunkUnboxer.UnboxerKernel {
    private final WritableFloatChunk<Values> primitiveChunk;

    FloatUnboxer(int capacity) {
        primitiveChunk = WritableFloatChunk.makeWritableChunk(capacity);
    }

    @Override
    public void close() {
        primitiveChunk.close();
    }

    @Override
    public FloatChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
        unboxTo(boxed, primitiveChunk, 0, 0);
        primitiveChunk.setSize(boxed.size());
        return primitiveChunk;
    }

    @Override
    public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
        unboxTo(boxed, primitives.asWritableFloatChunk(), sourceOffset, destOffset);
    }

    public static void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableFloatChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
        final ObjectChunk<Float, ? extends Values> floatChunk = boxed.asObjectChunk();
        for (int ii = 0; ii < boxed.size(); ++ii) {
            primitives.set(ii + destOffset, TypeUtils.unbox(floatChunk.get(ii + sourceOffset)));
        }
    }
}
