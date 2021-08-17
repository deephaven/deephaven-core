/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharUnboxer and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.utils.unboxer;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.util.type.TypeUtils;

class DoubleUnboxer implements ChunkUnboxer.UnboxerKernel {
    private final WritableDoubleChunk<Attributes.Values> primitiveChunk;

    DoubleUnboxer(int capacity) {
        primitiveChunk = WritableDoubleChunk.makeWritableChunk(capacity);
    }

    @Override
    public void close() {
        primitiveChunk.close();
    }

    @Override
    public DoubleChunk<? extends Attributes.Values> unbox(ObjectChunk<?, ? extends Attributes.Values> boxed) {
        unboxTo(boxed, primitiveChunk, 0, 0);
        primitiveChunk.setSize(boxed.size());
        return primitiveChunk;
    }

    @Override
    public void unboxTo(ObjectChunk<?, ? extends Attributes.Values> boxed, WritableChunk<? extends Attributes.Values> primitives, int sourceOffset, int destOffset) {
        unboxTo(boxed, primitives.asWritableDoubleChunk(), sourceOffset, destOffset);
    }

    public static void unboxTo(ObjectChunk<?, ? extends Attributes.Values> boxed, WritableDoubleChunk<? extends Attributes.Values> primitives, int sourceOffset, int destOffset) {
        final ObjectChunk<Double, ? extends Attributes.Values> doubleChunk = boxed.asObjectChunk();
        for (int ii = 0; ii < boxed.size(); ++ii) {
            primitives.set(ii + destOffset, TypeUtils.unbox(doubleChunk.get(ii + sourceOffset)));
        }
    }
}
