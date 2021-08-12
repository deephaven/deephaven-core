/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharUnboxer and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.utils.unboxer;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.util.type.TypeUtils;

class IntUnboxer implements ChunkUnboxer.UnboxerKernel {
    private final WritableIntChunk<Attributes.Values> primitiveChunk;

    IntUnboxer(int capacity) {
        primitiveChunk = WritableIntChunk.makeWritableChunk(capacity);
    }

    @Override
    public void close() {
        primitiveChunk.close();
    }

    @Override
    public IntChunk<? extends Attributes.Values> unbox(ObjectChunk<?, ? extends Attributes.Values> boxed) {
        unboxTo(boxed, primitiveChunk, 0, 0);
        primitiveChunk.setSize(boxed.size());
        return primitiveChunk;
    }

    @Override
    public void unboxTo(ObjectChunk<?, ? extends Attributes.Values> boxed, WritableChunk<? extends Attributes.Values> primitives, int sourceOffset, int destOffset) {
        unboxTo(boxed, primitives.asWritableIntChunk(), sourceOffset, destOffset);
    }

    public static void unboxTo(ObjectChunk<?, ? extends Attributes.Values> boxed, WritableIntChunk<? extends Attributes.Values> primitives, int sourceOffset, int destOffset) {
        final ObjectChunk<Integer, ? extends Attributes.Values> intChunk = boxed.asObjectChunk();
        for (int ii = 0; ii < boxed.size(); ++ii) {
            primitives.set(ii + destOffset, TypeUtils.unbox(intChunk.get(ii + sourceOffset)));
        }
    }
}
