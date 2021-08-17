/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharUnboxer and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.utils.unboxer;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.util.type.TypeUtils;

class ByteUnboxer implements ChunkUnboxer.UnboxerKernel {
    private final WritableByteChunk<Attributes.Values> primitiveChunk;

    ByteUnboxer(int capacity) {
        primitiveChunk = WritableByteChunk.makeWritableChunk(capacity);
    }

    @Override
    public void close() {
        primitiveChunk.close();
    }

    @Override
    public ByteChunk<? extends Attributes.Values> unbox(ObjectChunk<?, ? extends Attributes.Values> boxed) {
        unboxTo(boxed, primitiveChunk, 0, 0);
        primitiveChunk.setSize(boxed.size());
        return primitiveChunk;
    }

    @Override
    public void unboxTo(ObjectChunk<?, ? extends Attributes.Values> boxed, WritableChunk<? extends Attributes.Values> primitives, int sourceOffset, int destOffset) {
        unboxTo(boxed, primitives.asWritableByteChunk(), sourceOffset, destOffset);
    }

    public static void unboxTo(ObjectChunk<?, ? extends Attributes.Values> boxed, WritableByteChunk<? extends Attributes.Values> primitives, int sourceOffset, int destOffset) {
        final ObjectChunk<Byte, ? extends Attributes.Values> byteChunk = boxed.asObjectChunk();
        for (int ii = 0; ii < boxed.size(); ++ii) {
            primitives.set(ii + destOffset, TypeUtils.unbox(byteChunk.get(ii + sourceOffset)));
        }
    }
}
