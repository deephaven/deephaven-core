/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharUnboxer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.util.unboxer;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;

class ByteUnboxer implements ChunkUnboxer.UnboxerKernel {
    private final WritableByteChunk<Values> primitiveChunk;

    ByteUnboxer(int capacity) {
        primitiveChunk = WritableByteChunk.makeWritableChunk(capacity);
    }

    @Override
    public void close() {
        primitiveChunk.close();
    }

    @Override
    public ByteChunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxed) {
        unboxTo(boxed, primitiveChunk, 0, 0);
        primitiveChunk.setSize(boxed.size());
        return primitiveChunk;
    }

    @Override
    public void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
        unboxTo(boxed, primitives.asWritableByteChunk(), sourceOffset, destOffset);
    }

    public static void unboxTo(ObjectChunk<?, ? extends Values> boxed, WritableByteChunk<? extends Values> primitives, int sourceOffset, int destOffset) {
        final ObjectChunk<Byte, ? extends Values> byteChunk = boxed.asObjectChunk();
        for (int ii = 0; ii < boxed.size(); ++ii) {
            primitives.set(ii + destOffset, TypeUtils.unbox(byteChunk.get(ii + sourceOffset)));
        }
    }
}
