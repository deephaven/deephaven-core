package io.deephaven.db.v2.utils.unboxer;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.util.type.TypeUtils;

class CharUnboxer implements ChunkUnboxer.UnboxerKernel {
    private final WritableCharChunk<Attributes.Values> primitiveChunk;

    CharUnboxer(int capacity) {
        primitiveChunk = WritableCharChunk.makeWritableChunk(capacity);
    }

    @Override
    public void close() {
        primitiveChunk.close();
    }

    @Override
    public CharChunk<? extends Attributes.Values> unbox(ObjectChunk<?, ? extends Attributes.Values> boxed) {
        unboxTo(boxed, primitiveChunk, 0, 0);
        primitiveChunk.setSize(boxed.size());
        return primitiveChunk;
    }

    @Override
    public void unboxTo(ObjectChunk<?, ? extends Attributes.Values> boxed, WritableChunk<? extends Attributes.Values> primitives, int sourceOffset, int destOffset) {
        unboxTo(boxed, primitives.asWritableCharChunk(), sourceOffset, destOffset);
    }

    public static void unboxTo(ObjectChunk<?, ? extends Attributes.Values> boxed, WritableCharChunk<? extends Attributes.Values> primitives, int sourceOffset, int destOffset) {
        final ObjectChunk<Character, ? extends Attributes.Values> charChunk = boxed.asObjectChunk();
        for (int ii = 0; ii < boxed.size(); ++ii) {
            primitives.set(ii + destOffset, TypeUtils.unbox(charChunk.get(ii + sourceOffset)));
        }
    }
}
