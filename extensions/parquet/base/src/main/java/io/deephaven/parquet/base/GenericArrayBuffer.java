package io.deephaven.parquet.base;

import java.nio.IntBuffer;

public class GenericArrayBuffer<ELEMENT_BUFFER> {
    final ELEMENT_BUFFER buffer;
    final IntBuffer endOffsets;

    public GenericArrayBuffer(ELEMENT_BUFFER buffer, IntBuffer endOffsets) {
        this.buffer = buffer;
        this.endOffsets = endOffsets;
    }
}
