package io.deephaven.parquet.base;

import java.nio.IntBuffer;

public class DataWithOffsets {
    public static final int NULL_OFFSET = -1;

    public final IntBuffer offsets;
    public final Object materializeResult;

    public DataWithOffsets(IntBuffer offsets, Object materializeResult) {
        this.offsets = offsets;
        this.materializeResult = materializeResult;
    }
}
