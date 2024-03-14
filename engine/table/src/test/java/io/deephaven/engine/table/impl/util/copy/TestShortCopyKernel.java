//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharCopyKernel and run "./gradlew replicateCopyKernelTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.copy;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.WritableShortChunk;
import org.junit.Test;

public class TestShortCopyKernel {
    /**
     * There was an edge case (IDS-5504) leading to an array out of bounds exception caused by an inconsistency between
     * closed and half-open intervals.
     */
    @Test
    public void confirmEdgecaseFixed() {
        final WritableShortChunk<Values> output = WritableShortChunk.makeWritableChunk(64);
        final short[] baseInput = new short[64];
        final short[] overInput = new short[64];
        final long[] useOverInput = new long[1];
        useOverInput[0] = 1L << 63; // This is the edge case
        ShortCopyKernel.conditionalCopy(output, baseInput, overInput, useOverInput, 0, 0, 64);
    }
}
