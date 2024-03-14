//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharCopyKernel and run "./gradlew replicateCopyKernelTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.copy;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.WritableObjectChunk;
import org.junit.Test;

public class TestObjectCopyKernel {
    /**
     * There was an edge case (IDS-5504) leading to an array out of bounds exception caused by an inconsistency between
     * closed and half-open intervals.
     */
    @Test
    public void confirmEdgecaseFixed() {
        final WritableObjectChunk<Object, Values> output = WritableObjectChunk.makeWritableChunk(64);
        final Object[] baseInput = new Object[64];
        final Object[] overInput = new Object[64];
        final long[] useOverInput = new long[1];
        useOverInput[0] = 1L << 63; // This is the edge case
        ObjectCopyKernel.conditionalCopy(output, baseInput, overInput, useOverInput, 0, 0, 64);
    }
}
