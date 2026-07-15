//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateTimsortKernels or ./gradlew generateTimsortKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.timsort.multi;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;

/**
 * Selects a pregenerated single-column indirect timsort kernel by chunk type and sort direction, returning
 * null for multi-column shapes, which MultiColumnTimsortKernelFactory compiles on demand.
 */
public final class IndirectTimsortDispatcher {
    private IndirectTimsortDispatcher() {
    }

    public static <PERMUTE_VALUES_ATTR extends Any> MultiColumnSortKernel<PERMUTE_VALUES_ATTR> makeContext(
            ChunkType[] chunkTypes, SortingOrder[] order, int size) {
        if (chunkTypes.length != 1) {
            return null;
        }
        switch (chunkTypes[0]) {
            case Char:
            if (order[0] == SortingOrder.Ascending) {
                return NullAwareCharIndirectTimsortKernel.createContext(size);
            }
            return NullAwareCharDescIndirectTimsortKernel.createContext(size);
            case Byte:
            if (order[0] == SortingOrder.Ascending) {
                return ByteIndirectTimsortKernel.createContext(size);
            }
            return ByteDescIndirectTimsortKernel.createContext(size);
            case Short:
            if (order[0] == SortingOrder.Ascending) {
                return ShortIndirectTimsortKernel.createContext(size);
            }
            return ShortDescIndirectTimsortKernel.createContext(size);
            case Int:
            if (order[0] == SortingOrder.Ascending) {
                return IntIndirectTimsortKernel.createContext(size);
            }
            return IntDescIndirectTimsortKernel.createContext(size);
            case Long:
            if (order[0] == SortingOrder.Ascending) {
                return LongIndirectTimsortKernel.createContext(size);
            }
            return LongDescIndirectTimsortKernel.createContext(size);
            case Float:
            if (order[0] == SortingOrder.Ascending) {
                return FloatIndirectTimsortKernel.createContext(size);
            }
            return FloatDescIndirectTimsortKernel.createContext(size);
            case Double:
            if (order[0] == SortingOrder.Ascending) {
                return DoubleIndirectTimsortKernel.createContext(size);
            }
            return DoubleDescIndirectTimsortKernel.createContext(size);
            case Object:
            if (order[0] == SortingOrder.Ascending) {
                return ObjectIndirectTimsortKernel.createContext(size);
            }
            return ObjectDescIndirectTimsortKernel.createContext(size);
            default: return null;
        }
    }
}
