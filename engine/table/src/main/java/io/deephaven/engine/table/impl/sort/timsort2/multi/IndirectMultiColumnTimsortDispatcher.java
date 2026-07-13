//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateTimsortKernels or ./gradlew generateTimsortKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.timsort2.multi;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;

/**
 * Selects a pregenerated indirect timsort kernel for the given column chunk types and sort directions,
 * returning null when no pregenerated kernel exists so that MultiColumnTimsortKernelFactory can compile
 * one on demand (or fall back to the single-column kernels).
 */
public final class IndirectMultiColumnTimsortDispatcher {
    private IndirectMultiColumnTimsortDispatcher() {
    }

    public static <PERMUTE_VALUES_ATTR extends Any> MultiColumnSortKernel<PERMUTE_VALUES_ATTR> makeContext(
            ChunkType[] chunkTypes, SortingOrder[] order, int size) {
        if (chunkTypes.length == 1) {
            if (chunkTypes[0] != ChunkType.Object) {
                return null;
            }
            if (order[0] == SortingOrder.Ascending) {
                return ObjectIndirectTimsortKernel.createContext(size);
            }
            return ObjectDescIndirectTimsortKernel.createContext(size);
        }
        if (chunkTypes.length != 2) {
            return null;
        }
        for (final SortingOrder columnOrder : order) {
            if (columnOrder != SortingOrder.Ascending) {
                return null;
            }
        }
        switch (chunkTypes[0]) {
            case Char:
            switch (chunkTypes[1]) {
                case Char: return NullAwareCharNullAwareCharIndirectMultiColumnTimsortKernel.createContext(size);
                case Byte: return NullAwareCharByteIndirectMultiColumnTimsortKernel.createContext(size);
                case Short: return NullAwareCharShortIndirectMultiColumnTimsortKernel.createContext(size);
                case Int: return NullAwareCharIntIndirectMultiColumnTimsortKernel.createContext(size);
                case Long: return NullAwareCharLongIndirectMultiColumnTimsortKernel.createContext(size);
                case Float: return NullAwareCharFloatIndirectMultiColumnTimsortKernel.createContext(size);
                case Double: return NullAwareCharDoubleIndirectMultiColumnTimsortKernel.createContext(size);
                case Object: return NullAwareCharObjectIndirectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Byte:
            switch (chunkTypes[1]) {
                case Char: return ByteNullAwareCharIndirectMultiColumnTimsortKernel.createContext(size);
                case Byte: return ByteByteIndirectMultiColumnTimsortKernel.createContext(size);
                case Short: return ByteShortIndirectMultiColumnTimsortKernel.createContext(size);
                case Int: return ByteIntIndirectMultiColumnTimsortKernel.createContext(size);
                case Long: return ByteLongIndirectMultiColumnTimsortKernel.createContext(size);
                case Float: return ByteFloatIndirectMultiColumnTimsortKernel.createContext(size);
                case Double: return ByteDoubleIndirectMultiColumnTimsortKernel.createContext(size);
                case Object: return ByteObjectIndirectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Short:
            switch (chunkTypes[1]) {
                case Char: return ShortNullAwareCharIndirectMultiColumnTimsortKernel.createContext(size);
                case Byte: return ShortByteIndirectMultiColumnTimsortKernel.createContext(size);
                case Short: return ShortShortIndirectMultiColumnTimsortKernel.createContext(size);
                case Int: return ShortIntIndirectMultiColumnTimsortKernel.createContext(size);
                case Long: return ShortLongIndirectMultiColumnTimsortKernel.createContext(size);
                case Float: return ShortFloatIndirectMultiColumnTimsortKernel.createContext(size);
                case Double: return ShortDoubleIndirectMultiColumnTimsortKernel.createContext(size);
                case Object: return ShortObjectIndirectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Int:
            switch (chunkTypes[1]) {
                case Char: return IntNullAwareCharIndirectMultiColumnTimsortKernel.createContext(size);
                case Byte: return IntByteIndirectMultiColumnTimsortKernel.createContext(size);
                case Short: return IntShortIndirectMultiColumnTimsortKernel.createContext(size);
                case Int: return IntIntIndirectMultiColumnTimsortKernel.createContext(size);
                case Long: return IntLongIndirectMultiColumnTimsortKernel.createContext(size);
                case Float: return IntFloatIndirectMultiColumnTimsortKernel.createContext(size);
                case Double: return IntDoubleIndirectMultiColumnTimsortKernel.createContext(size);
                case Object: return IntObjectIndirectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Long:
            switch (chunkTypes[1]) {
                case Char: return LongNullAwareCharIndirectMultiColumnTimsortKernel.createContext(size);
                case Byte: return LongByteIndirectMultiColumnTimsortKernel.createContext(size);
                case Short: return LongShortIndirectMultiColumnTimsortKernel.createContext(size);
                case Int: return LongIntIndirectMultiColumnTimsortKernel.createContext(size);
                case Long: return LongLongIndirectMultiColumnTimsortKernel.createContext(size);
                case Float: return LongFloatIndirectMultiColumnTimsortKernel.createContext(size);
                case Double: return LongDoubleIndirectMultiColumnTimsortKernel.createContext(size);
                case Object: return LongObjectIndirectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Float:
            switch (chunkTypes[1]) {
                case Char: return FloatNullAwareCharIndirectMultiColumnTimsortKernel.createContext(size);
                case Byte: return FloatByteIndirectMultiColumnTimsortKernel.createContext(size);
                case Short: return FloatShortIndirectMultiColumnTimsortKernel.createContext(size);
                case Int: return FloatIntIndirectMultiColumnTimsortKernel.createContext(size);
                case Long: return FloatLongIndirectMultiColumnTimsortKernel.createContext(size);
                case Float: return FloatFloatIndirectMultiColumnTimsortKernel.createContext(size);
                case Double: return FloatDoubleIndirectMultiColumnTimsortKernel.createContext(size);
                case Object: return FloatObjectIndirectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Double:
            switch (chunkTypes[1]) {
                case Char: return DoubleNullAwareCharIndirectMultiColumnTimsortKernel.createContext(size);
                case Byte: return DoubleByteIndirectMultiColumnTimsortKernel.createContext(size);
                case Short: return DoubleShortIndirectMultiColumnTimsortKernel.createContext(size);
                case Int: return DoubleIntIndirectMultiColumnTimsortKernel.createContext(size);
                case Long: return DoubleLongIndirectMultiColumnTimsortKernel.createContext(size);
                case Float: return DoubleFloatIndirectMultiColumnTimsortKernel.createContext(size);
                case Double: return DoubleDoubleIndirectMultiColumnTimsortKernel.createContext(size);
                case Object: return DoubleObjectIndirectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Object:
            switch (chunkTypes[1]) {
                case Char: return ObjectNullAwareCharIndirectMultiColumnTimsortKernel.createContext(size);
                case Byte: return ObjectByteIndirectMultiColumnTimsortKernel.createContext(size);
                case Short: return ObjectShortIndirectMultiColumnTimsortKernel.createContext(size);
                case Int: return ObjectIntIndirectMultiColumnTimsortKernel.createContext(size);
                case Long: return ObjectLongIndirectMultiColumnTimsortKernel.createContext(size);
                case Float: return ObjectFloatIndirectMultiColumnTimsortKernel.createContext(size);
                case Double: return ObjectDoubleIndirectMultiColumnTimsortKernel.createContext(size);
                case Object: return ObjectObjectIndirectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            default: return null;
        }
    }
}
