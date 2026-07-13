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
 * Selects a generated multi-column timsort kernel for the given column chunk types and sort directions,
 * returning null when no specialized kernel exists so callers can fall back to sorting one column at a
 * time.
 */
public final class MultiColumnTimsortDispatcher {
    private MultiColumnTimsortDispatcher() {
    }

    public static <PERMUTE_VALUES_ATTR extends Any> MultiColumnSortKernel<PERMUTE_VALUES_ATTR> makeContext(
            ChunkType[] chunkTypes, SortingOrder[] order, int size) {
        for (final SortingOrder columnOrder : order) {
            if (columnOrder != SortingOrder.Ascending) {
                return null;
            }
        }
        if (chunkTypes.length != 2) {
            return null;
        }
        switch (chunkTypes[0]) {
            case Char:
            switch (chunkTypes[1]) {
                case Char: return NullAwareCharNullAwareCharMultiColumnTimsortKernel.createContext(size);
                case Byte: return NullAwareCharByteMultiColumnTimsortKernel.createContext(size);
                case Short: return NullAwareCharShortMultiColumnTimsortKernel.createContext(size);
                case Int: return NullAwareCharIntMultiColumnTimsortKernel.createContext(size);
                case Long: return NullAwareCharLongMultiColumnTimsortKernel.createContext(size);
                case Float: return NullAwareCharFloatMultiColumnTimsortKernel.createContext(size);
                case Double: return NullAwareCharDoubleMultiColumnTimsortKernel.createContext(size);
                case Object: return NullAwareCharObjectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Byte:
            switch (chunkTypes[1]) {
                case Char: return ByteNullAwareCharMultiColumnTimsortKernel.createContext(size);
                case Byte: return ByteByteMultiColumnTimsortKernel.createContext(size);
                case Short: return ByteShortMultiColumnTimsortKernel.createContext(size);
                case Int: return ByteIntMultiColumnTimsortKernel.createContext(size);
                case Long: return ByteLongMultiColumnTimsortKernel.createContext(size);
                case Float: return ByteFloatMultiColumnTimsortKernel.createContext(size);
                case Double: return ByteDoubleMultiColumnTimsortKernel.createContext(size);
                case Object: return ByteObjectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Short:
            switch (chunkTypes[1]) {
                case Char: return ShortNullAwareCharMultiColumnTimsortKernel.createContext(size);
                case Byte: return ShortByteMultiColumnTimsortKernel.createContext(size);
                case Short: return ShortShortMultiColumnTimsortKernel.createContext(size);
                case Int: return ShortIntMultiColumnTimsortKernel.createContext(size);
                case Long: return ShortLongMultiColumnTimsortKernel.createContext(size);
                case Float: return ShortFloatMultiColumnTimsortKernel.createContext(size);
                case Double: return ShortDoubleMultiColumnTimsortKernel.createContext(size);
                case Object: return ShortObjectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Int:
            switch (chunkTypes[1]) {
                case Char: return IntNullAwareCharMultiColumnTimsortKernel.createContext(size);
                case Byte: return IntByteMultiColumnTimsortKernel.createContext(size);
                case Short: return IntShortMultiColumnTimsortKernel.createContext(size);
                case Int: return IntIntMultiColumnTimsortKernel.createContext(size);
                case Long: return IntLongMultiColumnTimsortKernel.createContext(size);
                case Float: return IntFloatMultiColumnTimsortKernel.createContext(size);
                case Double: return IntDoubleMultiColumnTimsortKernel.createContext(size);
                case Object: return IntObjectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Long:
            switch (chunkTypes[1]) {
                case Char: return LongNullAwareCharMultiColumnTimsortKernel.createContext(size);
                case Byte: return LongByteMultiColumnTimsortKernel.createContext(size);
                case Short: return LongShortMultiColumnTimsortKernel.createContext(size);
                case Int: return LongIntMultiColumnTimsortKernel.createContext(size);
                case Long: return LongLongMultiColumnTimsortKernel.createContext(size);
                case Float: return LongFloatMultiColumnTimsortKernel.createContext(size);
                case Double: return LongDoubleMultiColumnTimsortKernel.createContext(size);
                case Object: return LongObjectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Float:
            switch (chunkTypes[1]) {
                case Char: return FloatNullAwareCharMultiColumnTimsortKernel.createContext(size);
                case Byte: return FloatByteMultiColumnTimsortKernel.createContext(size);
                case Short: return FloatShortMultiColumnTimsortKernel.createContext(size);
                case Int: return FloatIntMultiColumnTimsortKernel.createContext(size);
                case Long: return FloatLongMultiColumnTimsortKernel.createContext(size);
                case Float: return FloatFloatMultiColumnTimsortKernel.createContext(size);
                case Double: return FloatDoubleMultiColumnTimsortKernel.createContext(size);
                case Object: return FloatObjectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Double:
            switch (chunkTypes[1]) {
                case Char: return DoubleNullAwareCharMultiColumnTimsortKernel.createContext(size);
                case Byte: return DoubleByteMultiColumnTimsortKernel.createContext(size);
                case Short: return DoubleShortMultiColumnTimsortKernel.createContext(size);
                case Int: return DoubleIntMultiColumnTimsortKernel.createContext(size);
                case Long: return DoubleLongMultiColumnTimsortKernel.createContext(size);
                case Float: return DoubleFloatMultiColumnTimsortKernel.createContext(size);
                case Double: return DoubleDoubleMultiColumnTimsortKernel.createContext(size);
                case Object: return DoubleObjectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            case Object:
            switch (chunkTypes[1]) {
                case Char: return ObjectNullAwareCharMultiColumnTimsortKernel.createContext(size);
                case Byte: return ObjectByteMultiColumnTimsortKernel.createContext(size);
                case Short: return ObjectShortMultiColumnTimsortKernel.createContext(size);
                case Int: return ObjectIntMultiColumnTimsortKernel.createContext(size);
                case Long: return ObjectLongMultiColumnTimsortKernel.createContext(size);
                case Float: return ObjectFloatMultiColumnTimsortKernel.createContext(size);
                case Double: return ObjectDoubleMultiColumnTimsortKernel.createContext(size);
                case Object: return ObjectObjectMultiColumnTimsortKernel.createContext(size);
                default: return null;
            }
            default: return null;
        }
    }
}
