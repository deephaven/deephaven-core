//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.typed.staticagg.gen;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;
import java.util.Arrays;

/**
 * The TypedHashDispatcher returns a pre-generated and precompiled hasher instance suitable for the provided column sources, or null if there is not a precompiled hasher suitable for the specified sources.
 */
public class TypedHashDispatcher {
    private TypedHashDispatcher() {
        // static use only
    }

    public static StaticChunkedOperatorAggregationStateManagerTypedBase dispatch(
            ColumnSource[] tableKeySources, ColumnSource[] originalTableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        final ChunkType[] chunkTypes = Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);;
        if (chunkTypes.length == 1) {
            return dispatchSingle(chunkTypes[0], tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
        if (chunkTypes.length == 2) {
            return dispatchDouble(chunkTypes[0], chunkTypes[1], tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
        return null;
    }

    private static StaticChunkedOperatorAggregationStateManagerTypedBase dispatchSingle(
            ChunkType chunkType, ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        switch (chunkType) {
            default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType);
            case Char: return new StaticAggHasherChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Byte: return new StaticAggHasherByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Short: return new StaticAggHasherShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Int: return new StaticAggHasherInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Long: return new StaticAggHasherLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Float: return new StaticAggHasherFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Double: return new StaticAggHasherDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Object: return new StaticAggHasherObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
    }

    private static StaticChunkedOperatorAggregationStateManagerTypedBase dispatchDouble(
            ChunkType chunkType0, ChunkType chunkType1, ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        switch (chunkType0) {
            default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType0);
            case Char:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new StaticAggHasherCharChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new StaticAggHasherCharByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new StaticAggHasherCharShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new StaticAggHasherCharInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new StaticAggHasherCharLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new StaticAggHasherCharFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new StaticAggHasherCharDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new StaticAggHasherCharObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Byte:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new StaticAggHasherByteChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new StaticAggHasherByteByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new StaticAggHasherByteShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new StaticAggHasherByteInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new StaticAggHasherByteLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new StaticAggHasherByteFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new StaticAggHasherByteDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new StaticAggHasherByteObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Short:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new StaticAggHasherShortChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new StaticAggHasherShortByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new StaticAggHasherShortShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new StaticAggHasherShortInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new StaticAggHasherShortLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new StaticAggHasherShortFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new StaticAggHasherShortDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new StaticAggHasherShortObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Int:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new StaticAggHasherIntChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new StaticAggHasherIntByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new StaticAggHasherIntShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new StaticAggHasherIntInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new StaticAggHasherIntLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new StaticAggHasherIntFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new StaticAggHasherIntDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new StaticAggHasherIntObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Long:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new StaticAggHasherLongChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new StaticAggHasherLongByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new StaticAggHasherLongShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new StaticAggHasherLongInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new StaticAggHasherLongLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new StaticAggHasherLongFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new StaticAggHasherLongDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new StaticAggHasherLongObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Float:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new StaticAggHasherFloatChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new StaticAggHasherFloatByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new StaticAggHasherFloatShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new StaticAggHasherFloatInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new StaticAggHasherFloatLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new StaticAggHasherFloatFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new StaticAggHasherFloatDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new StaticAggHasherFloatObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Double:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new StaticAggHasherDoubleChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new StaticAggHasherDoubleByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new StaticAggHasherDoubleShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new StaticAggHasherDoubleInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new StaticAggHasherDoubleLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new StaticAggHasherDoubleFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new StaticAggHasherDoubleDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new StaticAggHasherDoubleObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Object:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new StaticAggHasherObjectChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new StaticAggHasherObjectByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new StaticAggHasherObjectShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new StaticAggHasherObjectInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new StaticAggHasherObjectLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new StaticAggHasherObjectFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new StaticAggHasherObjectDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new StaticAggHasherObjectObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
        }
    }
}
