//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.typed.distinctset.gen;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.DistinctKeySet;
import java.util.Arrays;

/**
 * The TypedHashDispatcher returns a pre-generated and precompiled hasher instance suitable for the provided column sources, or null if there is not a precompiled hasher suitable for the specified sources.
 */
public class TypedHashDispatcher {
    private TypedHashDispatcher() {
        // static use only
    }

    public static DistinctKeySet dispatch(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        final ChunkType[] chunkTypes = Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);;
        if (chunkTypes.length == 1) {
            return dispatchSingle(chunkTypes[0], tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
        if (chunkTypes.length == 2) {
            return dispatchDouble(chunkTypes[0], chunkTypes[1], tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
        return null;
    }

    private static DistinctKeySet dispatchSingle(ChunkType chunkType,
            ColumnSource[] tableKeySources, ColumnSource[] originalTableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        switch (chunkType) {
            default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType);
            case Char: return new DistinctSetHasherChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Byte: return new DistinctSetHasherByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Short: return new DistinctSetHasherShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Int: return new DistinctSetHasherInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Long: return new DistinctSetHasherLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Float: return new DistinctSetHasherFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Double: return new DistinctSetHasherDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Object: return new DistinctSetHasherObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
    }

    private static DistinctKeySet dispatchDouble(ChunkType chunkType0, ChunkType chunkType1,
            ColumnSource[] tableKeySources, ColumnSource[] originalTableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        switch (chunkType0) {
            default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType0);
            case Char:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new DistinctSetHasherCharChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new DistinctSetHasherCharByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new DistinctSetHasherCharShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new DistinctSetHasherCharInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new DistinctSetHasherCharLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new DistinctSetHasherCharFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new DistinctSetHasherCharDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new DistinctSetHasherCharObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Byte:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new DistinctSetHasherByteChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new DistinctSetHasherByteByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new DistinctSetHasherByteShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new DistinctSetHasherByteInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new DistinctSetHasherByteLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new DistinctSetHasherByteFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new DistinctSetHasherByteDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new DistinctSetHasherByteObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Short:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new DistinctSetHasherShortChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new DistinctSetHasherShortByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new DistinctSetHasherShortShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new DistinctSetHasherShortInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new DistinctSetHasherShortLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new DistinctSetHasherShortFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new DistinctSetHasherShortDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new DistinctSetHasherShortObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Int:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new DistinctSetHasherIntChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new DistinctSetHasherIntByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new DistinctSetHasherIntShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new DistinctSetHasherIntInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new DistinctSetHasherIntLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new DistinctSetHasherIntFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new DistinctSetHasherIntDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new DistinctSetHasherIntObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Long:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new DistinctSetHasherLongChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new DistinctSetHasherLongByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new DistinctSetHasherLongShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new DistinctSetHasherLongInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new DistinctSetHasherLongLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new DistinctSetHasherLongFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new DistinctSetHasherLongDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new DistinctSetHasherLongObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Float:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new DistinctSetHasherFloatChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new DistinctSetHasherFloatByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new DistinctSetHasherFloatShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new DistinctSetHasherFloatInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new DistinctSetHasherFloatLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new DistinctSetHasherFloatFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new DistinctSetHasherFloatDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new DistinctSetHasherFloatObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Double:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new DistinctSetHasherDoubleChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new DistinctSetHasherDoubleByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new DistinctSetHasherDoubleShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new DistinctSetHasherDoubleInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new DistinctSetHasherDoubleLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new DistinctSetHasherDoubleFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new DistinctSetHasherDoubleDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new DistinctSetHasherDoubleObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Object:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new DistinctSetHasherObjectChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new DistinctSetHasherObjectByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new DistinctSetHasherObjectShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new DistinctSetHasherObjectInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new DistinctSetHasherObjectLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new DistinctSetHasherObjectFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new DistinctSetHasherObjectDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new DistinctSetHasherObjectObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
        }
    }
}
