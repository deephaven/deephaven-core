//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.typed.incopenaggts.gen;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones;
import java.util.Arrays;

/**
 * The TypedHashDispatcher returns a pre-generated and precompiled hasher instance suitable for the provided column sources, or null if there is not a precompiled hasher suitable for the specified sources.
 */
public class TypedHashDispatcher {
    private TypedHashDispatcher() {
        // static use only
    }

    public static IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones dispatch(
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

    private static IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones dispatchSingle(
            ChunkType chunkType, ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        switch (chunkType) {
            default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType);
            case Char: return new IncrementalAggOpenHasherWithTombstoneChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Byte: return new IncrementalAggOpenHasherWithTombstoneByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Short: return new IncrementalAggOpenHasherWithTombstoneShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Int: return new IncrementalAggOpenHasherWithTombstoneInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Long: return new IncrementalAggOpenHasherWithTombstoneLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Float: return new IncrementalAggOpenHasherWithTombstoneFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Double: return new IncrementalAggOpenHasherWithTombstoneDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Object: return new IncrementalAggOpenHasherWithTombstoneObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
    }

    private static IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones dispatchDouble(
            ChunkType chunkType0, ChunkType chunkType1, ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        switch (chunkType0) {
            default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType0);
            case Char:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new IncrementalAggOpenHasherWithTombstoneCharChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new IncrementalAggOpenHasherWithTombstoneCharByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new IncrementalAggOpenHasherWithTombstoneCharShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new IncrementalAggOpenHasherWithTombstoneCharInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new IncrementalAggOpenHasherWithTombstoneCharLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new IncrementalAggOpenHasherWithTombstoneCharFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new IncrementalAggOpenHasherWithTombstoneCharDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new IncrementalAggOpenHasherWithTombstoneCharObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Byte:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new IncrementalAggOpenHasherWithTombstoneByteChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new IncrementalAggOpenHasherWithTombstoneByteByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new IncrementalAggOpenHasherWithTombstoneByteShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new IncrementalAggOpenHasherWithTombstoneByteInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new IncrementalAggOpenHasherWithTombstoneByteLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new IncrementalAggOpenHasherWithTombstoneByteFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new IncrementalAggOpenHasherWithTombstoneByteDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new IncrementalAggOpenHasherWithTombstoneByteObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Short:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new IncrementalAggOpenHasherWithTombstoneShortChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new IncrementalAggOpenHasherWithTombstoneShortByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new IncrementalAggOpenHasherWithTombstoneShortShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new IncrementalAggOpenHasherWithTombstoneShortInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new IncrementalAggOpenHasherWithTombstoneShortLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new IncrementalAggOpenHasherWithTombstoneShortFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new IncrementalAggOpenHasherWithTombstoneShortDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new IncrementalAggOpenHasherWithTombstoneShortObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Int:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new IncrementalAggOpenHasherWithTombstoneIntChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new IncrementalAggOpenHasherWithTombstoneIntByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new IncrementalAggOpenHasherWithTombstoneIntShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new IncrementalAggOpenHasherWithTombstoneIntInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new IncrementalAggOpenHasherWithTombstoneIntLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new IncrementalAggOpenHasherWithTombstoneIntFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new IncrementalAggOpenHasherWithTombstoneIntDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new IncrementalAggOpenHasherWithTombstoneIntObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Long:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new IncrementalAggOpenHasherWithTombstoneLongChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new IncrementalAggOpenHasherWithTombstoneLongByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new IncrementalAggOpenHasherWithTombstoneLongShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new IncrementalAggOpenHasherWithTombstoneLongInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new IncrementalAggOpenHasherWithTombstoneLongLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new IncrementalAggOpenHasherWithTombstoneLongFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new IncrementalAggOpenHasherWithTombstoneLongDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new IncrementalAggOpenHasherWithTombstoneLongObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Float:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new IncrementalAggOpenHasherWithTombstoneFloatChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new IncrementalAggOpenHasherWithTombstoneFloatByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new IncrementalAggOpenHasherWithTombstoneFloatShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new IncrementalAggOpenHasherWithTombstoneFloatInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new IncrementalAggOpenHasherWithTombstoneFloatLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new IncrementalAggOpenHasherWithTombstoneFloatFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new IncrementalAggOpenHasherWithTombstoneFloatDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new IncrementalAggOpenHasherWithTombstoneFloatObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Double:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new IncrementalAggOpenHasherWithTombstoneDoubleChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new IncrementalAggOpenHasherWithTombstoneDoubleByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new IncrementalAggOpenHasherWithTombstoneDoubleShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new IncrementalAggOpenHasherWithTombstoneDoubleInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new IncrementalAggOpenHasherWithTombstoneDoubleLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new IncrementalAggOpenHasherWithTombstoneDoubleFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new IncrementalAggOpenHasherWithTombstoneDoubleDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new IncrementalAggOpenHasherWithTombstoneDoubleObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            case Object:switch (chunkType1) {
                default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
                case Char: return new IncrementalAggOpenHasherWithTombstoneObjectChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Byte: return new IncrementalAggOpenHasherWithTombstoneObjectByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Short: return new IncrementalAggOpenHasherWithTombstoneObjectShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Int: return new IncrementalAggOpenHasherWithTombstoneObjectInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Long: return new IncrementalAggOpenHasherWithTombstoneObjectLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Float: return new IncrementalAggOpenHasherWithTombstoneObjectFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Double: return new IncrementalAggOpenHasherWithTombstoneObjectDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                case Object: return new IncrementalAggOpenHasherWithTombstoneObjectObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
        }
    }
}
