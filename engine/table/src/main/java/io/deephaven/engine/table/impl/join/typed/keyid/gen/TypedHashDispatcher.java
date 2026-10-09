//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.join.typed.keyid.gen;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.join.KeyIdHasherTypedBase;
import java.util.Arrays;

/**
 * The TypedHashDispatcher returns a pre-generated and precompiled hasher instance suitable for the provided column sources, or null if there is not a precompiled hasher suitable for the specified sources.
 */
public class TypedHashDispatcher {
    private TypedHashDispatcher() {
        // static use only
    }

    public static KeyIdHasherTypedBase dispatch(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        final ChunkType[] chunkTypes = Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);;
        if (chunkTypes.length == 1) {
            return dispatchSingle(chunkTypes[0], tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
        return null;
    }

    private static KeyIdHasherTypedBase dispatchSingle(ChunkType chunkType,
            ColumnSource[] tableKeySources, ColumnSource[] originalTableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        switch (chunkType) {
            default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType);
            case Char: return new KeyIdHasherChar(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Byte: return new KeyIdHasherByte(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Short: return new KeyIdHasherShort(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Int: return new KeyIdHasherInt(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Long: return new KeyIdHasherLong(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Float: return new KeyIdHasherFloat(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Double: return new KeyIdHasherDouble(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Object: return new KeyIdHasherObject(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
    }
}
