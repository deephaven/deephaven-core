// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.replicators.ReplicateTypedHashers
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin.typed.rightincopen.gen;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.RightIncrementalNaturalJoinStateManagerTypedBase;
import java.util.Arrays;

/**
 * The TypedHashDispatcher returns a pre-generated and precompiled hasher instance suitable for the provided column sources, or null if there is not a precompiled hasher suitable for the specified sources. */
public class TypedHashDispatcher {
    private TypedHashDispatcher() {
        // static use only
    }

    public static RightIncrementalNaturalJoinStateManagerTypedBase dispatch(ColumnSource[] tableKeySources,
            int tableSize, double maximumLoadFactor, double targetLoadFactor) {
        final ChunkType[] chunkTypes = Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);;
        if (chunkTypes.length == 1) {
            return dispatchSingle(chunkTypes[0], tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
        return null;
    }

    private static RightIncrementalNaturalJoinStateManagerTypedBase dispatchSingle(ChunkType chunkType,
            ColumnSource[] tableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        switch (chunkType) {
            default: throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType);
            case Char: return new RightIncrementalNaturalJoinHasherChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Byte: return new RightIncrementalNaturalJoinHasherByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Short: return new RightIncrementalNaturalJoinHasherShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Int: return new RightIncrementalNaturalJoinHasherInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Long: return new RightIncrementalNaturalJoinHasherLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Float: return new RightIncrementalNaturalJoinHasherFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Double: return new RightIncrementalNaturalJoinHasherDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            case Object: return new RightIncrementalNaturalJoinHasherObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        }
    }
}
