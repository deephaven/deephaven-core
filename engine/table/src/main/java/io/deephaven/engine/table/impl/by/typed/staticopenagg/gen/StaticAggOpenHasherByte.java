// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.staticopenagg.gen;

import static io.deephaven.util.compare.ByteComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sort.timsort.IntIntTimsortKernel;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Byte;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticAggOpenHasherByte extends StaticChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private final ImmutableByteArraySource mainKeySource0;

    public StaticAggOpenHasherByte(ColumnSource[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableByteArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    @Override
    protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            int tableLocation = hashToTableLocation(hash);
            final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
            while (true) {
                if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_OUTPUT_POSITION) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    handler.doMainInsert(tableLocation, chunkPosition);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    handler.doMainFound(tableLocation, chunkPosition);
                    break;
                } else {
                    Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                    tableLocation = (tableLocation + 1) & (tableSize - 1);
                }
            }
        }
    }

    private static int hash(byte k0) {
        int hash = ByteChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    @Override
    protected void rehashInternal(HashHandler handler) {
        final int entries = (int)numEntries;
        try (final WritableIntChunk<ChunkPositions> moveMainSource = WritableIntChunk.makeWritableChunk(entries);
        final WritableIntChunk<ChunkPositions> moveMainDest = WritableIntChunk.makeWritableChunk(entries)) {
            moveMainSource.setSize(entries);
            moveMainDest.setSize(entries);
            int startMovePointer = 0;
            int endMovePointer = entries - 1;
            final int oldSize = tableSize >> 1;
            final byte[] destArray0 = new byte[tableSize];
            final int[] destState = new int[tableSize];
            Arrays.fill(destState, EMPTY_OUTPUT_POSITION);
            for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
                if (mainOutputPosition.getUnsafe(sourceBucket) == EMPTY_OUTPUT_POSITION) {
                    continue;
                }
                final byte k0 = mainKeySource0.getUnsafe(sourceBucket);
                final int hash = hash(k0);
                int tableLocation = hashToTableLocation(hash);
                final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
                while (true) {
                    if (destState[tableLocation] == EMPTY_OUTPUT_POSITION) {
                        destArray0[tableLocation] = k0;
                        destState[tableLocation] = mainOutputPosition.getUnsafe(sourceBucket);
                        if (sourceBucket != tableLocation) {
                            if (tableLocation < oldSize) {
                                moveMainSource.set(startMovePointer, sourceBucket);
                                moveMainDest.set(startMovePointer++, tableLocation);
                            } else {
                                moveMainSource.set(endMovePointer, sourceBucket);
                                moveMainDest.set(endMovePointer--, tableLocation);
                            }
                        }
                        break;
                    } else {
                        Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                        tableLocation = (tableLocation + 1) & (tableSize - 1);
                    }
                }
            }
            mainKeySource0.setArray(destArray0);
            mainOutputPosition.setArray(destState);
            final int copySize = entries - endMovePointer - 1;
            moveMainSource.copyFromTypedChunk(moveMainSource, endMovePointer + 1, startMovePointer, copySize);
            moveMainDest.copyFromTypedChunk(moveMainDest, endMovePointer + 1, startMovePointer, copySize);
            moveMainSource.setSize(startMovePointer + copySize);
            moveMainDest.setSize(startMovePointer + copySize);
            try (final IntIntTimsortKernel.IntIntSortKernelContext sortContext = IntIntTimsortKernel.createContext((int)numEntries)) {
                sortContext.sort(moveMainSource, moveMainDest);
            }
            for (int ii = moveMainSource.size() - 1; ii >= 0; --ii) {
                handler.doMoveMain(moveMainSource.get(ii), moveMainDest.get(ii));
            }
        }
    }

    @Override
    public int findPositionForKey(Object key) {
        final byte k0 = TypeUtils.unbox((Byte)key);
        int hash = hash(k0);
        int tableLocation = hashToTableLocation(hash);
        final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (positionValue == EMPTY_OUTPUT_POSITION) {
                return -1;
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                return positionValue;
            }
            Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
            tableLocation = (tableLocation + 1) & (tableSize - 1);
        }
    }
}
