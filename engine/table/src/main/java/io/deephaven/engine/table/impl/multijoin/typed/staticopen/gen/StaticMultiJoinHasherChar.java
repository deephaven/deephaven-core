// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.multijoin.typed.staticopen.gen;

import static io.deephaven.util.compare.CharComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.multijoin.StaticMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableCharArraySource;
import java.lang.Override;
import java.util.Arrays;

final class StaticMultiJoinHasherChar extends StaticMultiJoinStateManagerTypedBase {
    private final ImmutableCharArraySource mainKeySource0;

    public StaticMultiJoinHasherChar(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableCharArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, long tableNumber) {
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                long sentinel = slotToOutputRow.getUnsafe(tableLocation);
                if (sentinel == EMPTY_RIGHT_STATE) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final long outputKey = numEntries - 1;
                    slotToOutputRow.set(tableLocation, outputKey);
                    tableRedirSource.set(outputKey, rowKeyChunk.get(chunkPosition));
                    outputKeySources[0].set(outputKey, k0);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (tableRedirSource.getLong(sentinel) != EMPTY_RIGHT_STATE) {
                        throw new IllegalStateException("Duplicate key found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                    }
                    tableRedirSource.set(sentinel, rowKeyChunk.get(chunkPosition));
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    private static int hash(char k0) {
        int hash = CharChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final char[] destKeyArray0 = new char[tableSize];
        final long[] destState = new long[tableSize];
        Arrays.fill(destState, EMPTY_RIGHT_STATE);
        final char [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final long [] originalStateArray = slotToOutputRow.getArray();
        slotToOutputRow.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final long currentStateValue = originalStateArray[sourceBucket];
            if (currentStateValue == EMPTY_RIGHT_STATE) {
                continue;
            }
            final char k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (destState[destinationTableLocation] == EMPTY_RIGHT_STATE) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
