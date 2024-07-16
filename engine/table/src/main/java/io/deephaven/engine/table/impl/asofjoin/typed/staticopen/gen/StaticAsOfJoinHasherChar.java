//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.asofjoin.typed.staticopen.gen;

import static io.deephaven.util.compare.CharComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.asofjoin.StaticAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableCharArraySource;
import io.deephaven.util.mutable.MutableInt;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticAsOfJoinHasherChar extends StaticAsOfJoinStateManagerTypedBase {
    private final ImmutableCharArraySource mainKeySource0;

    public StaticAsOfJoinHasherChar(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableCharArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                Object rightSideSentinel = rightRowSetSource.getUnsafe(tableLocation);
                if (isStateEmpty(rightSideSentinel)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    addLeftKey(tableLocation, rowKeyChunk.get(chunkPosition));
                    rightRowSetSource.set(tableLocation, RowSetFactory.builderSequential());
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    addLeftKey(tableLocation, rowKeyChunk.get(chunkPosition));
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                Object rightSideSentinel = rightRowSetSource.getUnsafe(tableLocation);
                if (isStateEmpty(rightSideSentinel)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    addRightKey(tableLocation, rowKeyChunk.get(chunkPosition));
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    addRightKey(tableLocation, rowKeyChunk.get(chunkPosition));
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void decorateLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            IntegerArraySource hashSlots, MutableInt hashSlotOffset,
            RowSetBuilderRandom foundBuilder) {
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(rightRowSetSource.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long indexKey = rowKeyChunk.get(chunkPosition);
                    if (addLeftKey(tableLocation, indexKey) && hashSlots != null) {
                        hashSlots.set(hashSlotOffset.getAndIncrement(), tableLocation);
                        foundBuilder.addKey(indexKey);
                    }
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void decorateWithRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(rightRowSetSource.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    addRightKey(tableLocation, rowKeyChunk.get(chunkPosition));
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    private static int hash(char k0) {
        int hash = CharChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(Object state) {
        return state == EMPTY_RIGHT_STATE;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final char[] destKeyArray0 = new char[tableSize];
        final Object[] destState = new Object[tableSize];
        Arrays.fill(destState, EMPTY_RIGHT_STATE);
        final char [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final Object [] originalStateArray = (Object[])rightRowSetSource.getArray();
        rightRowSetSource.setArray(destState);
        final Object [] oldLeftState = leftRowSetSource.getArray();
        final Object [] destLeftState = new Object[tableSize];
        leftRowSetSource.setArray(destLeftState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final Object currentStateValue = (Object)originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final char k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty(destState[destinationTableLocation])) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    destLeftState[destinationTableLocation] = oldLeftState[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
