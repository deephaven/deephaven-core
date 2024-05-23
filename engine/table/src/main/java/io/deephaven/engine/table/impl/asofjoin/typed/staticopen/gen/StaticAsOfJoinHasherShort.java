//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.asofjoin.typed.staticopen.gen;

import static io.deephaven.util.compare.ShortComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ShortChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.asofjoin.StaticAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableShortArraySource;
import io.deephaven.util.mutable.MutableInt;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticAsOfJoinHasherShort extends StaticAsOfJoinStateManagerTypedBase {
    private final ImmutableShortArraySource mainKeySource0;

    public StaticAsOfJoinHasherShort(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableShortArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                Object rightSideSentinel = rightRowSetSource.getUnsafe(tableLocation);
                if (rightSideSentinel == EMPTY_RIGHT_STATE) {
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
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                Object rightSideSentinel = rightRowSetSource.getUnsafe(tableLocation);
                if (rightSideSentinel == EMPTY_RIGHT_STATE) {
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
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (rightRowSetSource.getUnsafe(tableLocation) != EMPTY_RIGHT_STATE) {
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
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (rightRowSetSource.getUnsafe(tableLocation) != EMPTY_RIGHT_STATE) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    addRightKey(tableLocation, rowKeyChunk.get(chunkPosition));
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    private static int hash(short k0) {
        int hash = ShortChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final short[] destKeyArray0 = new short[tableSize];
        final Object[] destState = new Object[tableSize];
        Arrays.fill(destState, EMPTY_RIGHT_STATE);
        final short [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final Object [] originalStateArray = (Object[])rightRowSetSource.getArray();
        rightRowSetSource.setArray(destState);
        final Object [] oldLeftState = leftRowSetSource.getArray();
        final Object [] destLeftState = new Object[tableSize];
        leftRowSetSource.setArray(destLeftState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final Object currentStateValue = (Object)originalStateArray[sourceBucket];
            if (currentStateValue == EMPTY_RIGHT_STATE) {
                continue;
            }
            final short k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (destState[destinationTableLocation] == EMPTY_RIGHT_STATE) {
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
