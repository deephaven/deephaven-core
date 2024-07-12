//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.naturaljoin.typed.staticopen.gen;

import static io.deephaven.util.compare.ByteComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.naturaljoin.DuplicateRightRowDecorationException;
import io.deephaven.engine.table.impl.naturaljoin.StaticNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;

final class StaticNaturalJoinHasherByte extends StaticNaturalJoinStateManagerTypedBase {
    private final ImmutableByteArraySource mainKeySource0;

    public StaticNaturalJoinHasherByte(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableByteArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            IntegerArraySource leftHashSlots, long hashSlotOffset) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                long rightSideSentinel = mainRightRowKey.getUnsafe(tableLocation);
                if (isStateEmpty(rightSideSentinel)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    mainRightRowKey.set(tableLocation, NO_RIGHT_STATE_VALUE);
                    leftHashSlots.set(hashSlotOffset++, tableLocation);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    leftHashSlots.set(hashSlotOffset++, tableLocation);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                long rightSideSentinel = mainRightRowKey.getUnsafe(tableLocation);
                if (isStateEmpty(rightSideSentinel)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final long rightRowKeyToInsert = rowKeyChunk.get(chunkPosition);
                    mainRightRowKey.set(tableLocation, rightRowKeyToInsert);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    mainRightRowKey.set(tableLocation, DUPLICATE_RIGHT_STATE);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void decorateLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource leftRedirections, long redirectionOffset) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            long rightRowKey;
            while (!isStateEmpty(rightRowKey = mainRightRowKey.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (rightRowKey == DUPLICATE_RIGHT_STATE) {
                        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
                        throw new IllegalStateException("Natural Join found duplicate right key for " + extractKeyStringFromSourceTable(rowKeyChunk.get(chunkPosition)));
                    }
                    leftRedirections.set(redirectionOffset++, rightRowKey);
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
                leftRedirections.set(redirectionOffset++, RowSet.NULL_ROW_KEY);
            }
        }
    }

    protected void decorateWithRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            long existingStateValue;
            while (!isStateEmpty(existingStateValue = mainRightRowKey.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (existingStateValue != NO_RIGHT_STATE_VALUE) {
                        mainRightRowKey.set(tableLocation, DUPLICATE_RIGHT_STATE);
                        throw new DuplicateRightRowDecorationException(tableLocation);
                    } else {
                        final long rightRowKeyToInsert = rowKeyChunk.get(chunkPosition);
                        mainRightRowKey.set(tableLocation, rightRowKeyToInsert);
                    }
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    private static int hash(byte k0) {
        int hash = ByteChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(long state) {
        return state == EMPTY_RIGHT_STATE;
    }
}
