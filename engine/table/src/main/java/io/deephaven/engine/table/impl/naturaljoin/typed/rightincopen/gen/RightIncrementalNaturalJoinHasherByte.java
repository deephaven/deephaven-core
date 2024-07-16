//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.naturaljoin.typed.rightincopen.gen;

import static io.deephaven.util.compare.ByteComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.naturaljoin.RightIncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import io.deephaven.util.QueryConstants;
import java.lang.Override;
import java.util.Arrays;

final class RightIncrementalNaturalJoinHasherByte extends RightIncrementalNaturalJoinStateManagerTypedBase {
    private final ImmutableByteArraySource mainKeySource0;

    public RightIncrementalNaturalJoinHasherByte(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableByteArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                RowSet leftRowSetForState = leftRowSet.getUnsafe(tableLocation);
                if (isStateEmpty(leftRowSetForState)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final long leftRowKey = rowKeyChunk.get(chunkPosition);
                    leftRowSet.set(tableLocation, RowSetFactory.fromKeys(leftRowKey));
                    rightRowKey.set(tableLocation, RowSet.NULL_ROW_KEY);
                    modifiedTrackerCookieSource.set(tableLocation, -1L);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long leftRowKey = rowKeyChunk.get(chunkPosition);
                    leftRowSet.getUnsafe(tableLocation).insert(leftRowKey);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long rightRowKeyForState = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition));
                    if (rightRowKeyForState != RowSet.NULL_ROW_KEY && rightRowKeyForState != QueryConstants.NULL_LONG) {
                        final long leftRowKeyForState = leftRowSet.getUnsafe(tableLocation).firstRowKey();
                        throw new IllegalStateException("Natural Join found duplicate right key for " + extractKeyStringFromSourceTable(leftRowKeyForState));
                    }
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void removeRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, RowSet.NULL_ROW_KEY);
                    Assert.eq(oldRightRow, "oldRightRow", rowKeyChunk.get(chunkPosition), "rowKeyChunk.get(chunkPosition)");
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition));
                    if (oldRightRow != RowSet.NULL_ROW_KEY && oldRightRow != QueryConstants.NULL_LONG) {
                        final long leftRowKeyForState = leftRowSet.getUnsafe(tableLocation).firstRowKey();
                        throw new IllegalStateException("Natural Join found duplicate right key for " + extractKeyStringFromSourceTable(leftRowKeyForState));
                    }
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void modifyByRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getUnsafe(tableLocation);
                    Assert.eq(oldRightRow, "oldRightRow", rowKeyChunk.get(chunkPosition), "rowKeyChunk.get(chunkPosition)");
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_MODIFY_PROBE));
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void applyRightShift(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            long shiftDelta, NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition));
                    Assert.eq(oldRightRow + shiftDelta, "oldRightRow + shiftDelta", rowKeyChunk.get(chunkPosition), "rowKeyChunk.get(chunkPosition)");
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT));
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

    private static boolean isStateEmpty(RowSet state) {
        return state == null;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final byte[] destKeyArray0 = new byte[tableSize];
        final Object[] destState = new Object[tableSize];
        Arrays.fill(destState, null);
        final byte [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final Object [] originalStateArray = (Object[])leftRowSet.getArray();
        leftRowSet.setArray(destState);
        final long [] oldRightRowKey = rightRowKey.getArray();
        final long [] destRightRowKey = new long[tableSize];
        rightRowKey.setArray(destRightRowKey);
        final long [] oldModifiedCookie = modifiedTrackerCookieSource.getArray();
        final long [] destModifiedCookie = new long[tableSize];
        modifiedTrackerCookieSource.setArray(destModifiedCookie);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final RowSet currentStateValue = (RowSet)originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final byte k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty((RowSet)destState[destinationTableLocation])) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    destRightRowKey[destinationTableLocation] = oldRightRowKey[sourceBucket];
                    destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
