// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.multijoin.typed.incopen.gen;

import static io.deephaven.util.compare.ByteComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.multijoin.IncrementalMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import java.lang.Override;
import java.util.Arrays;

final class IncrementalMultiJoinHasherByte extends IncrementalMultiJoinStateManagerTypedBase {
    private ImmutableByteArraySource mainKeySource0;

    private ImmutableByteArraySource alternateKeySource0;

    public IncrementalMultiJoinHasherByte(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableByteArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    private int alternateNextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (alternateTableSize - 1);
    }

    protected void buildFromTable(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                long slotValue = slotToOutputRow.getUnsafe(tableLocation);
                if (slotValue == EMPTY_RIGHT_STATE) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        slotValue = alternateSlotToOutputRow.getUnsafe(alternateTableLocation);
                        if (slotValue == EMPTY_RIGHT_STATE) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            if (tableRedirSource.getLong(slotValue) != EMPTY_RIGHT_STATE) {
                                throw new IllegalStateException("Duplicate key found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                            }
                            tableRedirSource.set(slotValue, rowKeyChunk.get(chunkPosition));
                            if (modifiedSlotTracker != null) {
                                final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation);
                                alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, -1L, trackerFlag));
                            }
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                        }
                    }
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final long outputKey = numEntries - 1;
                    slotToOutputRow.set(tableLocation, outputKey);
                    tableRedirSource.set(outputKey, rowKeyChunk.get(chunkPosition));
                    outputKeySources[0].set(outputKey, k0);
                    mainModifiedTrackerCookieSource.set(tableLocation, -1L);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (tableRedirSource.getLong(slotValue) != EMPTY_RIGHT_STATE) {
                        throw new IllegalStateException("Duplicate key found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                    }
                    tableRedirSource.set(slotValue, rowKeyChunk.get(chunkPosition));
                    if (modifiedSlotTracker != null) {
                        final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation);
                        mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, -1L, trackerFlag));
                    }
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void remove(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            long slotValue;
            while ((slotValue = slotToOutputRow.getUnsafe(tableLocation)) != EMPTY_RIGHT_STATE) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long mappedRowKey = tableRedirSource.getUnsafe(slotValue);
                    tableRedirSource.set(slotValue, EMPTY_RIGHT_STATE);
                    Assert.eq(rowKeyChunk.get(chunkPosition), "rowKey", mappedRowKey, "mappedRowKey");
                    final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation);
                    mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag));
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
                final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                boolean alternateFound = false;
                if (firstAlternateTableLocation < rehashPointer) {
                    int alternateTableLocation = firstAlternateTableLocation;
                    while ((slotValue = alternateSlotToOutputRow.getUnsafe(alternateTableLocation)) != EMPTY_RIGHT_STATE) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long mappedRowKey = tableRedirSource.getUnsafe(slotValue);
                            tableRedirSource.set(slotValue, EMPTY_RIGHT_STATE);
                            Assert.eq(rowKeyChunk.get(chunkPosition), "rowKey", mappedRowKey, "mappedRowKey");
                            final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation);
                            alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag));
                            alternateFound = true;
                            break;
                        }
                        alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                        Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                    }
                }
                if (!alternateFound) {
                    throw new IllegalStateException("Matching row not found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                }
            }
        }
    }

    protected void shift(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag, long shiftDelta) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            long slotValue;
            while ((slotValue = slotToOutputRow.getUnsafe(tableLocation)) != EMPTY_RIGHT_STATE) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long mappedRowKey = tableRedirSource.getUnsafe(slotValue);
                    Assert.eq(rowKeyChunk.get(chunkPosition), "rowKey", mappedRowKey, "mappedRowKey");
                    tableRedirSource.set(slotValue, mappedRowKey + shiftDelta);
                    final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation);
                    mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag));
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
                final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                boolean alternateFound = false;
                if (firstAlternateTableLocation < rehashPointer) {
                    int alternateTableLocation = firstAlternateTableLocation;
                    while ((slotValue = alternateSlotToOutputRow.getUnsafe(alternateTableLocation)) != EMPTY_RIGHT_STATE) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long mappedRowKey = tableRedirSource.getUnsafe(slotValue);
                            Assert.eq(rowKeyChunk.get(chunkPosition), "rowKey", mappedRowKey, "mappedRowKey");
                            tableRedirSource.set(slotValue, mappedRowKey + shiftDelta);
                            final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation);
                            alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag));
                            alternateFound = true;
                            break;
                        }
                        alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                        Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                    }
                }
                if (!alternateFound) {
                    throw new IllegalStateException("Matching row not found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                }
            }
        }
    }

    protected void modify(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            long slotValue;
            while ((slotValue = slotToOutputRow.getUnsafe(tableLocation)) != EMPTY_RIGHT_STATE) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation);
                    mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.modifySlot(cookie, slotValue, tableNumber, trackerFlag));
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
                final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                boolean alternateFound = false;
                if (firstAlternateTableLocation < rehashPointer) {
                    int alternateTableLocation = firstAlternateTableLocation;
                    while ((slotValue = alternateSlotToOutputRow.getUnsafe(alternateTableLocation)) != EMPTY_RIGHT_STATE) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation);
                            alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.modifySlot(cookie, slotValue, tableNumber, trackerFlag));
                            alternateFound = true;
                            break;
                        }
                        alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                        Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                    }
                }
                if (!alternateFound) {
                    throw new IllegalStateException("Matching row not found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                }
            }
        }
    }

    private static int hash(byte k0) {
        int hash = ByteChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private boolean migrateOneLocation(int locationToMigrate,
            MultiJoinModifiedSlotTracker modifiedSlotTracker) {
        final long currentStateValue = alternateSlotToOutputRow.getUnsafe(locationToMigrate);
        if (currentStateValue == EMPTY_RIGHT_STATE) {
            return false;
        }
        final byte k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationTableLocation = hashToTableLocation(hash);
        while (slotToOutputRow.getUnsafe(destinationTableLocation) != EMPTY_RIGHT_STATE) {
            destinationTableLocation = nextTableLocation(destinationTableLocation);
        }
        mainKeySource0.set(destinationTableLocation, k0);
        slotToOutputRow.set(destinationTableLocation, currentStateValue);
        final long cookie  = alternateModifiedTrackerCookieSource.getUnsafe(locationToMigrate);
        mainModifiedTrackerCookieSource.set(destinationTableLocation, cookie);
        alternateModifiedTrackerCookieSource.set(locationToMigrate, -1L);
        alternateSlotToOutputRow.set(locationToMigrate, EMPTY_RIGHT_STATE);
        return true;
    }

    @Override
    protected int rehashInternalPartial(int entriesToRehash,
            MultiJoinModifiedSlotTracker modifiedSlotTracker) {
        int rehashedEntries = 0;
        while (rehashPointer > 0 && rehashedEntries < entriesToRehash) {
            if (migrateOneLocation(--rehashPointer, modifiedSlotTracker)) {
                rehashedEntries++;
            }
        }
        return rehashedEntries;
    }

    @Override
    protected void newAlternate() {
        super.newAlternate();
        this.mainKeySource0 = (ImmutableByteArraySource)super.mainKeySources[0];
        this.alternateKeySource0 = (ImmutableByteArraySource)super.alternateKeySources[0];
    }

    @Override
    protected void clearAlternate() {
        super.clearAlternate();
        this.alternateKeySource0 = null;
    }

    @Override
    protected void migrateFront(MultiJoinModifiedSlotTracker modifiedSlotTracker) {
        int location = 0;
        while (migrateOneLocation(location++, modifiedSlotTracker));
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final byte[] destKeyArray0 = new byte[tableSize];
        final long[] destState = new long[tableSize];
        Arrays.fill(destState, EMPTY_RIGHT_STATE);
        final byte [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final long [] originalStateArray = slotToOutputRow.getArray();
        slotToOutputRow.setArray(destState);
        final long [] oldModifiedCookie = mainModifiedTrackerCookieSource.getArray();
        final long [] destModifiedCookie = new long[tableSize];
        mainModifiedTrackerCookieSource.setArray(destModifiedCookie);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final long currentStateValue = originalStateArray[sourceBucket];
            if (currentStateValue == EMPTY_RIGHT_STATE) {
                continue;
            }
            final byte k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (destState[destinationTableLocation] == EMPTY_RIGHT_STATE) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
