//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.multijoin.typed.incopen.gen;

import static io.deephaven.util.compare.IntComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.IntChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.multijoin.IncrementalMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import java.lang.Override;
import java.util.Arrays;

final class IncrementalMultiJoinHasherInt extends IncrementalMultiJoinStateManagerTypedBase {
    private ImmutableIntArraySource mainKeySource0;

    private ImmutableIntArraySource alternateKeySource0;

    public IncrementalMultiJoinHasherInt(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableIntArraySource) super.mainKeySources[0];
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
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                int slotValue = slotToOutputRow.getUnsafe(tableLocation);
                if (isStateEmpty(slotValue)) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        slotValue = alternateSlotToOutputRow.getUnsafe(alternateTableLocation);
                        if (isStateEmpty(slotValue)) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            if (tableRedirSource.getLong(slotValue) != NO_REDIRECTION) {
                                throw new IllegalStateException("Duplicate key found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                            }
                            tableRedirSource.set(slotValue, rowKeyChunk.get(chunkPosition));
                            if (modifiedSlotTracker != null) {
                                final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation);
                                alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, RowSequence.NULL_ROW_KEY, trackerFlag));
                            }
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                        }
                    }
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final int outputKey = numEntries - 1;
                    slotToOutputRow.set(tableLocation, outputKey);
                    tableRedirSource.set(outputKey, rowKeyChunk.get(chunkPosition));
                    outputKeySources[0].set((long)outputKey, k0);
                    // NOTE: if there are other tables adding this row this cycle, we will add these into the slot
                    // tracker. However, when the modified slots are processed we will identify the output row as new
                    // for this cycle and ignore the incomplete tracker data.
                    mainModifiedTrackerCookieSource.set(tableLocation, EMPTY_COOKIE_SLOT);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (tableRedirSource.getLong(slotValue) != NO_REDIRECTION) {
                        throw new IllegalStateException("Duplicate key found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                    }
                    tableRedirSource.set(slotValue, rowKeyChunk.get(chunkPosition));
                    if (modifiedSlotTracker != null) {
                        final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation);
                        mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, RowSequence.NULL_ROW_KEY, trackerFlag));
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
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            int slotValue;
            while (!isStateEmpty(slotValue = slotToOutputRow.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long mappedRowKey = tableRedirSource.getUnsafe(slotValue);
                    tableRedirSource.set(slotValue, NO_REDIRECTION);
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
                    while (!isStateEmpty(slotValue = alternateSlotToOutputRow.getUnsafe(alternateTableLocation))) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long mappedRowKey = tableRedirSource.getUnsafe(slotValue);
                            tableRedirSource.set(slotValue, NO_REDIRECTION);
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
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            int slotValue;
            while (!isStateEmpty(slotValue = slotToOutputRow.getUnsafe(tableLocation))) {
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
                    while (!isStateEmpty(slotValue = alternateSlotToOutputRow.getUnsafe(alternateTableLocation))) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long mappedRowKey = tableRedirSource.getUnsafe(slotValue);
                            Assert.eq(rowKeyChunk.get(chunkPosition), "rowKey", mappedRowKey, "mappedRowKey");
                            tableRedirSource.set(slotValue, mappedRowKey + shiftDelta);
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

    protected void modify(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            int slotValue;
            while (!isStateEmpty(slotValue = slotToOutputRow.getUnsafe(tableLocation))) {
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
                    while (!isStateEmpty(slotValue = alternateSlotToOutputRow.getUnsafe(alternateTableLocation))) {
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

    private static int hash(int k0) {
        int hash = IntChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(int state) {
        return state == EMPTY_OUTPUT_ROW;
    }

    private boolean migrateOneLocation(int locationToMigrate) {
        final int currentStateValue = alternateSlotToOutputRow.getUnsafe(locationToMigrate);
        if (isStateEmpty(currentStateValue)) {
            return false;
        }
        final int k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationTableLocation = hashToTableLocation(hash);
        while (!isStateEmpty(slotToOutputRow.getUnsafe(destinationTableLocation))) {
            destinationTableLocation = nextTableLocation(destinationTableLocation);
        }
        mainKeySource0.set(destinationTableLocation, k0);
        slotToOutputRow.set(destinationTableLocation, currentStateValue);
        final long cookie  = alternateModifiedTrackerCookieSource.getUnsafe(locationToMigrate);
        mainModifiedTrackerCookieSource.set(destinationTableLocation, cookie);
        alternateModifiedTrackerCookieSource.set(locationToMigrate, EMPTY_COOKIE_SLOT);
        alternateSlotToOutputRow.set(locationToMigrate, EMPTY_OUTPUT_ROW);
        return true;
    }

    @Override
    protected int rehashInternalPartial(int entriesToRehash) {
        int rehashedEntries = 0;
        while (rehashPointer > 0 && rehashedEntries < entriesToRehash) {
            if (migrateOneLocation(--rehashPointer)) {
                rehashedEntries++;
            }
        }
        return rehashedEntries;
    }

    @Override
    protected void adviseNewAlternate() {
        this.mainKeySource0 = (ImmutableIntArraySource)super.mainKeySources[0];
        this.alternateKeySource0 = (ImmutableIntArraySource)super.alternateKeySources[0];
    }

    @Override
    protected void clearAlternate() {
        super.clearAlternate();
        this.alternateKeySource0 = null;
    }

    @Override
    protected void migrateFront() {
        int location = 0;
        while (migrateOneLocation(location++) && location < alternateTableSize);
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final int[] destKeyArray0 = new int[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_OUTPUT_ROW);
        final int [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final int [] originalStateArray = slotToOutputRow.getArray();
        slotToOutputRow.setArray(destState);
        final long [] oldModifiedCookie = mainModifiedTrackerCookieSource.getArray();
        final long [] destModifiedCookie = new long[tableSize];
        mainModifiedTrackerCookieSource.setArray(destModifiedCookie);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final int currentStateValue = originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final int k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty(destState[destinationTableLocation])) {
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
