//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.asofjoin.typed.rightincopen.gen;

import static io.deephaven.util.compare.LongComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.LongChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.util.QueryConstants;
import java.lang.Override;
import java.util.Arrays;

final class RightIncrementalAsOfJoinHasherLong extends RightIncrementalAsOfJoinStateManagerTypedBase {
    private ImmutableLongArraySource mainKeySource0;

    private ImmutableLongArraySource alternateKeySource0;

    public RightIncrementalAsOfJoinHasherLong(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableLongArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    private int alternateNextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (alternateTableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            ObjectArraySource sequentialBuilders) {
        final LongChunk<Values> keyChunk0 = sourceKeyChunks[0].asLongChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final long k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                byte rowState = stateSource.getUnsafe(tableLocation);
                if (isStateEmpty(rowState)) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        rowState = alternateStateSource.getUnsafe(alternateTableLocation);
                        if (isStateEmpty(rowState)) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long cookie = getCookieAlternate(alternateTableLocation);
                            hashSlots.set(cookie, alternateTableLocation | alternateInsertMask);
                            if (sequentialBuilders != null) {
                                addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                            } else {
                                addAlternateLeftKey(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState);
                            }
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                        }
                    }
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final long cookie = makeCookieMain(tableLocation);
                    hashSlots.set(cookie, tableLocation | mainInsertMask);
                    if (sequentialBuilders != null) {
                        addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                        stateSource.set(tableLocation, (byte)(ENTRY_RIGHT_IS_EMPTY | ENTRY_LEFT_IS_EMPTY));
                    } else {
                        addLeftKey(tableLocation, rowKeyChunk.get(chunkPosition), (byte) 0);
                    }
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long cookie = getCookieMain(tableLocation);
                    assert hashSlots != null;
                    hashSlots.set(cookie, tableLocation | mainInsertMask);
                    if (sequentialBuilders != null) {
                        addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                    } else {
                        addLeftKey(tableLocation, rowKeyChunk.get(chunkPosition), rowState);
                    }
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            ObjectArraySource sequentialBuilders) {
        final LongChunk<Values> keyChunk0 = sourceKeyChunks[0].asLongChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final long k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                byte rowState = stateSource.getUnsafe(tableLocation);
                if (isStateEmpty(rowState)) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        rowState = alternateStateSource.getUnsafe(alternateTableLocation);
                        if (isStateEmpty(rowState)) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long cookie = getCookieAlternate(alternateTableLocation);
                            hashSlots.set(cookie, alternateTableLocation | alternateInsertMask);
                            if (sequentialBuilders != null) {
                                addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                            } else {
                                addAlternateRightKey(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState);
                            }
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                        }
                    }
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final long cookie = makeCookieMain(tableLocation);
                    hashSlots.set(cookie, tableLocation | mainInsertMask);
                    if (sequentialBuilders != null) {
                        addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                        stateSource.set(tableLocation, (byte)(ENTRY_RIGHT_IS_EMPTY | ENTRY_LEFT_IS_EMPTY));
                    } else {
                        addRightKey(tableLocation, rowKeyChunk.get(chunkPosition), (byte) 0);
                    }
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long cookie = getCookieMain(tableLocation);
                    hashSlots.set(cookie, tableLocation | mainInsertMask);
                    if (sequentialBuilders != null) {
                        addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                    } else {
                        addRightKey(tableLocation, rowKeyChunk.get(chunkPosition), rowState);
                    }
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void probeRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            ObjectArraySource sequentialBuilders) {
        final LongChunk<Values> keyChunk0 = sourceKeyChunks[0].asLongChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final long k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            byte rowState;
            while (!isStateEmpty(rowState = stateSource.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (sequentialBuilders != null) {
                        final long cookie = getCookieMain(tableLocation);
                        hashSlots.set(cookie, tableLocation | mainInsertMask);
                        addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                    } else {
                        addRightKey(tableLocation, rowKeyChunk.get(chunkPosition), rowState);
                    }
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
                final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                if (firstAlternateTableLocation < rehashPointer) {
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (!isStateEmpty(rowState = alternateStateSource.getUnsafe(alternateTableLocation))) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            if (sequentialBuilders != null) {
                                final long cookie = getCookieAlternate(alternateTableLocation);
                                hashSlots.set(cookie, alternateTableLocation | alternateInsertMask);
                                addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                            } else {
                                addAlternateRightKey(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState);
                            }
                            break;
                        }
                        alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                        Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                    }
                }
            }
        }
    }

    private static int hash(long k0) {
        int hash = LongChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(byte state) {
        return state == ENTRY_EMPTY_STATE;
    }

    private boolean migrateOneLocation(int locationToMigrate) {
        final byte currentStateValue = alternateStateSource.getUnsafe(locationToMigrate);
        if (isStateEmpty(currentStateValue)) {
            return false;
        }
        final long k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationTableLocation = hashToTableLocation(hash);
        while (!isStateEmpty(stateSource.getUnsafe(destinationTableLocation))) {
            destinationTableLocation = nextTableLocation(destinationTableLocation);
        }
        mainKeySource0.set(destinationTableLocation, k0);
        stateSource.set(destinationTableLocation, currentStateValue);
        leftRowSetSource.set(destinationTableLocation, alternateLeftRowSetSource.getUnsafe(locationToMigrate));
        alternateLeftRowSetSource.set(locationToMigrate, null);
        rightRowSetSource.set(destinationTableLocation, alternateRightRowSetSource.getUnsafe(locationToMigrate));
        alternateRightRowSetSource.set(locationToMigrate, null);
        final long cookie  = alternateCookieSource.getUnsafe(locationToMigrate);
        migrateCookie(cookie, destinationTableLocation);
        alternateStateSource.set(locationToMigrate, ENTRY_EMPTY_STATE);
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
        this.mainKeySource0 = (ImmutableLongArraySource)super.mainKeySources[0];
        this.alternateKeySource0 = (ImmutableLongArraySource)super.alternateKeySources[0];
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
        final long[] destKeyArray0 = new long[tableSize];
        final byte[] destState = new byte[tableSize];
        Arrays.fill(destState, ENTRY_EMPTY_STATE);
        final long [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final byte [] originalStateArray = stateSource.getArray();
        stateSource.setArray(destState);
        final Object [] oldLeftSource = leftRowSetSource.getArray();
        final Object [] destLeftSource = new Object[tableSize];
        leftRowSetSource.setArray(destLeftSource);
        final Object [] oldRightSource = rightRowSetSource.getArray();
        final Object [] destRightSource = new Object[tableSize];
        rightRowSetSource.setArray(destRightSource);
        final long [] oldModifiedCookie = mainCookieSource.getArray();
        final long [] destModifiedCookie = new long[tableSize];
        Arrays.fill(destModifiedCookie, QueryConstants.NULL_LONG);
        mainCookieSource.setArray(destModifiedCookie);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final byte currentStateValue = originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final long k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty(destState[destinationTableLocation])) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    destLeftSource[destinationTableLocation] = oldLeftSource[sourceBucket];
                    destRightSource[destinationTableLocation] = oldRightSource[sourceBucket];
                    hashSlots.set(oldModifiedCookie[sourceBucket], destinationTableLocation);
                    destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
