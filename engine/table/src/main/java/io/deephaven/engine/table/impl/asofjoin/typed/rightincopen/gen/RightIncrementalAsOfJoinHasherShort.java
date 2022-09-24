// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.asofjoin.typed.rightincopen.gen;

import static io.deephaven.util.compare.ShortComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ShortChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableShortArraySource;
import io.deephaven.util.QueryConstants;
import java.lang.Override;
import java.util.Arrays;

final class RightIncrementalAsOfJoinHasherShort extends RightIncrementalAsOfJoinStateManagerTypedBase {
    private ImmutableShortArraySource mainKeySource0;

    private ImmutableShortArraySource alternateKeySource0;

    public RightIncrementalAsOfJoinHasherShort(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableShortArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    private int alternateNextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (alternateTableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            IntegerArraySource hashSlots, ObjectArraySource sequentialBuilders) {
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                byte rowState = stateSource.getUnsafe(tableLocation);
                if (rowState == ENTRY_EMPTY_STATE) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        rowState = alternateStateSource.getUnsafe(alternateTableLocation);
                        if (rowState == ENTRY_EMPTY_STATE) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long cookie = getCookieAlternate(alternateTableLocation);
                            hashSlots.set(cookie, alternateTableLocation | alternateInsertMask);
                            if (sequentialBuilders != null) {
                                addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                            } else {
                                addAlternateLeftIndex(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState);
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
                        addLeftIndex(tableLocation, rowKeyChunk.get(chunkPosition), (byte) 0);
                    }
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long cookie = getCookieMain(tableLocation);
                    assert hashSlots != null;
                    hashSlots.set(cookie, tableLocation | mainInsertMask);
                    if (sequentialBuilders != null) {
                        addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                    } else {
                        addLeftIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState);
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
            IntegerArraySource hashSlots, ObjectArraySource sequentialBuilders) {
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                byte rowState = stateSource.getUnsafe(tableLocation);
                if (rowState == ENTRY_EMPTY_STATE) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        rowState = alternateStateSource.getUnsafe(alternateTableLocation);
                        if (rowState == ENTRY_EMPTY_STATE) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            final long cookie = getCookieAlternate(alternateTableLocation);
                            hashSlots.set(cookie, alternateTableLocation | alternateInsertMask);
                            if (sequentialBuilders != null) {
                                addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                            } else {
                                addAlternateRightIndex(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState);
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
                        addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition), (byte) 0);
                    }
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long cookie = getCookieMain(tableLocation);
                    hashSlots.set(cookie, tableLocation | mainInsertMask);
                    if (sequentialBuilders != null) {
                        addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                    } else {
                        addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState);
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
            IntegerArraySource hashSlots, ObjectArraySource sequentialBuilders) {
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            byte rowState;
            while ((rowState = stateSource.getUnsafe(tableLocation)) != ENTRY_EMPTY_STATE) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (sequentialBuilders != null) {
                        final long cookie = getCookieMain(tableLocation);
                        hashSlots.set(cookie, tableLocation | mainInsertMask);
                        addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                    } else {
                        addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState);
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
                    while ((rowState = alternateStateSource.getUnsafe(alternateTableLocation)) != ENTRY_EMPTY_STATE) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            if (sequentialBuilders != null) {
                                final long cookie = getCookieAlternate(alternateTableLocation);
                                hashSlots.set(cookie, alternateTableLocation | alternateInsertMask);
                                addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition));
                            } else {
                                addAlternateRightIndex(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState);
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

    private static int hash(short k0) {
        int hash = ShortChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private boolean migrateOneLocation(int locationToMigrate, IntegerArraySource hashSlots) {
        final byte currentStateValue = alternateStateSource.getUnsafe(locationToMigrate);
        if (currentStateValue == ENTRY_EMPTY_STATE) {
            return false;
        }
        final short k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationTableLocation = hashToTableLocation(hash);
        while (stateSource.getUnsafe(destinationTableLocation) != ENTRY_EMPTY_STATE) {
            destinationTableLocation = nextTableLocation(destinationTableLocation);
        }
        mainKeySource0.set(destinationTableLocation, k0);
        stateSource.set(destinationTableLocation, currentStateValue);
        leftRowSetSource.set(destinationTableLocation, alternateLeftRowSetSource.getUnsafe(locationToMigrate));
        alternateLeftRowSetSource.set(locationToMigrate, null);
        rightRowSetSource.set(destinationTableLocation, alternateRightRowSetSource.getUnsafe(locationToMigrate));
        alternateRightRowSetSource.set(locationToMigrate, null);
        final long cookie  = alternateCookieSource.getUnsafe(locationToMigrate);
        migrateCookie(cookie, destinationTableLocation, hashSlots);
        alternateStateSource.set(locationToMigrate, ENTRY_EMPTY_STATE);
        return true;
    }

    @Override
    protected int rehashInternalPartial(int entriesToRehash, IntegerArraySource hashSlots) {
        int rehashedEntries = 0;
        while (rehashPointer > 0 && rehashedEntries < entriesToRehash) {
            if (migrateOneLocation(--rehashPointer, hashSlots)) {
                rehashedEntries++;
            }
        }
        return rehashedEntries;
    }

    @Override
    protected void newAlternate() {
        super.newAlternate();
        this.mainKeySource0 = (ImmutableShortArraySource)super.mainKeySources[0];
        this.alternateKeySource0 = (ImmutableShortArraySource)super.alternateKeySources[0];
    }

    @Override
    protected void clearAlternate() {
        super.clearAlternate();
        this.alternateKeySource0 = null;
    }

    @Override
    protected void migrateFront(IntegerArraySource hashSlots) {
        int location = 0;
        while (migrateOneLocation(location++, hashSlots));
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final short[] destKeyArray0 = new short[tableSize];
        final byte[] destState = new byte[tableSize];
        Arrays.fill(destState, ENTRY_EMPTY_STATE);
        final short [] originalKeyArray0 = mainKeySource0.getArray();
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
            if (currentStateValue == ENTRY_EMPTY_STATE) {
                continue;
            }
            final short k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (destState[destinationTableLocation] == ENTRY_EMPTY_STATE) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    destLeftSource[destinationTableLocation] = oldLeftSource[sourceBucket];
                    destRightSource[destinationTableLocation] = oldRightSource[sourceBucket];
                    destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
