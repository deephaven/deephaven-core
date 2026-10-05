//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.typed.distinctset.gen;

import static io.deephaven.util.compare.LongComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.LongChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.DistinctKeySet;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import java.lang.IllegalStateException;
import java.lang.Override;
import java.util.Arrays;

final class DistinctSetHasherLong extends DistinctKeySet {
    private ImmutableLongArraySource mainKeySource0;

    private ImmutableLongArraySource alternateKeySource0;

    public DistinctSetHasherLong(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableLongArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    private int alternateNextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (alternateTableSize - 1);
    }

    protected void addKeys(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final LongChunk<Values> keyChunk0 = sourceKeyChunks[0].asLongChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final long k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            int firstDeletedLocation = -1;
            MAIN_SEARCH: while (true) {
                long count = mainCount.getUnsafe(tableLocation);
                if (firstDeletedLocation < 0 && isStateDeleted(count)) {
                    firstDeletedLocation = tableLocation;
                }
                if (isStateEmpty(count)) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        count = alternateCount.getUnsafe(alternateTableLocation);
                        if (isStateEmpty(count)) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            if (isStateDeleted(count)) {
                                break;
                            }
                            if (count == 0) {
                                ++revivedKeys;
                            }
                            alternateCount.set(alternateTableLocation, count + 1);
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            if (alternateTableLocation == firstAlternateTableLocation) {
                                throw Assert.statementNeverExecuted("alternateTableLocation wraps around to firstAlternateTableLocation");
                            }
                        }
                    }
                    if (firstDeletedLocation >= 0) {
                        tableLocation = firstDeletedLocation;
                    } else {
                        numEntries++;
                    }
                    liveEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    mainCount.set(tableLocation, 1L);
                    ++insertedKeys;
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (isStateDeleted(count)) {
                        tableLocation = firstDeletedLocation;
                        liveEntries++;
                        mainKeySource0.set(tableLocation, k0);
                        mainCount.set(tableLocation, 1L);
                        ++insertedKeys;
                        break;
                    }
                    if (count == 0) {
                        ++revivedKeys;
                    }
                    mainCount.set(tableLocation, count + 1);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    if (tableLocation == firstTableLocation) {
                        throw Assert.statementNeverExecuted("tableLocation wraps around to firstTableLocation");
                    }
                }
            }
        }
    }

    protected void removeKeys(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final LongChunk<Values> keyChunk0 = sourceKeyChunks[0].asLongChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final long k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            boolean searchAlternate = true;
            int tableLocation = firstTableLocation;
            long count;
            while (!isStateEmpty(count = mainCount.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (isStateDeleted(count)) {
                        searchAlternate = false;
                        break;
                    }
                    Assert.gtZero(count, "count");
                    mainCount.set(tableLocation, count - 1);
                    if (count == 1) {
                        ++emptiedKeys;
                    }
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                if (tableLocation == firstTableLocation) {
                    throw Assert.statementNeverExecuted("tableLocation wraps around to firstTableLocation");
                }
            }
            if (!found) {
                if (!searchAlternate) {
                    throw new IllegalStateException("Removed key is not in the set");
                } else {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    boolean alternateFound = false;
                    if (firstAlternateTableLocation < rehashPointer) {
                        int alternateTableLocation = firstAlternateTableLocation;
                        while (!isStateEmpty(count = alternateCount.getUnsafe(alternateTableLocation))) {
                            if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                                if (isStateDeleted(count)) {
                                    break;
                                }
                                Assert.gtZero(count, "count");
                                alternateCount.set(alternateTableLocation, count - 1);
                                if (count == 1) {
                                    ++emptiedKeys;
                                }
                                alternateFound = true;
                                break;
                            }
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            if (alternateTableLocation == firstAlternateTableLocation) {
                                throw Assert.statementNeverExecuted("alternateTableLocation wraps around to firstAlternateTableLocation");
                            }
                        }
                    }
                    if (!alternateFound) {
                        throw new IllegalStateException("Removed key is not in the set");
                    }
                }
            }
        }
    }

    protected void tombstoneEmptied(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final LongChunk<Values> keyChunk0 = sourceKeyChunks[0].asLongChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final long k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            boolean searchAlternate = true;
            int tableLocation = firstTableLocation;
            long count;
            while (!isStateEmpty(count = mainCount.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (isStateDeleted(count)) {
                        searchAlternate = false;
                        break;
                    }
                    if (count == 0) {
                        mainCount.set(tableLocation, TOMBSTONE_STATE);
                        --liveEntries;
                    }
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                if (tableLocation == firstTableLocation) {
                    throw Assert.statementNeverExecuted("tableLocation wraps around to firstTableLocation");
                }
            }
            if (!found) {
                if (!searchAlternate) {
                    // A key removed by several rows is replaced when the first of them is probed;
                } else {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    boolean alternateFound = false;
                    if (firstAlternateTableLocation < rehashPointer) {
                        int alternateTableLocation = firstAlternateTableLocation;
                        while (!isStateEmpty(count = alternateCount.getUnsafe(alternateTableLocation))) {
                            if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                                if (isStateDeleted(count)) {
                                    break;
                                }
                                if (count == 0) {
                                    alternateCount.set(alternateTableLocation, TOMBSTONE_STATE);
                                    --liveEntries;
                                }
                                alternateFound = true;
                                break;
                            }
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            if (alternateTableLocation == firstAlternateTableLocation) {
                                throw Assert.statementNeverExecuted("alternateTableLocation wraps around to firstAlternateTableLocation");
                            }
                        }
                    }
                    if (!alternateFound) {
                        // A key removed by several rows is replaced when the first of them is probed;
                    }
                }
            }
        }
    }

    protected void match(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongChunk<OrderedRowKeys> rowKeys, WritableLongChunk<OrderedRowKeys> results,
            boolean inclusion) {
        final LongChunk<Values> keyChunk0 = sourceKeyChunks[0].asLongChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final long k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            boolean searchAlternate = true;
            int tableLocation = firstTableLocation;
            long count;
            while (!isStateEmpty(count = mainCount.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (isStateDeleted(count)) {
                        searchAlternate = false;
                        break;
                    }
                    if ((count > 0) == inclusion) {
                        results.add(rowKeys.get(chunkPosition));
                    }
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                if (tableLocation == firstTableLocation) {
                    throw Assert.statementNeverExecuted("tableLocation wraps around to firstTableLocation");
                }
            }
            if (!found) {
                if (!searchAlternate) {
                    if (!inclusion) {
                        results.add(rowKeys.get(chunkPosition));
                    }
                } else {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    boolean alternateFound = false;
                    if (firstAlternateTableLocation < rehashPointer) {
                        int alternateTableLocation = firstAlternateTableLocation;
                        while (!isStateEmpty(count = alternateCount.getUnsafe(alternateTableLocation))) {
                            if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                                if (isStateDeleted(count)) {
                                    break;
                                }
                                if ((count > 0) == inclusion) {
                                    results.add(rowKeys.get(chunkPosition));
                                }
                                alternateFound = true;
                                break;
                            }
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            if (alternateTableLocation == firstAlternateTableLocation) {
                                throw Assert.statementNeverExecuted("alternateTableLocation wraps around to firstAlternateTableLocation");
                            }
                        }
                    }
                    if (!alternateFound) {
                        if (!inclusion) {
                            results.add(rowKeys.get(chunkPosition));
                        }
                    }
                }
            }
        }
    }

    private static int hash(long k0) {
        int hash = LongChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(long state) {
        return state == EMPTY_STATE;
    }

    private static boolean isStateDeleted(long state) {
        return state == TOMBSTONE_STATE;
    }

    private boolean migrateOneLocation(int locationToMigrate, boolean trueOnDeletedEntry) {
        final long currentStateValue = alternateCount.getUnsafe(locationToMigrate);
        if (isStateEmpty(currentStateValue)) {
            return false;
        }
        if (isStateDeleted(currentStateValue)) {
            alternateEntries--;
            alternateCount.set(locationToMigrate, EMPTY_STATE);
            return trueOnDeletedEntry;
        }
        final long k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationTableLocation = hashToTableLocation(hash);
        long candidateState;
        while (!isStateEmpty(candidateState = mainCount.getUnsafe(destinationTableLocation)) && !isStateDeleted(candidateState)) {
            destinationTableLocation = nextTableLocation(destinationTableLocation);
        }
        mainKeySource0.set(destinationTableLocation, k0);
        mainCount.set(destinationTableLocation, currentStateValue);
        alternateCount.set(locationToMigrate, EMPTY_STATE);
        if (!isStateDeleted(candidateState)) {
            numEntries++;
        }
        alternateEntries--;
        return true;
    }

    @Override
    protected int rehashInternalPartial(int entriesToRehash) {
        final long slotsToExamine = (long) entriesToRehash * DistinctKeySet.REHASH_SLOTS_PER_ENTRY;
        long examinedSlots = 0;
        while (rehashPointer > 0 && examinedSlots < slotsToExamine) {
            migrateOneLocation(--rehashPointer, false);
            ++examinedSlots;
        }
        if (rehashPointer == 0) {
            return entriesToRehash;
        }
        return (int) (examinedSlots / DistinctKeySet.REHASH_SLOTS_PER_ENTRY);
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
        while (migrateOneLocation(location++, true) && location < alternateTableSize);
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final long[] destKeyArray0 = new long[tableSize];
        final long[] destState = new long[tableSize];
        Arrays.fill(destState, EMPTY_STATE);
        final long [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final long [] originalStateArray = mainCount.getArray();
        mainCount.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final long currentStateValue = originalStateArray[sourceBucket];
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
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                if (destinationTableLocation == firstDestinationTableLocation) {
                    throw Assert.statementNeverExecuted("destinationTableLocation wraps around to firstDestinationTableLocation");
                }
            }
        }
    }
}
