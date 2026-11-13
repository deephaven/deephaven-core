//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.join.typed.inckeyid.gen;

import static io.deephaven.util.compare.DoubleComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.DoubleChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.join.IncrementalKeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableDoubleArraySource;
import java.lang.Override;
import java.util.Arrays;

final class IncrementalKeyIdHasherDouble extends IncrementalKeyIdHasherTypedBase {
    private ImmutableDoubleArraySource mainKeySource0;

    private ImmutableDoubleArraySource alternateKeySource0;

    public IncrementalKeyIdHasherDouble(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableDoubleArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    private int alternateNextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (alternateTableSize - 1);
    }

    protected void build(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            WritableIntChunk<Values> ids) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            int firstDeletedLocation = -1;
            MAIN_SEARCH: while (true) {
                int idValue = mainId.getUnsafe(tableLocation);
                if (firstDeletedLocation < 0 && isStateDeleted(idValue)) {
                    firstDeletedLocation = tableLocation;
                }
                if (isStateEmpty(idValue)) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        idValue = alternateId.getUnsafe(alternateTableLocation);
                        if (isStateEmpty(idValue)) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            if (isStateDeleted(idValue)) {
                                break;
                            }
                            ids.set(chunkPosition, idValue);
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
                    final int id = allocateId(tableLocation);
                    mainId.set(tableLocation, id);
                    ids.set(chunkPosition, id);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (isStateDeleted(idValue)) {
                        tableLocation = firstDeletedLocation;
                        liveEntries++;
                        mainKeySource0.set(tableLocation, k0);
                        final int id = allocateId(tableLocation);
                        mainId.set(tableLocation, id);
                        ids.set(chunkPosition, id);
                        break;
                    }
                    ids.set(chunkPosition, idValue);
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

    protected void probe(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            WritableIntChunk<Values> ids) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            boolean searchAlternate = true;
            int tableLocation = firstTableLocation;
            int idValue;
            while (!isStateEmpty(idValue = mainId.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (isStateDeleted(idValue)) {
                        searchAlternate = false;
                        break;
                    }
                    ids.set(chunkPosition, idValue);
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
                    ids.set(chunkPosition, NULL_ID);
                } else {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    boolean alternateFound = false;
                    if (firstAlternateTableLocation < rehashPointer) {
                        int alternateTableLocation = firstAlternateTableLocation;
                        while (!isStateEmpty(idValue = alternateId.getUnsafe(alternateTableLocation))) {
                            if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                                if (isStateDeleted(idValue)) {
                                    break;
                                }
                                ids.set(chunkPosition, idValue);
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
                        ids.set(chunkPosition, NULL_ID);
                    }
                }
            }
        }
    }

    private static int hash(double k0) {
        int hash = DoubleChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(int state) {
        return state == EMPTY_ID;
    }

    private static boolean isStateDeleted(int state) {
        return state == TOMBSTONE_ID;
    }

    private boolean migrateOneLocation(int locationToMigrate, boolean trueOnDeletedEntry) {
        final int currentStateValue = alternateId.getUnsafe(locationToMigrate);
        if (isStateEmpty(currentStateValue)) {
            return false;
        }
        if (isStateDeleted(currentStateValue)) {
            alternateEntries--;
            alternateId.set(locationToMigrate, EMPTY_ID);
            return trueOnDeletedEntry;
        }
        final double k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationTableLocation = hashToTableLocation(hash);
        int candidateState;
        while (!isStateEmpty(candidateState = mainId.getUnsafe(destinationTableLocation)) && !isStateDeleted(candidateState)) {
            destinationTableLocation = nextTableLocation(destinationTableLocation);
        }
        mainKeySource0.set(destinationTableLocation, k0);
        mainId.set(destinationTableLocation, currentStateValue);
        idToSlot.set(currentStateValue, destinationTableLocation);
        alternateId.set(locationToMigrate, EMPTY_ID);
        if (!isStateDeleted(candidateState)) {
            numEntries++;
        }
        alternateEntries--;
        return true;
    }

    @Override
    protected int rehashInternalPartial(int entriesToRehash) {
        final long slotsToExamine = (long) entriesToRehash * IncrementalKeyIdHasherTypedBase.REHASH_SLOTS_PER_ENTRY;
        long examinedSlots = 0;
        while (rehashPointer > 0 && examinedSlots < slotsToExamine) {
            migrateOneLocation(--rehashPointer, false);
            ++examinedSlots;
        }
        if (rehashPointer == 0) {
            return entriesToRehash;
        }
        return (int) (examinedSlots / IncrementalKeyIdHasherTypedBase.REHASH_SLOTS_PER_ENTRY);
    }

    @Override
    protected void adviseNewAlternate() {
        this.mainKeySource0 = (ImmutableDoubleArraySource)super.mainKeySources[0];
        this.alternateKeySource0 = (ImmutableDoubleArraySource)super.alternateKeySources[0];
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
        final double[] destKeyArray0 = new double[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_ID);
        final double [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final int [] originalStateArray = mainId.getArray();
        mainId.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final int currentStateValue = originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final double k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty(destState[destinationTableLocation])) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    idToSlot.set(currentStateValue, destinationTableLocation);
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
