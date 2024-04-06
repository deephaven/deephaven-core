//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.hashing.typed.open.gen;

import static io.deephaven.util.compare.IntComparisons.eq;
import static io.deephaven.util.compare.LongComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.IntChunkHasher;
import io.deephaven.chunk.util.hashing.LongChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.updateby.hashing.UpdateByStateManagerTypedBase;
import java.lang.Override;
import java.util.Arrays;
import org.apache.commons.lang3.mutable.MutableInt;

final class UpdateByHasherIntLong extends UpdateByStateManagerTypedBase {
    private ImmutableIntArraySource mainKeySource0;

    private ImmutableIntArraySource alternateKeySource0;

    private ImmutableLongArraySource mainKeySource1;

    private ImmutableLongArraySource alternateKeySource1;

    public UpdateByHasherIntLong(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableIntArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
        this.mainKeySource1 = (ImmutableLongArraySource) super.mainKeySources[1];
        this.mainKeySource1.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    private int alternateNextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (alternateTableSize - 1);
    }

    protected void buildHashTable(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            MutableInt outputPositionOffset, WritableIntChunk<RowKeys> outputPositions) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<Values> keyChunk1 = sourceKeyChunks[1].asLongChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final long k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                int rowState = stateSource.getUnsafe(tableLocation);
                if (rowState == EMPTY_RIGHT_VALUE) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        rowState = alternateStateSource.getUnsafe(alternateTableLocation);
                        if (rowState == EMPTY_RIGHT_VALUE) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0) && eq(alternateKeySource1.getUnsafe(alternateTableLocation), k1)) {
                            // map the existing bucket to this chunk position;
                            outputPositions.set(chunkPosition, rowState);
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                        }
                    }
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    mainKeySource1.set(tableLocation, k1);
                    // create a new bucket and put it in the hash slot;
                    final int outputPosForLocation = outputPositionOffset.getAndIncrement();
                    stateSource.set(tableLocation, outputPosForLocation);
                    // map the new bucket to this chunk position;
                    outputPositions.set(chunkPosition, outputPosForLocation);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                    // map the existing bucket to this chunk position;
                    outputPositions.set(chunkPosition, rowState);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void probeHashTable(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            WritableIntChunk<RowKeys> outputPositions) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<Values> keyChunk1 = sourceKeyChunks[1].asLongChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final long k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            int rowState;
            while ((rowState = stateSource.getUnsafe(tableLocation)) != EMPTY_RIGHT_VALUE) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                    // map the existing bucket to this chunk position;
                    outputPositions.set(chunkPosition, rowState);
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
                    while ((rowState = alternateStateSource.getUnsafe(alternateTableLocation)) != EMPTY_RIGHT_VALUE) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0) && eq(alternateKeySource1.getUnsafe(alternateTableLocation), k1)) {
                            // map the existing bucket (from alternate) to this chunk position;
                            outputPositions.set(chunkPosition, rowState);
                            alternateFound = true;
                            break;
                        }
                        alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                        Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                    }
                }
                if (!alternateFound) {
                    // throw exception if the bucket isn't found;
                    throw new IllegalStateException("Failed to find main aggregation slot for key " + extractKeyStringFromSourceTable(rowKeyChunk.get(chunkPosition)));
                }
            }
        }
    }

    private static int hash(int k0, long k1) {
        int hash = IntChunkHasher.hashInitialSingle(k0);
        hash = LongChunkHasher.hashUpdateSingle(hash, k1);
        return hash;
    }

    private boolean migrateOneLocation(int locationToMigrate,
            WritableIntChunk<RowKeys> outputPositions) {
        final int currentStateValue = alternateStateSource.getUnsafe(locationToMigrate);
        if (currentStateValue == EMPTY_RIGHT_VALUE) {
            return false;
        }
        final int k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final long k1 = alternateKeySource1.getUnsafe(locationToMigrate);
        final int hash = hash(k0, k1);
        int destinationTableLocation = hashToTableLocation(hash);
        while (stateSource.getUnsafe(destinationTableLocation) != EMPTY_RIGHT_VALUE) {
            destinationTableLocation = nextTableLocation(destinationTableLocation);
        }
        mainKeySource0.set(destinationTableLocation, k0);
        mainKeySource1.set(destinationTableLocation, k1);
        stateSource.set(destinationTableLocation, currentStateValue);
        alternateStateSource.set(locationToMigrate, EMPTY_RIGHT_VALUE);
        return true;
    }

    @Override
    protected int rehashInternalPartial(int entriesToRehash,
            WritableIntChunk<RowKeys> outputPositions) {
        int rehashedEntries = 0;
        while (rehashPointer > 0 && rehashedEntries < entriesToRehash) {
            if (migrateOneLocation(--rehashPointer, outputPositions)) {
                rehashedEntries++;
            }
        }
        return rehashedEntries;
    }

    @Override
    protected void newAlternate() {
        super.newAlternate();
        this.mainKeySource0 = (ImmutableIntArraySource)super.mainKeySources[0];
        this.alternateKeySource0 = (ImmutableIntArraySource)super.alternateKeySources[0];
        this.mainKeySource1 = (ImmutableLongArraySource)super.mainKeySources[1];
        this.alternateKeySource1 = (ImmutableLongArraySource)super.alternateKeySources[1];
    }

    @Override
    protected void clearAlternate() {
        super.clearAlternate();
        this.alternateKeySource0 = null;
        this.alternateKeySource1 = null;
    }

    @Override
    protected void migrateFront(WritableIntChunk<RowKeys> outputPositions) {
        int location = 0;
        while (migrateOneLocation(location++, outputPositions));
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final int[] destKeyArray0 = new int[tableSize];
        final long[] destKeyArray1 = new long[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_RIGHT_VALUE);
        final int [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final long [] originalKeyArray1 = mainKeySource1.getArray();
        mainKeySource1.setArray(destKeyArray1);
        final int [] originalStateArray = stateSource.getArray();
        stateSource.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final int currentStateValue = originalStateArray[sourceBucket];
            if (currentStateValue == EMPTY_RIGHT_VALUE) {
                continue;
            }
            final int k0 = originalKeyArray0[sourceBucket];
            final long k1 = originalKeyArray1[sourceBucket];
            final int hash = hash(k0, k1);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (destState[destinationTableLocation] == EMPTY_RIGHT_VALUE) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destKeyArray1[destinationTableLocation] = k1;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
