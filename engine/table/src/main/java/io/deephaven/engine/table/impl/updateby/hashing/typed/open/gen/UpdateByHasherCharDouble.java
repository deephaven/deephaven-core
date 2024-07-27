//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.hashing.typed.open.gen;

import static io.deephaven.util.compare.CharComparisons.eq;
import static io.deephaven.util.compare.DoubleComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.chunk.util.hashing.DoubleChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableCharArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableDoubleArraySource;
import io.deephaven.engine.table.impl.updateby.hashing.UpdateByStateManagerTypedBase;
import io.deephaven.util.mutable.MutableInt;
import java.lang.Override;
import java.util.Arrays;

final class UpdateByHasherCharDouble extends UpdateByStateManagerTypedBase {
    private ImmutableCharArraySource mainKeySource0;

    private ImmutableCharArraySource alternateKeySource0;

    private ImmutableDoubleArraySource mainKeySource1;

    private ImmutableDoubleArraySource alternateKeySource1;

    public UpdateByHasherCharDouble(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableCharArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
        this.mainKeySource1 = (ImmutableDoubleArraySource) super.mainKeySources[1];
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
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final DoubleChunk<Values> keyChunk1 = sourceKeyChunks[1].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final double k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                int rowState = stateSource.getUnsafe(tableLocation);
                if (isStateEmpty(rowState)) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        rowState = alternateStateSource.getUnsafe(alternateTableLocation);
                        if (isStateEmpty(rowState)) {
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
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final DoubleChunk<Values> keyChunk1 = sourceKeyChunks[1].asDoubleChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final double k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            int rowState;
            while (!isStateEmpty(rowState = stateSource.getUnsafe(tableLocation))) {
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
                    while (!isStateEmpty(rowState = alternateStateSource.getUnsafe(alternateTableLocation))) {
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

    private static int hash(char k0, double k1) {
        int hash = CharChunkHasher.hashInitialSingle(k0);
        hash = DoubleChunkHasher.hashUpdateSingle(hash, k1);
        return hash;
    }

    private static boolean isStateEmpty(int state) {
        return state == EMPTY_RIGHT_VALUE;
    }

    private boolean migrateOneLocation(int locationToMigrate,
            WritableIntChunk<RowKeys> outputPositions) {
        final int currentStateValue = alternateStateSource.getUnsafe(locationToMigrate);
        if (isStateEmpty(currentStateValue)) {
            return false;
        }
        final char k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final double k1 = alternateKeySource1.getUnsafe(locationToMigrate);
        final int hash = hash(k0, k1);
        int destinationTableLocation = hashToTableLocation(hash);
        while (!isStateEmpty(stateSource.getUnsafe(destinationTableLocation))) {
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
    protected void adviseNewAlternate() {
        this.mainKeySource0 = (ImmutableCharArraySource)super.mainKeySources[0];
        this.alternateKeySource0 = (ImmutableCharArraySource)super.alternateKeySources[0];
        this.mainKeySource1 = (ImmutableDoubleArraySource)super.mainKeySources[1];
        this.alternateKeySource1 = (ImmutableDoubleArraySource)super.alternateKeySources[1];
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
        while (migrateOneLocation(location++, outputPositions) && location < alternateTableSize);
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final char[] destKeyArray0 = new char[tableSize];
        final double[] destKeyArray1 = new double[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_RIGHT_VALUE);
        final char [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final double [] originalKeyArray1 = mainKeySource1.getArray();
        mainKeySource1.setArray(destKeyArray1);
        final int [] originalStateArray = stateSource.getArray();
        stateSource.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final int currentStateValue = originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final char k0 = originalKeyArray0[sourceBucket];
            final double k1 = originalKeyArray1[sourceBucket];
            final int hash = hash(k0, k1);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty(destState[destinationTableLocation])) {
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
