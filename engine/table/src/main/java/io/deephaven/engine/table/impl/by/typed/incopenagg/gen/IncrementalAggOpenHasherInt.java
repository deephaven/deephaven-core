//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.typed.incopenagg.gen;

import static io.deephaven.util.compare.IntComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.IntChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Integer;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class IncrementalAggOpenHasherInt extends IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private ImmutableIntArraySource mainKeySource0;

    private ImmutableIntArraySource alternateKeySource0;

    public IncrementalAggOpenHasherInt(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableIntArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    private int alternateNextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (alternateTableSize - 1);
    }

    protected void build(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
                if (isStateEmpty(outputPosition)) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        outputPosition = alternateOutputPosition.getUnsafe(alternateTableLocation);
                        if (isStateEmpty(outputPosition)) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            outputPositions.set(chunkPosition, outputPosition);
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                            Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                        }
                    }
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    outputPosition = nextOutputPosition.getAndIncrement();
                    outputPositions.set(chunkPosition, outputPosition);
                    mainOutputPosition.set(tableLocation, outputPosition);
                    outputPositionToHashSlot.set(outputPosition, mainInsertMask | tableLocation);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, outputPosition);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void buildInitial(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        Assert.eqZero(rehashPointer, "rehashPointer");
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
                if (isStateEmpty(outputPosition)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    outputPosition = nextOutputPosition.getAndIncrement();
                    outputPositions.set(chunkPosition, outputPosition);
                    mainOutputPosition.set(tableLocation, outputPosition);
                    outputPositionToHashSlot.set(outputPosition, mainInsertMask | tableLocation);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, outputPosition);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void probe(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            int outputPosition;
            while (!isStateEmpty(outputPosition = mainOutputPosition.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, outputPosition);
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
                    while (!isStateEmpty(outputPosition = alternateOutputPosition.getUnsafe(alternateTableLocation))) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            outputPositions.set(chunkPosition, outputPosition);
                            alternateFound = true;
                            break;
                        }
                        alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                        Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                    }
                }
                if (!alternateFound) {
                    throw new IllegalStateException("Missing value in probe");
                }
            }
        }
    }

    private static int hash(int k0) {
        int hash = IntChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(int state) {
        return state == EMPTY_OUTPUT_POSITION;
    }

    private boolean migrateOneLocation(int locationToMigrate) {
        final int currentStateValue = alternateOutputPosition.getUnsafe(locationToMigrate);
        if (isStateEmpty(currentStateValue)) {
            return false;
        }
        final int k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationTableLocation = hashToTableLocation(hash);
        while (!isStateEmpty(mainOutputPosition.getUnsafe(destinationTableLocation))) {
            destinationTableLocation = nextTableLocation(destinationTableLocation);
        }
        mainKeySource0.set(destinationTableLocation, k0);
        mainOutputPosition.set(destinationTableLocation, currentStateValue);
        outputPositionToHashSlot.set(currentStateValue, mainInsertMask | destinationTableLocation);
        alternateOutputPosition.set(locationToMigrate, EMPTY_OUTPUT_POSITION);
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
        Arrays.fill(destState, EMPTY_OUTPUT_POSITION);
        final int [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final int [] originalStateArray = mainOutputPosition.getArray();
        mainOutputPosition.setArray(destState);
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
                    outputPositionToHashSlot.set(currentStateValue, mainInsertMask | destinationTableLocation);
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }

    @Override
    public int findPositionForKey(Object key) {
        final int k0 = TypeUtils.unbox((Integer)key);
        int hash = hash(k0);
        int tableLocation = hashToTableLocation(hash);
        final int firstTableLocation = tableLocation;
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (isStateEmpty(positionValue)) {
                int alternateTableLocation = hashToTableLocationAlternate(hash);
                if (alternateTableLocation >= rehashPointer) {
                    return UNKNOWN_ROW;
                }
                final int firstAlternateTableLocation = alternateTableLocation;
                while (true) {
                    final int alternatePositionValue = alternateOutputPosition.getUnsafe(alternateTableLocation);
                    if (isStateEmpty(alternatePositionValue)) {
                        return UNKNOWN_ROW;
                    }
                    if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                        return alternatePositionValue;
                    }
                    alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                    Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                }
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                return positionValue;
            }
            tableLocation = nextTableLocation(tableLocation);
            Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
        }
    }
}
