// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.incopenagg.gen;

import static io.deephaven.util.compare.FloatComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.FloatChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Float;
import java.lang.Object;
import java.lang.Override;

final class IncrementalAggOpenHasherFloat extends IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private final FloatArraySource mainKeySource0;

    public IncrementalAggOpenHasherFloat(ColumnSource[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        this.mainKeySource0 = (FloatArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return tableLocation == tableHashPivot - 1 ? 0 : (tableLocation + 1);
    }

    @Override
    protected void build(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final FloatChunk<Values> keyChunk0 = sourceKeyChunks[0].asFloatChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final float k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            int tableLocation = hashToTableLocation(hash);
            final int lastTableLocation = nextTableLocation(tableLocation);
            while (true) {
                int tableState = mainOutputPosition.getUnsafe(tableLocation);
                if (tableState == EMPTY_OUTPUT_POSITION) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final int outputPosition = nextOutputPosition.getAndIncrement();
                    outputPositions.set(chunkPosition, outputPosition);
                    mainOutputPosition.set(tableLocation, outputPosition);
                    outputPositionToHashSlot.set(outputPosition, tableLocation);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, tableState);
                    break;
                } else {
                    Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                    tableLocation = nextTableLocation(tableLocation);
                }
            }
        }
    }

    private static int hash(float k0) {
        int hash = FloatChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    protected void migrateOneLocation(int locationToMigrate) {
        if (mainOutputPosition.getUnsafe(locationToMigrate) == EMPTY_OUTPUT_POSITION) {
            return;
        }
        final float k0 = mainKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationLocation = hashToTableLocation(hash);
        if (destinationLocation == locationToMigrate) {
            return;
        }
        while (mainOutputPosition.getUnsafe(destinationLocation) == EMPTY_OUTPUT_POSITION) {
            destinationLocation = nextTableLocation(destinationLocation);
        }
        mainKeySource0.set(destinationLocation, mainKeySource0.getUnsafe(locationToMigrate));
        mainOutputPosition.set(destinationLocation, mainOutputPosition.getUnsafe(locationToMigrate));
        mainOutputPosition.set(locationToMigrate, EMPTY_OUTPUT_POSITION);
        outputPositionToHashSlot.set(mainOutputPosition.getUnsafe(destinationLocation), destinationLocation);
    }

    @Override
    protected void rehashInternal(int bucketsToAdd) {
        mainKeySource0.ensureCapacity(tableHashPivot + bucketsToAdd);
        mainOutputPosition.ensureCapacity(tableHashPivot + bucketsToAdd);
        final boolean lastValueExists = mainOutputPosition.getUnsafe(tableHashPivot - 1) != EMPTY_OUTPUT_POSITION;
        final int firstLocationToMigrate = tableHashPivot - (tableSize >> 1);
        int lastLocationToMigrate = firstLocationToMigrate + bucketsToAdd;
        final int frontLocationsToMigrate;;
        if (lastLocationToMigrate >= tableSize) {
            frontLocationsToMigrate = lastLocationToMigrate - tableSize + 1;
            lastLocationToMigrate = tableSize;
        } else {
            frontLocationsToMigrate = lastValueExists ? 1 : 0;
        }
        tableHashPivot += bucketsToAdd;
        for (int locationToMigrate = firstLocationToMigrate; locationToMigrate < lastLocationToMigrate || (locationToMigrate < tableSize && mainOutputPosition.getUnsafe(locationToMigrate) != EMPTY_OUTPUT_POSITION); ++locationToMigrate) {
            migrateOneLocation(locationToMigrate);
        }
        for (int locationToMigrate = 0; locationToMigrate < frontLocationsToMigrate || (locationToMigrate < firstLocationToMigrate && mainOutputPosition.getUnsafe(locationToMigrate) != EMPTY_OUTPUT_POSITION); ++locationToMigrate) {
            migrateOneLocation(locationToMigrate);
        }
    }

    @Override
    public int findPositionForKey(Object key) {
        final float k0 = TypeUtils.unbox((Float)key);
        int hash = hash(k0);
        int tableLocation = hashToTableLocation(hash);
        final int lastTableLocation = nextTableLocation(tableLocation);
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (positionValue == EMPTY_OUTPUT_POSITION) {
                return -1;
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                return positionValue;
            }
            Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
            tableLocation = nextTableLocation(tableLocation);
        }
    }
}
