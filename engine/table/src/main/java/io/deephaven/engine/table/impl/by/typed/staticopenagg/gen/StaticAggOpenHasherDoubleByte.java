//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.typed.staticopenagg.gen;

import static io.deephaven.util.compare.ByteComparisons.eq;
import static io.deephaven.util.compare.DoubleComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.chunk.util.hashing.DoubleChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableDoubleArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Byte;
import java.lang.Double;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticAggOpenHasherDoubleByte extends StaticChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private final ImmutableDoubleArraySource mainKeySource0;

    private final ImmutableByteArraySource mainKeySource1;

    public StaticAggOpenHasherDoubleByte(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableDoubleArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
        this.mainKeySource1 = (ImmutableByteArraySource) super.mainKeySources[1];
        this.mainKeySource1.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void build(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final ByteChunk<Values> keyChunk1 = sourceKeyChunks[1].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final byte k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
                if (outputPosition == EMPTY_OUTPUT_POSITION) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    mainKeySource1.set(tableLocation, k1);
                    outputPosition = nextOutputPosition.getAndIncrement();
                    outputPositions.set(chunkPosition, outputPosition);
                    mainOutputPosition.set(tableLocation, outputPosition);
                    outputPositionToHashSlot.set(outputPosition, tableLocation);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                    outputPositions.set(chunkPosition, outputPosition);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    private static int hash(double k0, byte k1) {
        int hash = DoubleChunkHasher.hashInitialSingle(k0);
        hash = ByteChunkHasher.hashUpdateSingle(hash, k1);
        return hash;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final double[] destKeyArray0 = new double[tableSize];
        final byte[] destKeyArray1 = new byte[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_OUTPUT_POSITION);
        final double [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final byte [] originalKeyArray1 = mainKeySource1.getArray();
        mainKeySource1.setArray(destKeyArray1);
        final int [] originalStateArray = mainOutputPosition.getArray();
        mainOutputPosition.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final int currentStateValue = originalStateArray[sourceBucket];
            if (currentStateValue == EMPTY_OUTPUT_POSITION) {
                continue;
            }
            final double k0 = originalKeyArray0[sourceBucket];
            final byte k1 = originalKeyArray1[sourceBucket];
            final int hash = hash(k0, k1);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (destState[destinationTableLocation] == EMPTY_OUTPUT_POSITION) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destKeyArray1[destinationTableLocation] = k1;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    if (sourceBucket != destinationTableLocation) {
                        outputPositionToHashSlot.set(currentStateValue, destinationTableLocation);
                    }
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }

    @Override
    public int findPositionForKey(Object key) {
        final Object [] ka = (Object[])key;
        final double k0 = TypeUtils.unbox((Double)ka[0]);
        final byte k1 = TypeUtils.unbox((Byte)ka[1]);
        int hash = hash(k0, k1);
        int tableLocation = hashToTableLocation(hash);
        final int firstTableLocation = tableLocation;
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (positionValue == EMPTY_OUTPUT_POSITION) {
                return UNKNOWN_ROW;
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                return positionValue;
            }
            tableLocation = nextTableLocation(tableLocation);
            Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
        }
    }
}
