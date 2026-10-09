//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.join.typed.statickeylongvalue.gen;

import static io.deephaven.util.compare.ByteComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.join.StaticKeyLongValueHasherTypedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import java.lang.Override;
import java.util.Arrays;

final class StaticKeyLongValueHasherByte extends StaticKeyLongValueHasherTypedBase {
    private final ImmutableByteArraySource mainKeySource0;

    public StaticKeyLongValueHasherByte(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableByteArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildChunk(Chunk[] sourceKeyChunks, LongChunk<? extends Any> values) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                long value = mainValue.getUnsafe(tableLocation);
                if (isStateEmpty(value)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    mainValue.set(tableLocation, values.get(chunkPosition));
                    growIfFull();
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long repeated = repeatedValue(value, values.get(chunkPosition));
                    if (repeated != value) {
                        mainValue.set(tableLocation, repeated);
                    }
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

    protected void probeChunk(Chunk[] sourceKeyChunks, WritableLongChunk<Values> values,
            long missingValue) {
        final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final byte k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            long value;
            while (!isStateEmpty(value = mainValue.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    values.set(chunkPosition, value);
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                if (tableLocation == firstTableLocation) {
                    throw Assert.statementNeverExecuted("tableLocation wraps around to firstTableLocation");
                }
            }
            if (!found) {
                values.set(chunkPosition, missingValue);
            }
        }
    }

    private static int hash(byte k0) {
        int hash = ByteChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(long state) {
        return state == EMPTY_VALUE;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final byte[] destKeyArray0 = new byte[tableSize];
        final long[] destState = new long[tableSize];
        Arrays.fill(destState, EMPTY_VALUE);
        final byte [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final long [] originalStateArray = mainValue.getArray();
        mainValue.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final long currentStateValue = originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final byte k0 = originalKeyArray0[sourceBucket];
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
