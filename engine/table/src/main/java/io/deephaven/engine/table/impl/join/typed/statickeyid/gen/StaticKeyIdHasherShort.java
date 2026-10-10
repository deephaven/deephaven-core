//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.join.typed.statickeyid.gen;

import static io.deephaven.util.compare.ShortComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ShortChunkHasher;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.join.StaticKeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableShortArraySource;
import java.lang.Override;
import java.util.Arrays;

final class StaticKeyIdHasherShort extends StaticKeyIdHasherTypedBase {
    private final ImmutableShortArraySource mainKeySource0;

    public StaticKeyIdHasherShort(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableShortArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void build(Chunk[] sourceKeyChunks, WritableIntChunk<Values> ids,
            WritableByteChunk<Values> statuses) {
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                byte tagValue = mainTag.getUnsafe(tableLocation);
                if (isStateEmpty(tagValue)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final int id = allocateId(tableLocation);
                    mainTag.set(tableLocation, tagFor(hash));
                    mainId.set(tableLocation, id);
                    ids.set(chunkPosition, id);
                    statuses.set(chunkPosition, ADDED);
                    growIfFull();
                    break;
                } else if ((tagValue == tagFor(hash)) && eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    ids.set(chunkPosition, mainId.getUnsafe(tableLocation));
                    statuses.set(chunkPosition, FOUND);
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

    protected void probe(Chunk[] sourceKeyChunks, WritableIntChunk<Values> ids) {
        final ShortChunk<Values> keyChunk0 = sourceKeyChunks[0].asShortChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final short k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            byte tagValue;
            while (!isStateEmpty(tagValue = mainTag.getUnsafe(tableLocation))) {
                if ((tagValue == tagFor(hash)) && eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    ids.set(chunkPosition, mainId.getUnsafe(tableLocation));
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                if (tableLocation == firstTableLocation) {
                    throw Assert.statementNeverExecuted("tableLocation wraps around to firstTableLocation");
                }
            }
            if (!found) {
                ids.set(chunkPosition, NULL_ID);
            }
        }
    }

    private static int hash(short k0) {
        int hash = ShortChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(byte state) {
        return state == EMPTY_TAG;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final short[] destKeyArray0 = new short[tableSize];
        final byte[] destState = new byte[tableSize];
        Arrays.fill(destState, EMPTY_TAG);
        final short [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final byte [] originalStateArray = mainTag.getArray();
        mainTag.setArray(destState);
        final int[] destIds = new int[tableSize];
        final int[] originalIds = mainId.getArray();
        mainId.setArray(destIds);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final byte currentStateValue = originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final short k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty(destState[destinationTableLocation])) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    destIds[destinationTableLocation] = originalIds[sourceBucket];
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
