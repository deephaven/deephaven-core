//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.typed.staticagg.gen;

import static io.deephaven.util.compare.ByteComparisons.eq;
import static io.deephaven.util.compare.FloatComparisons.eq;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.chunk.util.hashing.FloatChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import java.lang.Byte;
import java.lang.Float;
import java.lang.Object;
import java.lang.Override;

final class StaticAggHasherFloatByte extends StaticChunkedOperatorAggregationStateManagerTypedBase {
    private final FloatArraySource mainKeySource0;

    private final FloatArraySource overflowKeySource0;

    private final ByteArraySource mainKeySource1;

    private final ByteArraySource overflowKeySource1;

    public StaticAggHasherFloatByte(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        this.mainKeySource0 = (FloatArraySource) super.mainKeySources[0];
        this.overflowKeySource0 = (FloatArraySource) super.overflowKeySources[0];
        this.mainKeySource1 = (ByteArraySource) super.mainKeySources[1];
        this.overflowKeySource1 = (ByteArraySource) super.overflowKeySources[1];
    }

    @Override
    protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final FloatChunk<Values> keyChunk0 = sourceKeyChunks[0].asFloatChunk();
        final ByteChunk<Values> keyChunk1 = sourceKeyChunks[1].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final float k0 = keyChunk0.get(chunkPosition);
            final byte k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int tableLocation = hashToTableLocation(tableHashPivot, hash);
            if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_OUTPUT_POSITION) {
                numEntries++;
                mainKeySource0.set(tableLocation, k0);
                mainKeySource1.set(tableLocation, k1);
                handler.doMainInsert(tableLocation, chunkPosition);
            } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                handler.doMainFound(tableLocation, chunkPosition);
            } else {
                int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
                if (!findOverflow(handler, k0, k1, chunkPosition, overflowLocation)) {
                    final int newOverflowLocation = allocateOverflowLocation();
                    overflowKeySource0.set(newOverflowLocation, k0);
                    overflowKeySource1.set(newOverflowLocation, k1);
                    mainOverflowLocationSource.set(tableLocation, newOverflowLocation);
                    overflowOverflowLocationSource.set(newOverflowLocation, overflowLocation);
                    numEntries++;
                    handler.doOverflowInsert(newOverflowLocation, chunkPosition);
                }
            }
        }
    }

    @Override
    protected void probe(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final FloatChunk<Values> keyChunk0 = sourceKeyChunks[0].asFloatChunk();
        final ByteChunk<Values> keyChunk1 = sourceKeyChunks[1].asByteChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final float k0 = keyChunk0.get(chunkPosition);
            final byte k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int tableLocation = hashToTableLocation(tableHashPivot, hash);
            if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_OUTPUT_POSITION) {
                handler.doMissing(chunkPosition);
            } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                handler.doMainFound(tableLocation, chunkPosition);
            } else {
                int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
                if (!findOverflow(handler, k0, k1, chunkPosition, overflowLocation)) {
                    handler.doMissing(chunkPosition);
                }
            }
        }
    }

    private static int hash(float k0, byte k1) {
        int hash = FloatChunkHasher.hashInitialSingle(k0);
        hash = ByteChunkHasher.hashUpdateSingle(hash, k1);
        return hash;
    }

    @Override
    protected void rehashBucket(HashHandler handler, int sourceBucket, int destBucket,
            int bucketsToAdd) {
        final int position = mainOutputPosition.getUnsafe(sourceBucket);
        if (position == EMPTY_OUTPUT_POSITION) {
            return;
        }
        int mainInsertLocation = maybeMoveMainBucket(handler, sourceBucket, destBucket, bucketsToAdd);
        int overflowLocation = mainOverflowLocationSource.getUnsafe(sourceBucket);
        mainOverflowLocationSource.set(sourceBucket, QueryConstants.NULL_INT);
        mainOverflowLocationSource.set(destBucket, QueryConstants.NULL_INT);
        while (overflowLocation != QueryConstants.NULL_INT) {
            final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
            final float overflowKey0 = overflowKeySource0.getUnsafe(overflowLocation);
            final byte overflowKey1 = overflowKeySource1.getUnsafe(overflowLocation);
            final int overflowHash = hash(overflowKey0, overflowKey1);
            final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
            if (overflowTableLocation == mainInsertLocation) {
                mainKeySource0.set(mainInsertLocation, overflowKey0);
                mainKeySource1.set(mainInsertLocation, overflowKey1);
                mainOutputPosition.set(mainInsertLocation, overflowOutputPosition.getUnsafe(overflowLocation));
                handler.doPromoteOverflow(overflowLocation, mainInsertLocation);
                overflowOutputPosition.set(overflowLocation, QueryConstants.NULL_INT);
                overflowKeySource0.set(overflowLocation, QueryConstants.NULL_FLOAT);
                overflowKeySource1.set(overflowLocation, QueryConstants.NULL_BYTE);
                freeOverflowLocation(overflowLocation);
                mainInsertLocation = -1;
            } else {
                final int oldOverflowLocation = mainOverflowLocationSource.getUnsafe(overflowTableLocation);
                mainOverflowLocationSource.set(overflowTableLocation, overflowLocation);
                overflowOverflowLocationSource.set(overflowLocation, oldOverflowLocation);
            }
            overflowLocation = nextOverflowLocation;
        }
    }

    private int maybeMoveMainBucket(HashHandler handler, int sourceBucket, int destBucket,
            int bucketsToAdd) {
        final float k0 = mainKeySource0.getUnsafe(sourceBucket);
        final byte k1 = mainKeySource1.getUnsafe(sourceBucket);
        final int hash = hash(k0, k1);
        final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash);
        final int mainInsertLocation;
        if (location == sourceBucket) {
            mainInsertLocation = destBucket;
            mainOutputPosition.set(destBucket, EMPTY_OUTPUT_POSITION);
        } else {
            mainInsertLocation = sourceBucket;
            mainOutputPosition.set(destBucket, mainOutputPosition.getUnsafe(sourceBucket));
            mainOutputPosition.set(sourceBucket, EMPTY_OUTPUT_POSITION);
            mainKeySource0.set(destBucket, k0);
            mainKeySource0.set(sourceBucket, QueryConstants.NULL_FLOAT);
            mainKeySource1.set(destBucket, k1);
            mainKeySource1.set(sourceBucket, QueryConstants.NULL_BYTE);
            handler.doMoveMain(sourceBucket, destBucket);
        }
        return mainInsertLocation;
    }

    private boolean findOverflow(HashHandler handler, float k0, byte k1, int chunkPosition,
            int overflowLocation) {
        while (overflowLocation != QueryConstants.NULL_INT) {
            if (eq(overflowKeySource0.getUnsafe(overflowLocation), k0) && eq(overflowKeySource1.getUnsafe(overflowLocation), k1)) {
                handler.doOverflowFound(overflowLocation, chunkPosition);
                return true;
            }
            overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
        }
        return false;
    }

    @Override
    public int findPositionForKey(Object key) {
        final Object [] ka = (Object[])key;
        final float k0 = TypeUtils.unbox((Float)ka[0]);
        final byte k1 = TypeUtils.unbox((Byte)ka[1]);
        int hash = hash(k0, k1);
        final int tableLocation = hashToTableLocation(tableHashPivot, hash);
        final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
        if (positionValue == EMPTY_OUTPUT_POSITION) {
            return -1;
        }
        if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
            return positionValue;
        }
        int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
        while (overflowLocation != QueryConstants.NULL_INT) {
            if (eq(overflowKeySource0.getUnsafe(overflowLocation), k0) && eq(overflowKeySource1.getUnsafe(overflowLocation), k1)) {
                return overflowOutputPosition.getUnsafe(overflowLocation);
            }
            overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
        }
        return -1;
    }
}
