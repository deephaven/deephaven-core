// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.staticopenagg.gen;

import static io.deephaven.util.compare.FloatComparisons.eq;
import static io.deephaven.util.compare.LongComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.FloatChunkHasher;
import io.deephaven.chunk.util.hashing.LongChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableFloatArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Float;
import java.lang.Long;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticAggOpenHasherLongFloat extends StaticChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private final ImmutableLongArraySource mainKeySource0;

    private final ImmutableFloatArraySource mainKeySource1;

    public StaticAggOpenHasherLongFloat(ColumnSource[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableLongArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
        this.mainKeySource1 = (ImmutableFloatArraySource) super.mainKeySources[1];
        this.mainKeySource1.ensureCapacity(tableSize);
    }

    @Override
    protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final LongChunk<Values> keyChunk0 = sourceKeyChunks[0].asLongChunk();
        final FloatChunk<Values> keyChunk1 = sourceKeyChunks[1].asFloatChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final long k0 = keyChunk0.get(chunkPosition);
            final float k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            int tableLocation = hashToTableLocation(hash);
            final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
            while (true) {
                if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_OUTPUT_POSITION) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    mainKeySource1.set(tableLocation, k1);
                    handler.doMainInsert(tableLocation, chunkPosition);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                    handler.doMainFound(tableLocation, chunkPosition);
                    break;
                } else {
                    Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                    tableLocation = (tableLocation + 1) & (tableSize - 1);
                }
            }
        }
    }

    private static int hash(long k0, float k1) {
        int hash = LongChunkHasher.hashInitialSingle(k0);
        hash = FloatChunkHasher.hashUpdateSingle(hash, k1);
        return hash;
    }

    @Override
    protected void rehashInternal(HashHandler handler) {
        final int entries = (int)numEntries;
        final int oldSize = tableSize >> 1;
        final long[] destArray0 = new long[tableSize];
        final float[] destArray1 = new float[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_OUTPUT_POSITION);
        final long [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destArray0);
        final float [] originalKeyArray1 = mainKeySource1.getArray();
        mainKeySource1.setArray(destArray1);
        final int [] originalStateArray = mainOutputPosition.getArray();
        mainOutputPosition.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            if (originalStateArray[sourceBucket] == EMPTY_OUTPUT_POSITION) {
                continue;
            }
            final long k0 = originalKeyArray0[sourceBucket];
            final float k1 = originalKeyArray1[sourceBucket];
            final int hash = hash(k0, k1);
            int tableLocation = hashToTableLocation(hash);
            final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
            while (true) {
                if (destState[tableLocation] == EMPTY_OUTPUT_POSITION) {
                    destArray0[tableLocation] = k0;
                    destArray1[tableLocation] = k1;
                    destState[tableLocation] = mainOutputPosition.getUnsafe(sourceBucket);
                    if (sourceBucket != tableLocation) {
                        handler.doMoveMain(sourceBucket, tableLocation);
                    }
                    break;
                } else {
                    Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                    tableLocation = (tableLocation + 1) & (tableSize - 1);
                }
            }
        }
    }

    @Override
    public int findPositionForKey(Object key) {
        final Object [] ka = (Object[])key;
        final long k0 = TypeUtils.unbox((Long)ka[0]);
        final float k1 = TypeUtils.unbox((Float)ka[1]);
        int hash = hash(k0, k1);
        int tableLocation = hashToTableLocation(hash);
        final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (positionValue == EMPTY_OUTPUT_POSITION) {
                return -1;
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                return positionValue;
            }
            Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
            tableLocation = (tableLocation + 1) & (tableSize - 1);
        }
    }
}
