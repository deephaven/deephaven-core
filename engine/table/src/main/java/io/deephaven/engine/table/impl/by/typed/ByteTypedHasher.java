/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharTypedHasher and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.typed;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;


import static io.deephaven.util.compare.ByteComparisons.eq;

public class ByteTypedHasher extends StaticChunkedOperatorAggregationStateManagerTypedBase {
    private final ByteArraySource keySource;
    private final ByteArraySource overflowKeySource;

    ByteTypedHasher(ColumnSource<?>[] tableKeySources, int tableSize, double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        this.keySource = (ByteArraySource) super.keySources[0];
        this.overflowKeySource = (ByteArraySource) super.overflowKeySources[0];
    }

    @Override
    protected void build(StaticAggHashHandler handler, RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks) {
        final ByteChunk<? extends Values> keyChunk = sourceKeyChunks[0].asByteChunk();

        for (int chunkPosition = 0; chunkPosition < keyChunk.size(); ++chunkPosition) {
            final byte value = keyChunk.get(chunkPosition);
            final int hash = hash(value);
            final int tableLocation = hashToTableLocation(tableHashPivot, hash);
            if (stateSource.getUnsafe(tableLocation) == EMPTY_RIGHT_VALUE) {
                numEntries++;
                keySource.set(tableLocation, value);
                handler.doMainInsert(tableLocation, chunkPosition);
            } else if (eq(keySource.getUnsafe(tableLocation), value)) {
                handler.doMainFound(tableLocation, chunkPosition);
            } else {
                int overflowLocation = overflowLocationSource.getUnsafe(tableLocation);
                if (!findOverflow(handler, value, chunkPosition, overflowLocation)) {
                    final int newOverflowLocation = allocateOverflowLocation();
                    overflowKeySource.set(newOverflowLocation, value);
                    overflowLocationSource.set(tableLocation, newOverflowLocation);
                    overflowOverflowLocationSource.set(newOverflowLocation, overflowLocation);
                    numEntries++;
                    handler.doOverflowInsert(newOverflowLocation, chunkPosition);
                }
            }
        }
    }

    private int hash(byte value) {
        return ByteChunkHasher.hashInitialSingle(value);
    }

    @Override
    protected void rehashBucket(StaticAggHashHandler handler, int bucket, int destBucket, int bucketsToAdd) {

        final int position = stateSource.getUnsafe(bucket);
        if (position == EMPTY_RIGHT_VALUE) {
            return;
        }

        int mainInsertLocation = maybeMoveMainBucket(handler, bucket, destBucket, bucketsToAdd);

        // now move overflows as necessary
        int overflowLocation = overflowLocationSource.getUnsafe(bucket);
        overflowLocationSource.set(bucket, QueryConstants.NULL_INT);
        overflowLocationSource.set(destBucket, QueryConstants.NULL_INT);

        while (overflowLocation != QueryConstants.NULL_INT) {
            final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);

            final byte overflowKey = overflowKeySource.getUnsafe(overflowLocation);
            final int overflowHash = hash(overflowKey);
            final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
            if (overflowTableLocation == mainInsertLocation) {
                keySource.set(mainInsertLocation, overflowKey);
                stateSource.set(mainInsertLocation, overflowStateSource.getUnsafe(overflowLocation));
                handler.promoteOverflow(overflowLocation, mainInsertLocation);

                overflowStateSource.set(overflowLocation, QueryConstants.NULL_INT);
                overflowKeySource.set(overflowLocation, QueryConstants.NULL_BYTE);
                freeOverflowLocation(overflowLocation);

                mainInsertLocation = -1;
            } else {
                final int oldOverflowLocation = overflowLocationSource.getUnsafe(overflowTableLocation);
                overflowLocationSource.set(overflowTableLocation, overflowLocation);
                overflowOverflowLocationSource.set(overflowLocation, oldOverflowLocation);
            }

            overflowLocation = nextOverflowLocation;
        }
    }

    private int maybeMoveMainBucket(StaticAggHashHandler handler, int bucket, int destBucket, int bucketsToAdd) {
        final byte key = keySource.getUnsafe(bucket);
        final int hash = hash(key);
        final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash);

        final int mainInsertLocation;
        if (location == bucket) {
            // no need to move, the main value stays in the same place
            mainInsertLocation = destBucket;
            stateSource.set(destBucket, EMPTY_RIGHT_VALUE);
        } else {
            mainInsertLocation = bucket;
            keySource.set(destBucket, key);
            stateSource.set(destBucket, stateSource.getUnsafe(bucket));
            handler.moveMain(bucket, destBucket);

            keySource.set(bucket, null);
            stateSource.set(bucket, EMPTY_RIGHT_VALUE);
        }
        return mainInsertLocation;
    }

    private boolean findOverflow(StaticAggHashHandler handler, byte value, int chunkPosition, int overflowLocation) {
        while (overflowLocation != QueryConstants.NULL_INT) {
            if (eq(overflowKeySource.getUnsafe(overflowLocation), value)) {
                handler.doOverflowFound(overflowLocation, chunkPosition);
                return true;
            }
            overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
        }
        return false;
    }

    public int findPositionForKey(Object value) {
        final byte key = TypeUtils.unbox((Byte)value);
        int hash = hash(key);
        final int location = hashToTableLocation(tableHashPivot, hash);

        final int positionValue = stateSource.getUnsafe(location);
        if (positionValue == EMPTY_RIGHT_VALUE) {
            return -1;
        }

        if (eq(keySource.getUnsafe(location), key)) {
            return positionValue;
        }

        int overflowLocation = overflowLocationSource.getUnsafe(location);
        while (overflowLocation != QueryConstants.NULL_INT) {
            if (eq(overflowKeySource.getUnsafe(overflowLocation), key)) {
                return overflowStateSource.getUnsafe(overflowLocation);
            }
            overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
        }

        return -1;
    }
}