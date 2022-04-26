// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin.typed.rightincopen.gen;

import static io.deephaven.util.compare.ObjectComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ObjectChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.RightIncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.util.QueryConstants;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class RightIncrementalNaturalJoinHasherObject extends RightIncrementalNaturalJoinStateManagerTypedBase {
    private final ImmutableObjectArraySource mainKeySource0;

    public RightIncrementalNaturalJoinHasherObject(ColumnSource[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableObjectArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final ObjectChunk<Object, Values> keyChunk0 = sourceKeyChunks[0].asObjectChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final Object k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                RowSet leftRowSetForState = leftRowSet.getUnsafe(tableLocation);
                if (leftRowSetForState == null) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final long leftRowKey = rowKeyChunk.get(chunkPosition);
                    leftRowSet.set(tableLocation, RowSetFactory.fromKeys(leftRowKey));
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long leftRowKey = rowKeyChunk.get(chunkPosition);
                    leftRowSet.getUnsafe(tableLocation).insert(leftRowKey);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final ObjectChunk<Object, Values> keyChunk0 = sourceKeyChunks[0].asObjectChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final Object k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            RowSet leftRowSetForState;
            while ((leftRowSetForState = leftRowSet.getUnsafe(tableLocation)) != null) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long rightRowKeyForState = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition));
                    if (rightRowKeyForState != RowSet.NULL_ROW_KEY && rightRowKeyForState != QueryConstants.NULL_LONG) {
                        throw new IllegalArgumentException("Duplicate right hand side row for state (TODO: WHAT STATE?)");
                    }
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
            }
        }
    }

    protected void removeRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final ObjectChunk<Object, Values> keyChunk0 = sourceKeyChunks[0].asObjectChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final Object k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            RowSet leftRowSetForState;
            while ((leftRowSetForState = leftRowSet.getUnsafe(tableLocation)) != null) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, RowSet.NULL_ROW_KEY);
                    Assert.eq(oldRightRow, "oldRightRow", rowKeyChunk.get(chunkPosition), "rowKeyChunk.get(chunkPosition)");
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
            }
        }
    }

    protected void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final ObjectChunk<Object, Values> keyChunk0 = sourceKeyChunks[0].asObjectChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final Object k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            RowSet leftRowSetForState;
            while ((leftRowSetForState = leftRowSet.getUnsafe(tableLocation)) != null) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition));
                    if (oldRightRow != RowSet.NULL_ROW_KEY && oldRightRow != QueryConstants.NULL_LONG) {
                        throw new IllegalArgumentException("Duplicate right hand side row for state (TODO: WHAT STATE?)");
                    }
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
            }
        }
    }

    protected void modifyByRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final ObjectChunk<Object, Values> keyChunk0 = sourceKeyChunks[0].asObjectChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final Object k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            RowSet leftRowSetForState;
            while ((leftRowSetForState = leftRowSet.getUnsafe(tableLocation)) != null) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getUnsafe(tableLocation);
                    Assert.eq(oldRightRow, "oldRightRow", rowKeyChunk.get(chunkPosition), "rowKeyChunk.get(chunkPosition)");
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_MODIFY_PROBE));
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
            }
        }
    }

    protected void applyRightShift(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            long shiftDelta, NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final ObjectChunk<Object, Values> keyChunk0 = sourceKeyChunks[0].asObjectChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final Object k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            RowSet leftRowSetForState;
            while ((leftRowSetForState = leftRowSet.getUnsafe(tableLocation)) != null) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition));
                    Assert.eq(oldRightRow - shiftDelta, "oldRightRow - shiftDelta", rowKeyChunk.get(chunkPosition), "rowKeyChunk.get(chunkPosition)");
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT));
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
            }
        }
    }

    private static int hash(Object k0) {
        int hash = ObjectChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final Object[] destKeyArray0 = new Object[tableSize];
        final RowSet[] destState = new RowSet[tableSize];
        Arrays.fill(destState, null);
        final Object [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final RowSet [] originalStateArray = (RowSet[])leftRowSet.getArray();
        leftRowSet.setArray(destState);
        final long [] oldRightRowKey = rightRowKey.getArray();
        final long [] destRightRowKey = new long[tableSize];
        rightRowKey.setArray(destRightRowKey);
        final long [] oldModifiedCookie = modifiedTrackerCookieSource.getArray();
        final long [] destModifiedCookie = new long[tableSize];
        rightRowKey.setArray(destModifiedCookie);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final RowSet currentStateValue = originalStateArray[sourceBucket];
            if (currentStateValue == null) {
                continue;
            }
            final Object k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (destState[destinationTableLocation] == null) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    destRightRowKey[destinationTableLocation] = oldRightRowKey[sourceBucket];
                    destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
