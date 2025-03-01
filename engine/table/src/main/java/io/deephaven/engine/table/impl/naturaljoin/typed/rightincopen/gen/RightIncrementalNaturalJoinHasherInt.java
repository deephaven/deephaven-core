//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.naturaljoin.typed.rightincopen.gen;

import static io.deephaven.util.compare.IntComparisons.eq;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.IntChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.naturaljoin.RightIncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.util.QueryConstants;
import java.lang.Override;
import java.util.Arrays;

final class RightIncrementalNaturalJoinHasherInt extends RightIncrementalNaturalJoinStateManagerTypedBase {
    private final ImmutableIntArraySource mainKeySource0;

    public RightIncrementalNaturalJoinHasherInt(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor, NaturalJoinType joinType, boolean addOnly) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, joinType, addOnly);
        this.mainKeySource0 = (ImmutableIntArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                RowSet leftRowSetForState = leftRowSet.getUnsafe(tableLocation);
                if (isStateEmpty(leftRowSetForState)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final long leftRowKey = rowKeyChunk.get(chunkPosition);
                    leftRowSet.set(tableLocation, RowSetFactory.fromKeys(leftRowKey));
                    rightRowKey.set(tableLocation, RowSet.NULL_ROW_KEY);
                    modifiedTrackerCookieSource.set(tableLocation, -1L);
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
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long rightRowKeyForState = rightRowKey.getUnsafe(tableLocation);
                    if (rightRowKeyForState == QueryConstants.NULL_LONG) {
                        // no matching LHS row, ignore;
                    } else if (rightRowKeyForState == RowSet.NULL_ROW_KEY) {
                        // we have a matching LHS row, add this new RHS row;
                        rightRowKey.set(tableLocation, rowKeyChunk.get(chunkPosition));
                    } else if (rightRowKeyForState <= FIRST_DUPLICATE) {
                        // another duplicate, add it to the list;
                        final long duplicateLocation = duplicateLocationFromRowKey(rightRowKeyForState);
                        rightSideDuplicateRowSets.getUnsafe(duplicateLocation).insert(rowKeyChunk.get(chunkPosition));
                    } else {
                        // we have a duplicate, how to handle it?;
                        if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
                            final long leftRowKeyForState = leftRowSet.getUnsafe(tableLocation).firstRowKey();
                            throw new IllegalStateException("Natural Join found duplicate right key for " + extractKeyStringFromSourceTable(leftRowKeyForState));
                        } else if (addOnly && joinType == NaturalJoinType.FIRST_MATCH) {
                            // nop, we already have the first match;
                        } else if (addOnly && joinType == NaturalJoinType.LAST_MATCH) {
                            // we have a later match;
                            rightRowKey.set(tableLocation, rowKeyChunk.get(chunkPosition));
                        } else {
                            // create a duplicate rowset and add the new row to it;
                            final long duplicateLocation = allocateDuplicateLocation();
                            rightSideDuplicateRowSets.set(duplicateLocation, RowSetFactory.fromKeys(rightRowKeyForState, rowKeyChunk.get(chunkPosition)));
                            rightRowKey.set(tableLocation, rowKeyFromDuplicateLocation(duplicateLocation));
                        }
                    }
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void removeRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long rightRowKeyForState = rightRowKey.getUnsafe(tableLocation);
                    if (rightRowKeyForState <= FIRST_DUPLICATE) {
                        // remove from the duplicate row set;
                        final long duplicateLocation = duplicateLocationFromRowKey(rightRowKeyForState);
                        final WritableRowSet duplicates = rightSideDuplicateRowSets.getUnsafe(duplicateLocation);
                        final long duplicateSize = duplicates.size();
                        final long inputKey = rowKeyChunk.get(chunkPosition);
                        final long originalKey = removeRightRowKeyFromDuplicates(duplicates, inputKey, joinType);
                        Assert.eq(duplicateSize, "duplicateSize", duplicates.size() + 1, "duplicates.size() + 1");
                        if (originalKey == inputKey) {
                            // we have a new output key for the LHS rows;;
                            modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                        }
                        if (duplicates.size() == 1) {
                            final long newKey = getRightRowKeyFromDuplicates(duplicates, joinType);;
                            rightRowKey.set(tableLocation, newKey);
                            freeDuplicateLocation(duplicateLocation);;
                        }
                    } else {
                        Assert.eq(rightRowKeyForState, "oldRightRow", rowKeyChunk.get(chunkPosition), "rowKeyChunk.get(chunkPosition)");
                        rightRowKey.set(tableLocation, RowSet.NULL_ROW_KEY);
                        modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                    }
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long rightRowKeyForState = rightRowKey.getUnsafe(tableLocation);
                    if (rightRowKeyForState == QueryConstants.NULL_LONG) {
                        // no matching LHS row, ignore;
                    } else if (rightRowKeyForState == RowSet.NULL_ROW_KEY) {
                        // we have a matching LHS row, add this new RHS row;
                        rightRowKey.set(tableLocation, rowKeyChunk.get(chunkPosition));
                        modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                    } else if (rightRowKeyForState <= FIRST_DUPLICATE) {
                        // another duplicate, add it to the list;
                        final long duplicateLocation = duplicateLocationFromRowKey(rightRowKeyForState);
                        final WritableRowSet duplicates = rightSideDuplicateRowSets.getUnsafe(duplicateLocation);
                        final long duplicateSize = duplicates.size();
                        final long inputKey = rowKeyChunk.get(chunkPosition);
                        final long newKey = addRightRowKeyToDuplicates(duplicates, inputKey, joinType);
                        Assert.eq(duplicateSize, "duplicateSize", duplicates.size() - 1, "duplicates.size() - 1");
                        if (inputKey == newKey) {
                            // we have a new output key for the LHS rows;
                            modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                        }
                    } else {
                        // we have a duplicate, how to handle it?;
                        final long inputKey = rowKeyChunk.get(chunkPosition);
                        if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
                            final long leftRowKeyForState = leftRowSet.getUnsafe(tableLocation).firstRowKey();
                            throw new IllegalStateException("Natural Join found duplicate right key for " + extractKeyStringFromSourceTable(leftRowKeyForState));
                        } else if (addOnly && joinType == NaturalJoinType.FIRST_MATCH) {
                            final long newKey = Math.min(rightRowKeyForState, inputKey);
                            if (newKey != rightRowKeyForState) {
                                rightRowKey.set(tableLocation, newKey);
                                modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                            }
                        } else if (addOnly && joinType == NaturalJoinType.LAST_MATCH) {
                            final long newKey = Math.max(rightRowKeyForState, inputKey);
                            if (newKey != rightRowKeyForState) {
                                rightRowKey.set(tableLocation, newKey);
                                modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                            }
                        } else {
                            // create a duplicate rowset and add the new row to it;
                            final long duplicateLocation = allocateDuplicateLocation();
                            final WritableRowSet duplicates = RowSetFactory.fromKeys(rightRowKeyForState, inputKey);
                            rightSideDuplicateRowSets.set(duplicateLocation, duplicates);
                            rightRowKey.set(tableLocation, rowKeyFromDuplicateLocation(duplicateLocation));
                            modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                        }
                    }
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void modifyByRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long oldRightRow = rightRowKey.getUnsafe(tableLocation);
                    modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_MODIFY_PROBE));
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    protected void applyRightShift(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            long shiftDelta, NaturalJoinModifiedSlotTracker modifiedSlotTracker,
            RightIncrementalNaturalJoinStateManagerTypedBase.ProbeContext pc) {
        final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final int k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (!isStateEmpty(leftRowSet.getUnsafe(tableLocation))) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    final long existingRightRowKey = rightRowKey.getUnsafe(tableLocation);
                    final long keyToShift = rowKeyChunk.get(chunkPosition);
                     if (existingRightRowKey == keyToShift - shiftDelta) {
                        rightRowKey.set(tableLocation, keyToShift);
                        modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT));
                    } else if (existingRightRowKey <= FIRST_DUPLICATE) {
                        final long duplicateLocation = duplicateLocationFromRowKey(existingRightRowKey);
                        if (shiftDelta < 0) {
                            final WritableRowSet duplicates = rightSideDuplicateRowSets.getUnsafe(duplicateLocation);
                            shiftOneKey(duplicates, keyToShift, shiftDelta);
                        } else {
                            pc.pendingShifts.set(pc.pendingShiftPointer++, (long)duplicateLocation);
                            pc.pendingShifts.set(pc.pendingShiftPointer++, keyToShift);;
                        }
                        modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT));
                    } else {
                        throw Assert.statementNeverExecuted("Could not find existing index for shifted right row");
                    }
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
        }
    }

    private static int hash(int k0) {
        int hash = IntChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(RowSet state) {
        return state == null;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final int[] destKeyArray0 = new int[tableSize];
        final Object[] destState = new Object[tableSize];
        Arrays.fill(destState, null);
        final int [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final Object [] originalStateArray = (Object[])leftRowSet.getArray();
        leftRowSet.setArray(destState);
        final long [] oldRightRowKey = rightRowKey.getArray();
        final long [] destRightRowKey = new long[tableSize];
        rightRowKey.setArray(destRightRowKey);
        final long [] oldModifiedCookie = modifiedTrackerCookieSource.getArray();
        final long [] destModifiedCookie = new long[tableSize];
        modifiedTrackerCookieSource.setArray(destModifiedCookie);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final RowSet currentStateValue = (RowSet)originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final int k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty((RowSet)destState[destinationTableLocation])) {
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
