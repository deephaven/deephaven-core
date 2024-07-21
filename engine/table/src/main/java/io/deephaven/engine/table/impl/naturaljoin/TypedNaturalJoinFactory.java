//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public class TypedNaturalJoinFactory {

    public static final String FIRST_DUPLICATE = "FIRST_DUPLICATE";

    public static void staticBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("leftHashSlots.set(hashSlotOffset++, tableLocation)");
    }

    public static void staticBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("mainRightRowKey.set(tableLocation, NO_RIGHT_STATE_VALUE)");
        builder.addStatement("leftHashSlots.set(hashSlotOffset++, tableLocation)");
    }

    public static void staticBuildRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("mainRightRowKey.set(tableLocation, DUPLICATE_RIGHT_STATE)");
    }

    public static void staticBuildRightInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long rightRowKeyToInsert = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("mainRightRowKey.set(tableLocation, rightRowKeyToInsert)");
    }

    public static void staticProbeDecorateLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.beginControlFlow("if (rightRowKey == DUPLICATE_RIGHT_STATE)");
        // we only use the row key chunk for an error, so it is lazily created here
        builder.addStatement("final $T<$T> rowKeyChunk = rowSequence.asRowKeyChunk()", LongChunk.class,
                OrderedRowKeys.class);
        builder.addStatement(
                "throw new IllegalStateException(\"Natural Join found duplicate right key for \" + extractKeyStringFromSourceTable(rowKeyChunk.get(chunkPosition)))");
        builder.endControlFlow();
        builder.addStatement("leftRedirections.set(redirectionOffset++, rightRowKey)");
    }

    public static void staticProbeDecorateLeftMissing(CodeBlock.Builder builder) {
        builder.addStatement("leftRedirections.set(redirectionOffset++, $T.NULL_ROW_KEY)", RowSet.class);
    }

    public static void staticProbeDecorateRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.beginControlFlow("if (existingStateValue != NO_RIGHT_STATE_VALUE)");
        builder.addStatement("mainRightRowKey.set(tableLocation, DUPLICATE_RIGHT_STATE)");
        builder.addStatement("throw new $T(tableLocation)", DuplicateRightRowDecorationException.class);
        builder.nextControlFlow("else");
        builder.addStatement("final long rightRowKeyToInsert = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("mainRightRowKey.set(tableLocation, rightRowKeyToInsert)");
        builder.endControlFlow();
    }

    public static void rightIncrementalRehashSetup(CodeBlock.Builder builder) {
        builder.addStatement("final long [] oldRightRowKey = rightRowKey.getArray()");
        builder.addStatement("final long [] destRightRowKey = new long[tableSize]");
        builder.addStatement("rightRowKey.setArray(destRightRowKey)");

        builder.addStatement("final long [] oldModifiedCookie = modifiedTrackerCookieSource.getArray()");
        builder.addStatement("final long [] destModifiedCookie = new long[tableSize]");
        builder.addStatement("modifiedTrackerCookieSource.setArray(destModifiedCookie)");
    }

    public static void rightIncrementalMoveMain(CodeBlock.Builder builder) {
        builder.addStatement("destRightRowKey[destinationTableLocation] = oldRightRowKey[sourceBucket]");
        builder.addStatement("destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket]");
    }

    public static void rightIncrementalBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long leftRowKey = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("leftRowSet.getUnsafe(tableLocation).insert(leftRowKey)");
    }

    public static void rightIncrementalBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long leftRowKey = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("leftRowSet.set(tableLocation, $T.fromKeys(leftRowKey))", RowSetFactory.class);
        builder.addStatement("rightRowKey.set(tableLocation, $T.NULL_ROW_KEY)", RowSet.class);
        builder.addStatement("modifiedTrackerCookieSource.set(tableLocation, -1L)");
    }

    private static void initializeModifiedCookie(CodeBlock.Builder builder) {
        builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation, -1L)");
    }

    public static void rightIncrementalRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement(
                "final long rightRowKeyForState = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition))");
        builder.beginControlFlow("if (rightRowKeyForState != $T.NULL_ROW_KEY && rightRowKeyForState != $T.NULL_LONG)",
                RowSet.class, QueryConstants.class);
        builder.addStatement("final long leftRowKeyForState = leftRowSet.getUnsafe(tableLocation).firstRowKey()");
        builder.addStatement(
                "throw new IllegalStateException(\"Natural Join found duplicate right key for \" + extractKeyStringFromSourceTable(leftRowKeyForState))");
        builder.endControlFlow();
    }

    public static void rightIncrementalRemoveFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, $T.NULL_ROW_KEY)",
                RowSet.class);
        assertEq(builder, "oldRightRow", "rowKeyChunk.get(chunkPosition)");
        builder.addStatement(
                "modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, $T.FLAG_RIGHT_CHANGE))",
                NaturalJoinModifiedSlotTracker.class);
    }

    public static void rightIncrementalAddFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement(
                "final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition))",
                RowSet.class);
        builder.beginControlFlow("if (oldRightRow != $T.NULL_ROW_KEY && oldRightRow != $T.NULL_LONG)", RowSet.class,
                QueryConstants.class);
        builder.addStatement("final long leftRowKeyForState = leftRowSet.getUnsafe(tableLocation).firstRowKey()");
        builder.addStatement(
                "throw new IllegalStateException(\"Natural Join found duplicate right key for \" + extractKeyStringFromSourceTable(leftRowKeyForState))");
        builder.endControlFlow();
        builder.addStatement(
                "modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, $T.FLAG_RIGHT_CHANGE))",
                NaturalJoinModifiedSlotTracker.class);
    }

    public static void rightIncrementalModify(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long oldRightRow = rightRowKey.getUnsafe(tableLocation)", RowSet.class);
        assertEq(builder, "oldRightRow", "rowKeyChunk.get(chunkPosition)");
        builder.addStatement(
                "modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, $T.FLAG_RIGHT_MODIFY_PROBE))",
                NaturalJoinModifiedSlotTracker.class);
    }

    public static void rightIncrementalShift(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement(
                "final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition))",
                RowSet.class);
        assertEq(builder, "oldRightRow + shiftDelta", "rowKeyChunk.get(chunkPosition)");
        builder.addStatement(
                "modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, $T.FLAG_RIGHT_SHIFT))",
                NaturalJoinModifiedSlotTracker.class);
    }

    public static void incrementalRehashSetup(CodeBlock.Builder builder) {
        builder.addStatement("final Object [] oldLeftRowSet = mainLeftRowSet.getArray()");
        builder.addStatement("final Object [] destLeftRowSet = new Object[tableSize]");
        builder.addStatement("mainLeftRowSet.setArray(destLeftRowSet)");

        builder.addStatement("final long [] oldModifiedCookie = mainModifiedTrackerCookieSource.getArray()");
        builder.addStatement("final long [] destModifiedCookie = new long[tableSize]");
        builder.addStatement("mainModifiedTrackerCookieSource.setArray(destModifiedCookie)");
    }

    public static void incrementalMoveMainFull(CodeBlock.Builder builder) {
        builder.addStatement("destLeftRowSet[destinationTableLocation] = oldLeftRowSet[sourceBucket]");
        builder.addStatement("destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket]");
    }

    public static void incrementalMoveMainAlternate(CodeBlock.Builder builder) {
        builder.addStatement(
                "mainLeftRowSet.set(destinationTableLocation, alternateLeftRowSet.getUnsafe(locationToMigrate))");
        builder.addStatement("alternateLeftRowSet.set(locationToMigrate, null)");
        builder.addStatement("final long cookie  = alternateModifiedTrackerCookieSource.getUnsafe(locationToMigrate)");
        builder.addStatement("mainModifiedTrackerCookieSource.set(destinationTableLocation, cookie)");
        builder.addStatement("alternateModifiedTrackerCookieSource.set(locationToMigrate, -1L)");
        builder.addStatement(
                "modifiedSlotTracker.moveTableLocation(cookie, locationToMigrate, mainInsertMask | destinationTableLocation);");
    }

    public static void incrementalBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        checkForDuplicateErrorLeftDecorate(builder, "rowKeyChunk.get(chunkPosition)", "rightRowKeyForState");
        if (alternate) {
            builder.addStatement(
                    "alternateLeftRowSet.getUnsafe(alternateTableLocation).insert(rowKeyChunk.get(chunkPosition))");
        } else {
            builder.addStatement("mainLeftRowSet.getUnsafe(tableLocation).insert(rowKeyChunk.get(chunkPosition))");
        }
    }

    public static void incrementalBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("mainLeftRowSet.set(tableLocation, $T.fromKeys(rowKeyChunk.get(chunkPosition)))",
                RowSetFactory.class);
        builder.addStatement("mainRightRowKey.set(tableLocation, $T.NULL_ROW_KEY)", RowSet.class);
        builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation, -1L)");
    }

    public static void incrementalRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        final String sourceType = getSourceType(alternate);
        final String tableLocation = getTableLocation(alternate);
        builder.beginControlFlow("if (existingRightRowKey == $T.NULL_ROW_KEY)", RowSet.class);
        builder.addStatement("$LRightRowKey.set($L, rowKeyChunk.get(chunkPosition))", sourceType, tableLocation);
        builder.nextControlFlow("else if (existingRightRowKey <= $L)", FIRST_DUPLICATE);
        builder.addStatement("final long duplicateLocation = duplicateLocationFromRowKey(existingRightRowKey)");
        builder.addStatement(
                "rightSideDuplicateRowSets.getUnsafe(duplicateLocation).insert(rowKeyChunk.get(chunkPosition))");
        builder.nextControlFlow("else", RowSet.class);
        builder.addStatement("final long duplicateLocation = allocateDuplicateLocation()");
        builder.addStatement(
                "rightSideDuplicateRowSets.set(duplicateLocation, $T.fromKeys(existingRightRowKey, rowKeyChunk.get(chunkPosition)))",
                RowSetFactory.class);
        builder.addStatement("$LRightRowKey.set($L, rowKeyFromDuplicateLocation(duplicateLocation))", sourceType,
                tableLocation);
        builder.endControlFlow();
    }

    private static void checkForDuplicateError(CodeBlock.Builder builder, String sourceType, String tableLocation) {
        builder.addStatement("final $T leftRowsForState = $LLeftRowSet.getUnsafe($L)", RowSet.class, sourceType,
                tableLocation);
        builder.beginControlFlow("if (!leftRowsForState.isEmpty())");
        builder.addStatement(
                "throw new IllegalStateException(\"Natural Join found duplicate right key for \" + extractKeyStringFromSourceTable(leftRowsForState.firstRowKey()))");
        builder.endControlFlow();
    }

    private static void checkForDuplicateErrorLeftDecorate(CodeBlock.Builder builder, String leftRowKey,
            String rightRowState) {
        builder.beginControlFlow("if ($L <= $L)", rightRowState, FIRST_DUPLICATE);
        builder.addStatement(
                "throw new IllegalStateException(\"Natural Join found duplicate right key for \" + extractKeyStringFromSourceTable($L))",
                leftRowKey);
        builder.endControlFlow();
    }

    public static void incrementalRightInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("mainLeftRowSet.set(tableLocation, $T.empty())", RowSetFactory.class);
        builder.addStatement("mainRightRowKey.set(tableLocation, rowKeyChunk.get(chunkPosition))");
        initializeModifiedCookie(builder);
    }

    public static void incrementalRemoveRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        final String sourceType = alternate ? "alternate" : "main";
        final String tableLocation = alternate ? "alternateTableLocation" : "tableLocation";

        builder.beginControlFlow("if (existingRightRowKey <= $L)", FIRST_DUPLICATE);
        builder.addStatement("final long duplicateLocation = duplicateLocationFromRowKey(existingRightRowKey)");
        builder.addStatement("final $T duplicates = rightSideDuplicateRowSets.getUnsafe(duplicateLocation)",
                WritableRowSet.class);
        builder.addStatement("final long duplicateSize = duplicates.size()");
        builder.addStatement("duplicates.remove(rowKeyChunk.get(chunkPosition))");
        assertEq(builder, "duplicateSize", "duplicates.size() + 1");
        builder.beginControlFlow("if (duplicates.size() == 1)");
        builder.addStatement("$LRightRowKey.set($L, duplicates.firstRowKey())", sourceType, tableLocation);
        builder.addStatement("freeDuplicateLocation(duplicateLocation)");
        builder.endControlFlow();
        builder.nextControlFlow("else if (existingRightRowKey != rowKeyChunk.get(chunkPosition))");
        builder.addStatement("$T.statementNeverExecuted($S)", Assert.class,
                "Could not find existing right row in state");
        builder.nextControlFlow("else");
        // we need to check if our left hand side is empty at this location, if so then we must mark the location as a
        // tombstone and reduce the number of entries in the table
        builder.addStatement("final boolean leftEmpty = $LLeftRowSet.getUnsafe($L).isEmpty()", sourceType,
                tableLocation);
        builder.beginControlFlow("if (leftEmpty)");
        builder.addStatement("$LRightRowKey.set($L, $L)", sourceType, tableLocation, hasherConfig.tombstoneStateName);
        builder.addStatement("liveEntries--");
        builder.nextControlFlow("else");
        builder.addStatement("$LRightRowKey.set($L, $T.NULL_ROW_KEY)", sourceType, tableLocation, RowSet.class);
        builder.endControlFlow();
        modifyCookie(builder, sourceType, tableLocation, "FLAG_RIGHT_CHANGE");
        builder.endControlFlow();
    }

    public static void incrementalRemoveRightMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw $T.statementNeverExecuted($S)", Assert.class,
                "Could not find existing state for removed right row");
    }

    public static void incrementalRightFoundUpdate(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        final String sourceType = getSourceType(alternate);
        final String tableLocation = getTableLocation(alternate);

        builder.beginControlFlow("if (existingRightRowKey == $T.NULL_ROW_KEY)", RowSet.class);
        builder.addStatement("$LRightRowKey.set($L, rowKeyChunk.get(chunkPosition))", sourceType, tableLocation);
        modifyCookie(builder, sourceType, tableLocation, "FLAG_RIGHT_CHANGE");
        builder.nextControlFlow("else if (existingRightRowKey <= $L)", FIRST_DUPLICATE);
        builder.addStatement("final long duplicateLocation = duplicateLocationFromRowKey(existingRightRowKey)");
        builder.addStatement("final $T duplicates = rightSideDuplicateRowSets.getUnsafe(duplicateLocation)",
                WritableRowSet.class);
        builder.addStatement("final long duplicateSize = duplicates.size()");
        builder.addStatement("duplicates.insert(rowKeyChunk.get(chunkPosition))");
        assertEq(builder, "duplicateSize", "duplicates.size() - 1");
        builder.nextControlFlow("else");
        builder.addStatement("final long duplicateLocation = allocateDuplicateLocation()");
        builder.addStatement(
                "rightSideDuplicateRowSets.set(duplicateLocation, $T.fromKeys(existingRightRowKey, rowKeyChunk.get(chunkPosition)))",
                RowSetFactory.class);
        builder.addStatement("$LRightRowKey.set($L, rowKeyFromDuplicateLocation(duplicateLocation))", sourceType,
                tableLocation);
        modifyCookie(builder, sourceType, tableLocation, "FLAG_RIGHT_CHANGE");
        builder.endControlFlow();
    }

    private static void modifyCookie(CodeBlock.Builder builder, String sourceType, String tableLocation, String flag) {
        builder.addStatement(
                "$LModifiedTrackerCookieSource.set($L, modifiedSlotTracker.addMain($LModifiedTrackerCookieSource.getUnsafe($L), $LInsertMask | $L, existingRightRowKey, $T.$L))",
                sourceType, tableLocation, sourceType, tableLocation, sourceType, tableLocation,
                NaturalJoinModifiedSlotTracker.class, flag);
    }

    public static void incrementalRightInsertUpdate(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("mainLeftRowSet.set(tableLocation, $T.empty())", RowSetFactory.class);
        builder.addStatement("mainRightRowKey.set(tableLocation, rowKeyChunk.get(chunkPosition))");
        builder.addStatement(
                "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(-1, mainInsertMask | tableLocation, existingRightRowKey, $T.FLAG_RIGHT_CHANGE))",
                NaturalJoinModifiedSlotTracker.class);
    }

    public static void incrementalModifyRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        modifyCookie(builder, getSourceType(alternate), getTableLocation(alternate), "FLAG_RIGHT_CHANGE");
    }

    @NotNull
    private static String getTableLocation(boolean alternate) {
        return alternate ? "alternateTableLocation" : "tableLocation";
    }

    @NotNull
    private static String getSourceType(boolean alternate) {
        return alternate ? "alternate" : "main";
    }

    public static void incrementalModifyRightMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw $T.statementNeverExecuted($S)", Assert.class,
                "Could not find existing state for modified right row");
    }

    public static void incrementalApplyRightShiftMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw $T.statementNeverExecuted($S)", Assert.class,
                "Could not find existing state for shifted right row");
    }

    public static void incrementalLeftFoundUpdate(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        incrementalBuildLeftFound(hasherConfig, alternate, builder);
        builder.addStatement("leftRedirections.set(leftRedirectionOffset++, rightRowKeyForState)");
    }

    public static void incrementalLeftInsertUpdate(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        incrementalBuildLeftInsert(hasherConfig, builder);
        builder.addStatement("leftRedirections.set(leftRedirectionOffset++, $T.NULL_ROW_KEY)", RowSet.class);
    }

    public static void incrementalRemoveLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        final String sourceType = getSourceType(alternate);
        final String tableLocation = getTableLocation(alternate);
        builder.addStatement("final WritableRowSet left = $LLeftRowSet.getUnsafe($L)",
                sourceType, tableLocation);
        builder.addStatement("left.remove(rowKeyChunk.get(chunkPosition))");
        builder.beginControlFlow("if (left.isEmpty() && rightState == $T.NULL_ROW_KEY)", RowSet.class);
        // it is actually deleted
        builder.addStatement("$LRightRowKey.set($L, TOMBSTONE_RIGHT_STATE)", sourceType, tableLocation);
        builder.addStatement("liveEntries--");
        builder.endControlFlow();
    }

    public static void incrementalRemoveLeftMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw $T.statementNeverExecuted($S)", Assert.class,
                "Could not find existing state for removed left row");
    }

    public static void incrementalShiftLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        final String tableLocation = getTableLocation(alternate);
        builder.addStatement("final $T leftRowSetForState = $LLeftRowSet.getUnsafe($L)", WritableRowSet.class,
                getSourceType(alternate), tableLocation);
        builder.addStatement("final long keyToShift = rowKeyChunk.get(chunkPosition)");
        builder.beginControlFlow("if (shiftDelta < 0)");
        builder.addStatement("shiftOneKey(leftRowSetForState, keyToShift, shiftDelta)");
        builder.nextControlFlow("else");
        addPendingShift(alternate, builder, tableLocation);
        builder.endControlFlow();
    }

    public static void incrementalApplyRightShift(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        final String sourceType = getSourceType(alternate);
        final String tableLocation = getTableLocation(alternate);

        builder.addStatement("final long keyToShift = rowKeyChunk.get(chunkPosition)");
        builder.beginControlFlow("if (existingRightRowKey == keyToShift - shiftDelta)");
        builder.addStatement("$LRightRowKey.set($L, keyToShift)", sourceType, tableLocation);
        modifyCookie(builder, sourceType, tableLocation, "FLAG_RIGHT_SHIFT");
        builder.nextControlFlow("else if (existingRightRowKey <= $L)", FIRST_DUPLICATE);
        builder.addStatement("final long duplicateLocation = duplicateLocationFromRowKey(existingRightRowKey)");
        builder.beginControlFlow("if (shiftDelta < 0)");
        builder.addStatement("final $T duplicates = rightSideDuplicateRowSets.getUnsafe(duplicateLocation)",
                WritableRowSet.class);
        builder.addStatement("shiftOneKey(duplicates, keyToShift, shiftDelta)");
        builder.nextControlFlow("else");
        addPendingShift(false, builder, "duplicateLocation");
        builder.endControlFlow();
        builder.nextControlFlow("else");
        builder.addStatement("throw $T.statementNeverExecuted($S)", Assert.class,
                "Could not find existing index for shifted right row");
        builder.endControlFlow();
    }

    private static void addPendingShift(boolean alternate, CodeBlock.Builder builder, String tableLocation) {
        if (alternate) {
            builder.addStatement(
                    "pc.pendingShifts.set(pc.pendingShiftPointer++, (long)($T.ALTERNATE_SWITCH_MASK | $L))",
                    AlternatingColumnSource.class, tableLocation);
        } else {
            builder.addStatement("pc.pendingShifts.set(pc.pendingShiftPointer++, (long)$L)", tableLocation);
        }
        builder.addStatement("pc.pendingShifts.set(pc.pendingShiftPointer++, keyToShift)");
    }

    private static void assertEq(CodeBlock.Builder builder, String lhs, String rhs) {
        builder.addStatement("$T.eq($L, $S, $L, $S)", Assert.class, lhs, lhs, rhs, rhs);
    }

    public static void incrementalShiftLeftMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw $T.statementNeverExecuted($S)", Assert.class,
                "Could not find existing state for shifted left row");
    }
}
