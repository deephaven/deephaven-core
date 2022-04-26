package io.deephaven.engine.table.impl.naturaljoin;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.util.QueryConstants;

public class TypedNaturalJoinFactory {
    public static void staticBuildLeftFound(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("leftHashSlots.set(hashSlotOffset++, (long)tableLocation)");
    }

    public static void staticBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("mainRightRowKey.set(tableLocation, NO_RIGHT_STATE_VALUE)");
        builder.addStatement("leftHashSlots.set(hashSlotOffset++, (long)tableLocation)");
    }

    public static void staticBuildRightFound(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("mainRightRowKey.set(tableLocation, DUPLICATE_RIGHT_STATE)");
    }

    public static void staticBuildRightInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long rightRowKeyToInsert = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("mainRightRowKey.set(tableLocation, rightRowKeyToInsert)");
    }

    public static void staticProbeDecorateLeftFound(CodeBlock.Builder builder) {
        builder.beginControlFlow("if (rightRowKey == DUPLICATE_RIGHT_STATE)");
        builder.addStatement(
                "throw new IllegalStateException(\"More than one right side mapping for \" + $T.extractKeyStringFromChunks(chunkTypes, sourceKeyChunks, chunkPosition))",
                ChunkUtils.class);
        builder.endControlFlow();
        builder.addStatement("leftRedirections.set(hashSlotOffset++, rightRowKey)");
    }

    public static void staticProbeDecorateLeftMissing(CodeBlock.Builder builder) {
        builder.addStatement("leftRedirections.set(hashSlotOffset++, $T.NULL_ROW_KEY)", RowSet.class);
    }

    public static void staticProbeDecorateRightFound(CodeBlock.Builder builder) {
        builder.beginControlFlow("if (existingStateValue != NO_RIGHT_STATE_VALUE)");
        builder.addStatement(
                "throw new IllegalStateException(\"More than one right side mapping for \" + $T.extractKeyStringFromChunks(chunkTypes, sourceKeyChunks, chunkPosition))",
                ChunkUtils.class);
        builder.endControlFlow();
        builder.addStatement("final long rightRowKeyToInsert = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("mainRightRowKey.set(tableLocation, rightRowKeyToInsert)");
    }

    public static void rightIncrementalRehashSetup(CodeBlock.Builder builder) {
        builder.addStatement("final long [] oldRightRowKey = rightRowKey.getArray()");
        builder.addStatement("final long [] destRightRowKey = new long[tableSize]");
        builder.addStatement("rightRowKey.setArray(destRightRowKey)");

        builder.addStatement("final long [] oldModifiedCookie = modifiedTrackerCookieSource.getArray()");
        builder.addStatement("final long [] destModifiedCookie = new long[tableSize]");
        builder.addStatement("rightRowKey.setArray(destModifiedCookie)");
    }

    public static void rightIncrementalMoveMain(CodeBlock.Builder builder) {
        builder.addStatement("destRightRowKey[destinationTableLocation] = oldRightRowKey[sourceBucket]");
        builder.addStatement("destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket]");
    }

    public static void rightIncrementalBuildLeftFound(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long leftRowKey = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("leftRowSet.getUnsafe(tableLocation).insert(leftRowKey)");
    }

    public static void rightIncrementalBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long leftRowKey = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("leftRowSet.set(tableLocation, $T.fromKeys(leftRowKey))", RowSetFactory.class);
    }

    public static void rightIncrementalRightFound(CodeBlock.Builder builder) {
        builder.addStatement("final long rightRowKeyForState = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition))");
        builder.beginControlFlow("if (rightRowKeyForState != $T.NULL_ROW_KEY && rightRowKeyForState != $T.NULL_LONG)", RowSet.class, QueryConstants.class);
        // TODO: LET THE USER KNOW WHAT BROKE
        builder.addStatement(
                "throw new IllegalStateException(\"Duplicate right hand side row for state (TODO: WHAT STATE?)\")");
        builder.endControlFlow();
    }

    public static void rightIncrementalRemoveFound(CodeBlock.Builder builder) {
        builder.addStatement("final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, $T.NULL_ROW_KEY)", RowSet.class);
        builder.addStatement("$T.eq(oldRightRow, $S, rowKeyChunk.get(chunkPosition), $S)", Assert.class, "oldRightRow", "rowKeyChunk.get(chunkPosition)");
        builder.addStatement("modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, $T.FLAG_RIGHT_CHANGE))", NaturalJoinModifiedSlotTracker.class);
    }

    public static void rightIncrementalAddFound(CodeBlock.Builder builder) {
        builder.addStatement("final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition))", RowSet.class);
        builder.beginControlFlow("if (oldRightRow != $T.NULL_ROW_KEY && oldRightRow != $T.NULL_LONG)", RowSet.class, QueryConstants.class);
        // TODO: LET THE USER KNOW WHAT BROKE
        builder.addStatement(
                "throw new IllegalStateException(\"Duplicate right hand side row for state (TODO: WHAT STATE?)\")");
        builder.endControlFlow();
        builder.addStatement("modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, $T.FLAG_RIGHT_CHANGE))", NaturalJoinModifiedSlotTracker.class);
    }

    public static void rightIncrementalModify(CodeBlock.Builder builder) {
        builder.addStatement("final long oldRightRow = rightRowKey.getUnsafe(tableLocation)", RowSet.class);
        builder.addStatement("$T.eq(oldRightRow, $S, rowKeyChunk.get(chunkPosition), $S)", Assert.class, "oldRightRow", "rowKeyChunk.get(chunkPosition)");
        builder.addStatement("modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, $T.FLAG_RIGHT_MODIFY_PROBE))", NaturalJoinModifiedSlotTracker.class);
    }

    public static void rightIncrementalShift(CodeBlock.Builder builder) {
        builder.addStatement("final long oldRightRow = rightRowKey.getAndSetUnsafe(tableLocation, rowKeyChunk.get(chunkPosition))", RowSet.class);
        builder.addStatement("$T.eq(oldRightRow - shiftDelta, $S, rowKeyChunk.get(chunkPosition), $S)", Assert.class, "oldRightRow - shiftDelta", "rowKeyChunk.get(chunkPosition)");
        builder.addStatement("modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, oldRightRow, $T.FLAG_RIGHT_SHIFT))", NaturalJoinModifiedSlotTracker.class);
    }

    public static void rightIncrementalMissing(CodeBlock.Builder builder) {
        // we don't care, this could be a right hand side that had no left value
    }
}
