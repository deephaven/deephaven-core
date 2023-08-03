/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.multijoin;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;

public class TypedMultiJoinFactory {
    public static void staticBuildLeftInsert(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes,
            CodeBlock.Builder builder) {
        builder.addStatement("final int outputKey = numEntries - 1");
        builder.addStatement("slotToOutputRow.set(tableLocation, outputKey)");
        builder.addStatement("tableRedirSource.set(outputKey, rowKeyChunk.get(chunkPosition))");
        for (int ii = 0; ii < chunkTypes.length; ii++) {
            builder.addStatement("outputKeySources[" + ii + "].set((long)outputKey, k" + ii + ")");
        }
    }

    public static void staticBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.beginControlFlow("if (tableRedirSource.getLong(slotValue) != NO_REDIRECTION)");
        builder.addStatement(
                "throw new IllegalStateException(\"Duplicate key found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
        builder.endControlFlow();
        builder.addStatement("tableRedirSource.set(slotValue, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticRehashSetup(CodeBlock.Builder builder) {}

    public static void staticMoveMainFull(CodeBlock.Builder builder) {}

    public static void incrementalRehashSetup(CodeBlock.Builder builder) {
        builder.addStatement("final long [] oldModifiedCookie = mainModifiedTrackerCookieSource.getArray()");
        builder.addStatement("final long [] destModifiedCookie = new long[tableSize]");
        builder.addStatement("mainModifiedTrackerCookieSource.setArray(destModifiedCookie)");
    }

    public static void incrementalMoveMainFull(CodeBlock.Builder builder) {
        builder.addStatement("destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket]");
    }

    public static void incrementalMoveMainAlternate(CodeBlock.Builder builder) {
        builder.addStatement("final long cookie  = alternateModifiedTrackerCookieSource.getUnsafe(locationToMigrate)");
        builder.addStatement("mainModifiedTrackerCookieSource.set(destinationTableLocation, cookie)");
        builder.addStatement("alternateModifiedTrackerCookieSource.set(locationToMigrate, EMPTY_COOKIE_SLOT)");
    }

    public static void incrementalBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.beginControlFlow("if (tableRedirSource.getLong(slotValue) != NO_REDIRECTION)");
        builder.addStatement(
                "throw new IllegalStateException(\"Duplicate key found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
        builder.endControlFlow();
        builder.addStatement("tableRedirSource.set(slotValue, rowKeyChunk.get(chunkPosition))");

        builder.beginControlFlow("if (modifiedSlotTracker != null)");
        if (!alternate) {
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            builder.addStatement(
                    "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, RowSequence.NULL_ROW_KEY, trackerFlag))");
        } else {
            builder.addStatement(
                    "final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation)");
            builder.addStatement(
                    "alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, RowSequence.NULL_ROW_KEY, trackerFlag))");
        }
        builder.endControlFlow();
    }

    public static void incrementalBuildLeftInsert(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes,
            CodeBlock.Builder builder) {
        builder.addStatement("final int outputKey = numEntries - 1");
        builder.addStatement("slotToOutputRow.set(tableLocation, outputKey)");
        builder.addStatement("tableRedirSource.set(outputKey, rowKeyChunk.get(chunkPosition))");
        for (int ii = 0; ii < chunkTypes.length; ii++) {
            builder.addStatement("outputKeySources[" + ii + "].set((long)outputKey, k" + ii + ")");
        }
        builder.add("// NOTE: if there are other tables adding this row this cycle, we will add these into the slot\n");
        builder.add(
                "// tracker. However, when the modified slots are processed we will identify the output row as new\n");
        builder.add("// for this cycle and ignore the incomplete tracker data.\n");
        builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation, EMPTY_COOKIE_SLOT)");
    }

    public static void incrementalModifyLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        if (!alternate) {
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            builder.addStatement(
                    "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.modifySlot(cookie, slotValue, tableNumber, trackerFlag))");
        } else {
            builder.addStatement(
                    "final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation)");
            builder.addStatement(
                    "alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.modifySlot(cookie, slotValue, tableNumber, trackerFlag))");
        }
    }

    public static void incrementalModifyLeftMissing(CodeBlock.Builder builder) {
        builder.addStatement(
                "throw new IllegalStateException(\"Matching row not found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
    }

    public static void incrementalRemoveLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long mappedRowKey = tableRedirSource.getUnsafe(slotValue)");
        builder.addStatement("tableRedirSource.set(slotValue, NO_REDIRECTION)");
        builder.addStatement(
                "Assert.eq(rowKeyChunk.get(chunkPosition), \"rowKey\", mappedRowKey, \"mappedRowKey\")");
        if (!alternate) {
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            builder.addStatement(
                    "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag))");
        } else {
            builder.addStatement(
                    "final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation)");
            builder.addStatement(
                    "alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag))");
        }
    }

    public static void incrementalRemoveLeftMissing(CodeBlock.Builder builder) {
        builder.addStatement(
                "throw new IllegalStateException(\"Matching row not found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
    }

    public static void incrementalShiftLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long mappedRowKey = tableRedirSource.getUnsafe(slotValue)");
        builder.addStatement(
                "Assert.eq(rowKeyChunk.get(chunkPosition), \"rowKey\", mappedRowKey, \"mappedRowKey\")");
        builder.addStatement("tableRedirSource.set(slotValue, mappedRowKey + shiftDelta)");
        if (!alternate) {
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            builder.addStatement(
                    "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag))");
        } else {
            builder.addStatement(
                    "final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation)");
            builder.addStatement(
                    "alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag))");
        }
    }

    public static void incrementalShiftLeftMissing(CodeBlock.Builder builder) {
        builder.addStatement(
                "throw new IllegalStateException(\"Matching row not found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
    }
}
