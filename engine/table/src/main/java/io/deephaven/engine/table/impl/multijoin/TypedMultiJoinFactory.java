/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.multijoin;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;

public class TypedMultiJoinFactory {
    public static void staticBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.add("// staticBuildLeftInsert\n");
        builder.addStatement("final long outputKey = numEntries - 1");
        builder.addStatement("slotToOutputRow.set(tableLocation, outputKey)");
        builder.addStatement("tableRedirSource.set(outputKey, rowKeyChunk.get(chunkPosition))");
        for (int ii = 0; ii < hasherConfig.chunkTypes.length; ii++) {
            builder.addStatement("outputKeySources[" + ii + "].set(outputKey, k" + ii + ")");
        }
        builder.add("// end\n");
    }

    public static void staticBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.add("// staticBuildLeftFound\n");
        builder.beginControlFlow("if (tableRedirSource.getLong(sentinel) != EMPTY_RIGHT_STATE)");
        builder.addStatement(
                "throw new IllegalStateException(\"Duplicate key found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
        builder.endControlFlow();
        builder.addStatement("tableRedirSource.set(sentinel, rowKeyChunk.get(chunkPosition))");
        builder.add("// end\n");
    }

    public static void staticRehashSetup(CodeBlock.Builder builder) {
        builder.add("// staticRehashSetup\n");
        builder.add("// end\n");
    }

    public static void staticMoveMainFull(CodeBlock.Builder builder) {
        builder.add("// staticMoveMainFull\n");
        builder.add("// end\n");
    }

    private static void initializeModifiedCookie(CodeBlock.Builder builder) {
        builder.add("// initializeModifiedCookie\n");
        builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation, -1L)");
        builder.add("// end\n");
    }

    public static void incrementalRehashSetup(CodeBlock.Builder builder) {
        builder.add("// incrementalRehashSetup\n");
        builder.addStatement("final long [] oldModifiedCookie = mainModifiedTrackerCookieSource.getArray()");
        builder.addStatement("final long [] destModifiedCookie = new long[tableSize]");
        builder.addStatement("mainModifiedTrackerCookieSource.setArray(destModifiedCookie)");
        builder.add("// end\n");
    }

    public static void incrementalMoveMainFull(CodeBlock.Builder builder) {
        builder.add("// incrementalMoveMainFull\n");
        builder.addStatement("destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket]");
        builder.add("// end\n");
    }

    public static void incrementalMoveMainAlternate(CodeBlock.Builder builder) {
        builder.add("// incrementalMoveMainAlternate\n");
        builder.addStatement("final long cookie  = alternateModifiedTrackerCookieSource.getUnsafe(locationToMigrate)");
        builder.addStatement("mainModifiedTrackerCookieSource.set(destinationTableLocation, cookie)");
        builder.addStatement("alternateModifiedTrackerCookieSource.set(locationToMigrate, -1L)");
        // builder.addStatement(
        // "modifiedSlotTracker.moveTableLocation(cookie, locationToMigrate, mainInsertMask |
        // destinationTableLocation);");
        // builder.addStatement(
        // "modifiedSlotTracker.moveTableLocation(cookie, locationToMigrate, destinationTableLocation);");
        builder.add("// end\n");
    }

    public static void incrementalBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.add("// incrementalBuildLeftFound\n");
        builder.beginControlFlow("if (tableRedirSource.getLong(slotValue) != EMPTY_RIGHT_STATE)");
        builder.addStatement(
                "throw new IllegalStateException(\"Duplicate key found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
        builder.endControlFlow();
        builder.addStatement("tableRedirSource.set(slotValue, rowKeyChunk.get(chunkPosition))");

        builder.beginControlFlow("if (modifiedSlotTracker != null)");
        if (!alternate) {
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            // builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation,
            // modifiedSlotTracker.addSlot(cookie, mainInsertMask | slotValue, tableNumber, -1L, trackerFlag))");
            builder.addStatement(
                    "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, -1L, trackerFlag))");
        } else {
            builder.addStatement(
                    "final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation)");
            // builder.addStatement("alternateModifiedTrackerCookieSource.set(alternateTableLocation,
            // modifiedSlotTracker.addSlot(cookie, alternateInsertMask | slotValue, tableNumber, -1L, trackerFlag))");
            builder.addStatement(
                    "alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, -1L, trackerFlag))");
        }
        builder.endControlFlow();
        builder.add("// end\n");
    }

    public static void incrementalBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.add("// incrementalBuildLeftInsert\n");
        builder.addStatement("final long outputKey = numEntries - 1");
        builder.addStatement("slotToOutputRow.set(tableLocation, outputKey)");
        builder.addStatement("tableRedirSource.set(outputKey, rowKeyChunk.get(chunkPosition))");
        for (int ii = 0; ii < hasherConfig.chunkTypes.length; ii++) {
            builder.addStatement("outputKeySources[" + ii + "].set(outputKey, k" + ii + ")");
        }
        builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation, -1L)");
        builder.add("// end\n");
    }

    public static void incrementalModifyLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        if (!alternate) {
            builder.add("// incrementalModifyLeftFound-main\n");
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            // builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation,
            // modifiedSlotTracker.modifySlot(cookie, mainInsertMask | slotValue, tableNumber, trackerFlag))");
            builder.addStatement(
                    "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.modifySlot(cookie, slotValue, tableNumber, trackerFlag))");
            builder.add("// end\n");
        } else {
            builder.add("// incrementalModifyLeftFound-alternate\n");
            builder.addStatement(
                    "final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation)");
            // builder.addStatement("alternateModifiedTrackerCookieSource.set(alternateTableLocation,
            // modifiedSlotTracker.modifySlot(cookie, alternateInsertMask | slotValue, tableNumber, trackerFlag))");
            builder.addStatement(
                    "alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.modifySlot(cookie, slotValue, tableNumber, trackerFlag))");
            builder.add("// end\n");
        }
    }

    public static void incrementalModifyLeftMissing(CodeBlock.Builder builder) {
        builder.add("// incrementalModifyLeftMissing\n");
        builder.addStatement(
                "throw new IllegalStateException(\"Matching row not found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
        builder.add("// end\n");
    }

    public static void incrementalRemoveLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        if (!alternate) {
            builder.add("// incrementalRemoveLeftFound-main\n");
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            builder.addStatement("final long mappedRowKey = tableRedirSource.getUnsafe(slotValue)");
            builder.addStatement("tableRedirSource.set(slotValue, EMPTY_RIGHT_STATE)");
            builder.addStatement(
                    "Assert.eq(rowKeyChunk.get(chunkPosition), \"rowKey\", mappedRowKey, \"mappedRowKey\")");
            // builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation,
            // modifiedSlotTracker.addSlot(cookie, mainInsertMask | slotValue, tableNumber, mappedRowKey,
            // trackerFlag))");
            builder.addStatement(
                    "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag))");
            builder.add("// end\n");
        } else {
            builder.add("// incrementalRemoveLeftFound-alternate\n");
            builder.addStatement(
                    "final long cookie = alternateModifiedTrackerCookieSource.getUnsafe(alternateTableLocation)");
            builder.addStatement("final long mappedRowKey = tableRedirSource.getUnsafe(slotValue)");
            builder.addStatement("tableRedirSource.set(slotValue, EMPTY_RIGHT_STATE)");
            builder.addStatement(
                    "Assert.eq(rowKeyChunk.get(chunkPosition), \"rowKey\", mappedRowKey, \"mappedRowKey\")");
            // builder.addStatement("alternateModifiedTrackerCookieSource.set(alternateTableLocation,
            // modifiedSlotTracker.addSlot(cookie, alternateInsertMask | slotValue, tableNumber, mappedRowKey,
            // trackerFlag))");
            builder.addStatement(
                    "alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag))");
            builder.add("// end\n");
        }
    }

    public static void incrementalRemoveLeftMissing(CodeBlock.Builder builder) {
        builder.add("// incrementalRemoveLeftMissing\n");
        builder.addStatement(
                "throw new IllegalStateException(\"Matching row not found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
        builder.add("// end\n");
    }

    public static void incrementalShiftLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        if (!alternate) {
            builder.add("// incrementalShiftLeftFound-main\n");
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            builder.addStatement("final long mappedRowKey = tableRedirSource.getUnsafe(slotValue)");
            builder.addStatement(
                    "Assert.eq(rowKeyChunk.get(chunkPosition), \"rowKey\", mappedRowKey, \"mappedRowKey\")");
            builder.addStatement("tableRedirSource.set(slotValue, mappedRowKey + shiftDelta)");
            // builder.addStatement("mainModifiedTrackerCookieSource.set(tableLocation,
            // modifiedSlotTracker.addSlot(cookie, mainInsertMask | slotValue, tableNumber, mappedRowKey,
            // trackerFlag))");
            builder.addStatement(
                    "mainModifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag))");
            builder.add("// end\n");
        } else {
            builder.add("// incrementalShiftLeftFound-alternate\n");
            builder.addStatement("final long cookie = mainModifiedTrackerCookieSource.getUnsafe(tableLocation)");
            builder.addStatement("final long mappedRowKey = tableRedirSource.getUnsafe(slotValue)");
            builder.addStatement(
                    "Assert.eq(rowKeyChunk.get(chunkPosition), \"rowKey\", mappedRowKey, \"mappedRowKey\")");
            builder.addStatement("tableRedirSource.set(slotValue, mappedRowKey + shiftDelta)");
            // builder.addStatement("alternateModifiedTrackerCookieSource.set(alternateTableLocation,
            // modifiedSlotTracker.addSlot(cookie, alternateInsertMask | slotValue, tableNumber, mappedRowKey,
            // trackerFlag))");
            builder.addStatement(
                    "alternateModifiedTrackerCookieSource.set(alternateTableLocation, modifiedSlotTracker.addSlot(cookie, slotValue, tableNumber, mappedRowKey, trackerFlag))");
            builder.add("// end\n");
        }
    }

    public static void incrementalShiftLeftMissing(CodeBlock.Builder builder) {
        builder.add("// incrementalShiftLeftMissing\n");
        builder.addStatement(
                "throw new IllegalStateException(\"Matching row not found for \" + keyString(sourceKeyChunks, chunkPosition) + \" in table \" + tableNumber + \".\")");
        builder.add("// end\n");
    }
}
