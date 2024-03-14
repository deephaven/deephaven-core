//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.asofjoin;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;
import io.deephaven.util.QueryConstants;

public class TypedAsOfJoinFactory {
    public static void staticBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("addLeftKey(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("addLeftKey(tableLocation, rowKeyChunk.get(chunkPosition))");
        builder.addStatement("rightRowSetSource.set(tableLocation, $T.builderSequential())", RowSetFactory.class);
    }

    public static void staticBuildRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("addRightKey(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticBuildRightInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("addRightKey(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticProbeDecorateLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long indexKey = rowKeyChunk.get(chunkPosition)");
        builder.beginControlFlow("if (addLeftKey(tableLocation, indexKey) && hashSlots != null)");
        builder.addStatement("hashSlots.set(hashSlotOffset.getAndIncrement(), tableLocation)");
        builder.addStatement("foundBuilder.addKey(indexKey)");
        builder.endControlFlow();
    }

    public static void staticProbeDecorateRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("addRightKey(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticRehashSetup(CodeBlock.Builder builder) {
        builder.addStatement("final Object [] oldLeftState = leftRowSetSource.getArray()");
        builder.addStatement("final Object [] destLeftState = new Object[tableSize]");
        builder.addStatement("leftRowSetSource.setArray(destLeftState)");
    }

    public static void staticMoveMainFull(CodeBlock.Builder builder) {
        builder.addStatement("destLeftState[destinationTableLocation] = oldLeftState[sourceBucket]");
    }

    public static void rightIncrementalRehashSetup(CodeBlock.Builder builder) {
        builder.addStatement("final Object [] oldLeftSource = leftRowSetSource.getArray()");
        builder.addStatement("final Object [] destLeftSource = new Object[tableSize]");
        builder.addStatement("leftRowSetSource.setArray(destLeftSource)");

        builder.addStatement("final Object [] oldRightSource = rightRowSetSource.getArray()");
        builder.addStatement("final Object [] destRightSource = new Object[tableSize]");
        builder.addStatement("rightRowSetSource.setArray(destRightSource)");

        builder.addStatement("final long [] oldModifiedCookie = mainCookieSource.getArray()");
        builder.addStatement("final long [] destModifiedCookie = new long[tableSize]");
        builder.addStatement("Arrays.fill(destModifiedCookie, $T.NULL_LONG)", QueryConstants.class);
        builder.addStatement("mainCookieSource.setArray(destModifiedCookie)");
    }

    public static void rightIncrementalMoveMainFull(CodeBlock.Builder builder) {
        builder.addStatement("destLeftSource[destinationTableLocation] = oldLeftSource[sourceBucket]");
        builder.addStatement("destRightSource[destinationTableLocation] = oldRightSource[sourceBucket]");
        builder.addStatement("hashSlots.set(oldModifiedCookie[sourceBucket], destinationTableLocation)");
        builder.addStatement("destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket]");
    }

    public static void rightIncrementalMoveMainAlternate(CodeBlock.Builder builder) {
        builder.addStatement(
                "leftRowSetSource.set(destinationTableLocation, alternateLeftRowSetSource.getUnsafe(locationToMigrate))");
        builder.addStatement("alternateLeftRowSetSource.set(locationToMigrate, null)");
        builder.addStatement(
                "rightRowSetSource.set(destinationTableLocation, alternateRightRowSetSource.getUnsafe(locationToMigrate))");
        builder.addStatement("alternateRightRowSetSource.set(locationToMigrate, null)");

        builder.addStatement("final long cookie  = alternateCookieSource.getUnsafe(locationToMigrate)");
        builder.addStatement("migrateCookie(cookie, destinationTableLocation)");
    }

    public static void rightIncrementalBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        if (!alternate) {
            builder.addStatement("final long cookie = getCookieMain(tableLocation)");
            builder.addStatement("assert hashSlots != null");
            builder.addStatement("hashSlots.set(cookie, tableLocation | mainInsertMask)");

            builder.beginControlFlow("if (sequentialBuilders != null)");
            builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
            builder.nextControlFlow("else");
            builder.addStatement("addLeftKey(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
            builder.endControlFlow();
        } else {
            builder.addStatement("final long cookie = getCookieAlternate(alternateTableLocation)");
            builder.addStatement("hashSlots.set(cookie, alternateTableLocation | alternateInsertMask)");

            builder.beginControlFlow("if (sequentialBuilders != null)");
            builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
            builder.nextControlFlow("else");
            builder.addStatement(
                    "addAlternateLeftKey(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState)");
            builder.endControlFlow();
        }
    }

    public static void rightIncrementalBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long cookie = makeCookieMain(tableLocation)");
        builder.addStatement("hashSlots.set(cookie, tableLocation | mainInsertMask)");

        builder.beginControlFlow("if (sequentialBuilders != null)");
        builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
        builder.addStatement("stateSource.set(tableLocation, (byte)(ENTRY_RIGHT_IS_EMPTY | ENTRY_LEFT_IS_EMPTY))");
        builder.nextControlFlow("else");
        builder.addStatement("addLeftKey(tableLocation, rowKeyChunk.get(chunkPosition), (byte) 0)");
        builder.endControlFlow();
    }

    public static void rightIncrementalRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        if (!alternate) {
            builder.addStatement("final long cookie = getCookieMain(tableLocation)");
            builder.addStatement("hashSlots.set(cookie, tableLocation | mainInsertMask)");

            builder.beginControlFlow("if (sequentialBuilders != null)");
            builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
            builder.nextControlFlow("else");
            builder.addStatement("addRightKey(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
            builder.endControlFlow();
        } else {
            builder.addStatement("final long cookie = getCookieAlternate(alternateTableLocation)");
            builder.addStatement("hashSlots.set(cookie, alternateTableLocation | alternateInsertMask)");

            builder.beginControlFlow("if (sequentialBuilders != null)");
            builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
            builder.nextControlFlow("else");
            builder.addStatement(
                    "addAlternateRightKey(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState)");
            builder.endControlFlow();
        }
    }

    public static void rightIncrementalRightInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long cookie = makeCookieMain(tableLocation)");
        builder.addStatement("hashSlots.set(cookie, tableLocation | mainInsertMask)");

        builder.beginControlFlow("if (sequentialBuilders != null)");
        builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
        builder.addStatement("stateSource.set(tableLocation, (byte)(ENTRY_RIGHT_IS_EMPTY | ENTRY_LEFT_IS_EMPTY))");
        builder.nextControlFlow("else");
        builder.addStatement("addRightKey(tableLocation, rowKeyChunk.get(chunkPosition), (byte) 0)");
        builder.endControlFlow();
    }

    public static void rightIncrementalProbeDecorateRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        if (!alternate) {
            builder.beginControlFlow("if (sequentialBuilders != null)");
            builder.addStatement("final long cookie = getCookieMain(tableLocation)");
            builder.addStatement("hashSlots.set(cookie, tableLocation | mainInsertMask)");
            builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
            builder.nextControlFlow("else");
            builder.addStatement("addRightKey(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
            builder.endControlFlow();
        } else {
            builder.beginControlFlow("if (sequentialBuilders != null)");
            builder.addStatement("final long cookie = getCookieAlternate(alternateTableLocation)");
            builder.addStatement("hashSlots.set(cookie, alternateTableLocation | alternateInsertMask)");
            builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
            builder.nextControlFlow("else");
            builder.addStatement(
                    "addAlternateRightKey(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState)");
            builder.endControlFlow();

        }
    }
}
