package io.deephaven.engine.table.impl.asofjoin;

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
import io.deephaven.engine.table.impl.naturaljoin.DuplicateRightRowDecorationException;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public class TypedAsOfJoinFactory {
    public static void staticBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("addLeftIndex(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("addLeftIndex(tableLocation, rowKeyChunk.get(chunkPosition))");
        builder.addStatement("rightRowSetSource.set(tableLocation, $T.builderSequential())", RowSetFactory.class);
    }

    public static void staticBuildRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticBuildRightInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticProbeDecorateLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long indexKey = rowKeyChunk.get(chunkPosition)");
        builder.beginControlFlow("if (addLeftIndex(tableLocation, indexKey) && hashSlots != null)");
        builder.addStatement("hashSlots.set(hashSlotOffset.getAndIncrement(), (long)tableLocation)");
        builder.addStatement("foundBuilder.addKey(indexKey)");
        builder.endControlFlow();
    }

    public static void staticProbeDecorateRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void staticRehashSetup(CodeBlock.Builder builder) {
        builder.addStatement("final Object [] oldLeftState = leftRowSetSource.getArray()");
        builder.addStatement("final Object [] destLeftState = new Object[tableSize]");
        builder.addStatement("leftRowSetSource.setArray(destLeftState)");
    }

    public static void staticMoveMainFull(CodeBlock.Builder builder) {
        builder.addStatement("destLeftState[destinationTableLocation] = oldLeftState[sourceBucket]");
    }

    public static void incrementalRehashSetup(CodeBlock.Builder builder) {
        builder.addStatement("final Object [] oldLeftSource = leftRowSetSource.getArray()");
        builder.addStatement("final Object [] destLeftSource = new Object[tableSize]");
        builder.addStatement("leftRowSetSource.setArray(destLeftSource)");

        builder.addStatement("final Object [] oldRightSource = rightRowSetSource.getArray()");
        builder.addStatement("final Object [] destRightSource = new Object[tableSize]");
        builder.addStatement("rightRowSetSource.setArray(destRightSource)");

        builder.addStatement("final long [] oldModifiedCookie = cookieSource.getArray()");
        builder.addStatement("final long [] destModifiedCookie = new long[tableSize]");
        builder.addStatement("Arrays.fill(destModifiedCookie, $T.NULL_LONG)", QueryConstants.class);
        builder.addStatement("cookieSource.setArray(destModifiedCookie)");
    }

    public static void incrementalMoveMainFull(CodeBlock.Builder builder) {
        builder.addStatement("destLeftSource[destinationTableLocation] = oldLeftSource[sourceBucket]");
        builder.addStatement("destRightSource[destinationTableLocation] = oldRightSource[sourceBucket]");
        builder.addStatement("destModifiedCookie[destinationTableLocation] = oldModifiedCookie[sourceBucket]");
    }

    public static void incrementalMoveMainAlternate(CodeBlock.Builder builder) {
        builder.addStatement("leftRowSetSource.set(destinationTableLocation, alternateLeftRowSetSource.getUnsafe(locationToMigrate))");
        builder.addStatement("alternateLeftRowSetSource.set(locationToMigrate, null)");
        builder.addStatement("rightRowSetSource.set(destinationTableLocation, alternateRightRowSetSource.getUnsafe(locationToMigrate))");
        builder.addStatement("alternateRightRowSetSource.set(locationToMigrate, null)");

        builder.addStatement("final long cookie  = alternateCookieSource.getUnsafe(locationToMigrate)");
        builder.addStatement("cookieSource.set(destinationTableLocation, cookie)");
        builder.addStatement("alternateCookieSource.set(locationToMigrate, -1L)");
    }

    public static void incrementalBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long cookie = getCookieMain(tableLocation)");
        builder.addStatement("assert hashSlots != null");
        builder.addStatement("hashSlots.set(cookie, (long)tableLocation)");

        builder.beginControlFlow("if (sequentialBuilders != null)");
        builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
        builder.nextControlFlow("else");
        builder.addStatement("addLeftIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
        builder.endControlFlow();
    }

    public static void incrementalBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long cookie = getCookieMain(tableLocation)");
        builder.addStatement("assert hashSlots != null");
        builder.addStatement("hashSlots.set(cookie, (long)tableLocation)");

        builder.beginControlFlow("if (sequentialBuilders != null)");
        builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
        builder.nextControlFlow("else");
        builder.addStatement("addLeftIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
        builder.endControlFlow();
    }

    public static void incrementalRightFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("final long cookie = getCookieMain(tableLocation)");
        builder.addStatement("assert hashSlots != null");
        builder.addStatement("hashSlots.set(cookie, (long)tableLocation)");

        builder.beginControlFlow("if (sequentialBuilders != null)");
        builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
        builder.nextControlFlow("else");
        builder.addStatement("addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
        builder.endControlFlow();
    }

    public static void incrementalProbeDecorateRightFound(HasherConfig<?> hasherConfig, boolean alternate,
                                                     CodeBlock.Builder builder) {
        builder.addStatement("final long cookie = getCookieMain(tableLocation)");
        builder.beginControlFlow("if (hashSlots != null)");
        builder.addStatement("hashSlots.set(cookie, (long)tableLocation)");
        builder.endControlFlow();
        builder.beginControlFlow("if (sequentialBuilders != null)");
        builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
        builder.nextControlFlow("else");
        builder.addStatement("addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
        builder.endControlFlow();
    }

    public static void incrementalRightInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final long cookie = getCookieMain(tableLocation)");
        builder.addStatement("assert hashSlots != null");
        builder.addStatement("hashSlots.set(cookie, (long)tableLocation)");

        builder.beginControlFlow("if (sequentialBuilders != null)");
        builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders, rowKeyChunk.get(chunkPosition))");
        builder.nextControlFlow("else");
        builder.addStatement("addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
        builder.endControlFlow();
    }
}
