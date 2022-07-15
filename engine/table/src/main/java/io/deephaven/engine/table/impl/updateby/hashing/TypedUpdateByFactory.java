/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.updateby.hashing;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;
import io.deephaven.engine.table.impl.util.ChunkUtils;

public class TypedUpdateByFactory {
    public static void addOnlyBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("// map the existing bucket to this chunk position");
        builder.addStatement("outputPositions.set(chunkPosition, rowState)");
    }

    public static void addOnlyBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("// create a new bucket and put it in the hash slot");
        builder.addStatement("final int outputPosForLocation = outputPositionOffset.getAndIncrement()");
        builder.addStatement("stateSource.set(tableLocation, outputPosForLocation)");
        builder.addStatement("// map the new bucket to this chunk position");
        builder.addStatement("outputPositions.set(chunkPosition, outputPosForLocation)");
    }

    public static void addOnlyRehashSetup(CodeBlock.Builder builder) {}

    public static void addOnlyMoveMainFull(CodeBlock.Builder builder) {}

    public static void addOnlyMoveMainAlternate(CodeBlock.Builder builder) {}

    public static void incrementalBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        builder.addStatement("// map the existing bucket to this chunk position");
        builder.addStatement("outputPositions.set(chunkPosition, rowState)");
    }

    public static void incrementalBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("// create a new bucket and put it in the hash slot");
        builder.addStatement("final int outputPosForLocation = outputPositionOffset.getAndIncrement()");
        builder.addStatement("stateSource.set(tableLocation, outputPosForLocation)");
        builder.addStatement("// map the new bucket to this chunk position");
        builder.addStatement("outputPositions.set(chunkPosition, outputPosForLocation)");
    }

    public static void incrementalRehashSetup(CodeBlock.Builder builder) {}

    public static void incrementalMoveMainFull(CodeBlock.Builder builder) {}

    public static void incrementalMoveMainAlternate(CodeBlock.Builder builder) {}

    public static void incrementalProbeFound(HasherConfig<?> hasherConfig, boolean alternate,
            CodeBlock.Builder builder) {
        if (!alternate) {
            builder.addStatement("// map the existing bucket to this chunk position");
            builder.addStatement("outputPositions.set(chunkPosition, rowState)");
            // builder.beginControlFlow("if (sequentialBuilders != null)");
            // builder.addStatement("final long cookie = getCookieMain(tableLocation)");
            // builder.addStatement("hashSlots.set(cookie, (long)tableLocation | mainInsertMask)");
            // builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders,
            // rowKeyChunk.get(chunkPosition))");
            // builder.nextControlFlow("else");
            // builder.addStatement("addRightIndex(tableLocation, rowKeyChunk.get(chunkPosition), rowState)");
            // builder.endControlFlow();
        } else {
            builder.addStatement("// map the existing bucket (from alternate) to this chunk position");
            builder.addStatement("outputPositions.set(chunkPosition, rowState)");
            // builder.beginControlFlow("if (sequentialBuilders != null)");
            // builder.addStatement("final long cookie = getCookieAlternate(alternateTableLocation)");
            // builder.addStatement("hashSlots.set(cookie, (long)alternateTableLocation | alternateInsertMask)");
            // builder.addStatement("addToSequentialBuilder(cookie, sequentialBuilders,
            // rowKeyChunk.get(chunkPosition))");
            // builder.nextControlFlow("else");
            // builder.addStatement(
            // "addAlternateRightIndex(alternateTableLocation, rowKeyChunk.get(chunkPosition), rowState)");
            // builder.endControlFlow();
        }
    }

    public static void incrementalProbeMissing(CodeBlock.Builder builder) {
        builder.addStatement("// throw exception if the bucket isn't found");
        builder.addStatement(
                "throw new IllegalStateException(\"Failed to find main aggregation slot for key \" + $T.extractKeyStringFromChunks(chunkTypes, sourceKeyChunks, chunkPosition))",
                ChunkUtils.class);
    }
}
