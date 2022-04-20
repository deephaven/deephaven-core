package io.deephaven.engine.table.impl.naturaljoin;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;
import io.deephaven.engine.table.impl.util.ChunkUtils;

public class TypedNaturalJoinFactory {
    public static void staticNaturalJoinMoveMain(CodeBlock.Builder builder) {}

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
        builder.addStatement("throw new IllegalStateException(\"More than one right side mapping for \" + $T.extractKeyStringFromChunks(chunkTypes, sourceKeyChunks, chunkPosition))", ChunkUtils.class);
        builder.endControlFlow();
        builder.addStatement("leftRedirections.set(hashSlotOffset++, rightRowKey)");
    }

    public static void staticProbeDecorateLeftMissing(CodeBlock.Builder builder) {
        builder.addStatement("leftRedirections.set(hashSlotOffset++, $T.NULL_ROW_KEY)", RowSet.class);
    }
    
    public static void staticProbeDecorateRightFound(CodeBlock.Builder builder) {
        builder.beginControlFlow("if (existingStateValue != NO_RIGHT_STATE_VALUE)");
        builder.addStatement("throw new IllegalStateException(\"More than one right side mapping for \" + $T.extractKeyStringFromChunks(chunkTypes, sourceKeyChunks, chunkPosition))", ChunkUtils.class);
        builder.endControlFlow();
        builder.addStatement("final long rightRowKeyToInsert = rowKeyChunk.get(chunkPosition)");
        builder.addStatement("mainRightRowKey.set(tableLocation, rightRowKeyToInsert)");
    }
}
