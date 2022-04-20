package io.deephaven.engine.table.impl.naturaljoin;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;

public class TypedNaturalJoinFactory {
    public static void staticNaturalJoinMoveMain(CodeBlock.Builder builder) {}

    public static void staticBuildLeftFound(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }

    public static void staticBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }

    public static void staticBuildRightFound(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }

    public static void staticBuildRightInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }
}
