package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.CodeBlock;

public class TypedAggregationFactory {
    public static void buildInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildInsertCommon(hasherConfig, builder);
        builder.addStatement("outputPositionToHashSlot.set(outputPosition, tableLocation)");
    }

    public static void buildInsertIncremental(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildInsertCommon(hasherConfig, builder);
        builder.addStatement("outputPositionToHashSlot.set(outputPosition, mainInsertMask | tableLocation)");
        builder.addStatement("rowCountSource.set(outputPosition, 1L)");
    }

    public static void probeFound(CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }

    public static void removeProbeFound(CodeBlock.Builder builder) {
        probeFound(builder);

        builder.addStatement("final long oldRowCount = rowCountSource.getAndAddUnsafe(outputPosition, -1)");
        builder.addStatement("Assert.gtZero(oldRowCount, \"oldRowCount\")");
        builder.beginControlFlow("if (oldRowCount == 1)");
        builder.addStatement("emptiedPositions.add(outputPosition)");
        builder.endControlFlow();
    }

    public static void probeMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw new IllegalStateException($S)", "Missing value in probe");
    }

    static void buildFound(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }

    private static void buildFoundIncremental(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildFound(hasherConfig, builder);
        builder.addStatement("final long oldRowCount = rowCountSource.getAndAddUnsafe(outputPosition, 1)");
    }

    static void buildFoundIncrementalInitial(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildFoundIncremental(hasherConfig, builder);
        builder.addStatement("Assert.gtZero(oldRowCount, \"oldRowCount\")");
    }

    static void buildFoundIncrementalUpdate(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildFoundIncremental(hasherConfig, builder);
        builder.beginControlFlow("if (oldRowCount == 0)");
        builder.addStatement("reincarnatedPositions.add(outputPosition)");
        builder.endControlFlow();
    }

    private static void buildInsertCommon(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("outputPosition = nextOutputPosition.getAndIncrement()");
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
        builder.addStatement("$L.set(tableLocation, outputPosition)", hasherConfig.mainStateName);
    }

    static void staticAggMoveMain(CodeBlock.Builder builder) {
        builder.addStatement("outputPositionToHashSlot.set(currentStateValue, destinationTableLocation)");
    }

    static void incAggMoveMain(CodeBlock.Builder builder) {
        builder.addStatement(
                "outputPositionToHashSlot.set(currentStateValue, mainInsertMask | destinationTableLocation)");
    }
}
