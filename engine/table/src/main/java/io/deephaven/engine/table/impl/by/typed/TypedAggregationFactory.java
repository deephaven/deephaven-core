/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Modifier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TypedAggregationFactory {
    public static void buildInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildInsertCommon(hasherConfig, builder);
        builder.addStatement("outputPositionToHashSlot.set(outputPosition, tableLocation)");
    }

    public static void buildInsertIncremental(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildInsertCommon(hasherConfig, builder);
        builder.addStatement("outputPositionToHashSlot.set(outputPosition, mainInsertMask | tableLocation)");
    }

    public static void probeFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }

    public static void probeMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw new IllegalStateException($S)", "Missing value in probe");
    }

    static void buildFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
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

    @NotNull
    public static MethodSpec createFindPositionForKey(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("findPositionForKey").addParameter(Object.class, "key")
                .returns(int.class).addModifiers(Modifier.PUBLIC).addAnnotation(Override.class);
        if (chunkTypes.length != 1) {
            builder.addStatement("final Object [] ka = (Object[])key");
            for (int ii = 0; ii < chunkTypes.length; ++ii) {
                final Class<?> element = TypedHasherFactory.elementType(chunkTypes[ii]);
                unboxKey(builder, ii, element);
            }
        } else {
            final Class<?> element = TypedHasherFactory.elementType(chunkTypes[0]);
            unboxKey(builder, element);
        }

        builder.addStatement("int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");

        if (hasherConfig.openAddressed) {
            findPositionForKeyOpenAddressed(hasherConfig, chunkTypes, builder, false);
        } else {
            findPositionForKeyOverflow(hasherConfig, chunkTypes, builder);
        }
        return builder.build();
    }

    private static void findPositionForKeyOpenAddressed(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes,
            MethodSpec.Builder builder, boolean alternate) {

        final String tableLocationName = alternate ? "alternateTableLocation" : "tableLocation";
        final String firstLocationName = "first" + StringUtils.capitalize(tableLocationName);
        if (alternate) {
            builder.addStatement("int $L = hashToTableLocationAlternate(hash)", tableLocationName);
            builder.beginControlFlow("if ($L >= rehashPointer)", tableLocationName);
            builder.addStatement("return UNKNOWN_ROW");
            builder.endControlFlow();
        } else {
            builder.addStatement("int $L = hashToTableLocation(hash)", tableLocationName);
        }
        builder.addStatement("final int $L = $L", firstLocationName, tableLocationName);

        builder.beginControlFlow("while (true)");

        final String positionValueName = alternate ? "alternatePositionValue" : "positionValue";

        builder.addStatement("final int $L = $L.getUnsafe($L)", positionValueName,
                alternate ? hasherConfig.overflowOrAlternateStateName : hasherConfig.mainStateName,
                tableLocationName);
        builder.beginControlFlow("if ($L == $L)", positionValueName, hasherConfig.emptyStateName);

        if (hasherConfig.openAddressedAlternate && !alternate) {
            findPositionForKeyOpenAddressed(hasherConfig, chunkTypes, builder, true);
        } else {
            builder.addStatement("return UNKNOWN_ROW");
        }

        builder.endControlFlow();

        builder.beginControlFlow(
                "if (" + (alternate ? TypedHasherFactory.getEqualsStatementAlternate(chunkTypes)
                        : TypedHasherFactory.getEqualsStatement(chunkTypes)) + ")");
        builder.addStatement("return $L", positionValueName);
        builder.endControlFlow();

        final String nextLocationName = alternate ? "alternateNextTableLocation" : "nextTableLocation";
        builder.addStatement("$L = $L($L)", tableLocationName, nextLocationName, tableLocationName);
        builder.addStatement("$T.neq($L, $S, $L, $S)", Assert.class, tableLocationName, tableLocationName,
                firstLocationName, firstLocationName);

        builder.endControlFlow();
    }

    private static void findPositionForKeyOverflow(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes,
            MethodSpec.Builder builder) {
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.addStatement("final int positionValue = $L.getUnsafe(tableLocation)", hasherConfig.mainStateName);
        builder.beginControlFlow("if (positionValue == $L)", hasherConfig.emptyStateName);
        builder.addStatement("return -1");
        builder.endControlFlow();

        builder.beginControlFlow("if (" + TypedHasherFactory.getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("return positionValue");
        builder.endControlFlow();

        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");

        builder.beginControlFlow("while (overflowLocation != QueryConstants.NULL_INT)");
        builder.beginControlFlow("if (" + TypedHasherFactory.getEqualsStatementOverflow(chunkTypes) + ")");
        builder.addStatement("return $L.getUnsafe(overflowLocation)", hasherConfig.overflowOrAlternateStateName);
        builder.endControlFlow();

        builder.addStatement("overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)");
        builder.endControlFlow();
        builder.addStatement("return -1");
    }

    private static void unboxKey(MethodSpec.Builder builder, int ii, Class<?> element) {
        if (element == Object.class) {
            builder.addStatement("final $T k$L = ka[$L]", element, ii, ii);
        } else {
            builder.addStatement("final $T k$L = $T.unbox(($T)ka[$L])", element, ii, TypeUtils.class,
                    TypeUtils.getBoxedType(element), ii);
        }
    }

    private static void unboxKey(MethodSpec.Builder builder, Class<?> element) {
        if (element == Object.class) {
            builder.addStatement("final $T k0 = key", element);
        } else {
            builder.addStatement("final $T k0 = $T.unbox(($T)key)", element, TypeUtils.class,
                    TypeUtils.getBoxedType(element));
        }
    }
}
