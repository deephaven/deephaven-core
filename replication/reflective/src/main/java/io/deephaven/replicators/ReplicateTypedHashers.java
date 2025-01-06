//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.asofjoin.StaticAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.multijoin.IncrementalMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.multijoin.StaticMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.naturaljoin.RightIncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.naturaljoin.IncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.naturaljoin.StaticNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.updateby.hashing.UpdateByStateManagerTypedBase;
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class ReplicateTypedHashers {
    public static void main(String[] args) throws IOException {
        generatePackage(StaticChunkedOperatorAggregationStateManagerOpenAddressedBase.class, true);
        generatePackage(IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.class, true);
        generatePackage(StaticNaturalJoinStateManagerTypedBase.class, false);
        generatePackage(RightIncrementalNaturalJoinStateManagerTypedBase.class, false);
        generatePackage(IncrementalNaturalJoinStateManagerTypedBase.class, false);
        generatePackage(StaticAsOfJoinStateManagerTypedBase.class, false);
        generatePackage(RightIncrementalAsOfJoinStateManagerTypedBase.class, false);
        generatePackage(UpdateByStateManagerTypedBase.class, true);
        generatePackage(StaticMultiJoinStateManagerTypedBase.class, false);
        generatePackage(IncrementalMultiJoinStateManagerTypedBase.class, false);
    }

    private static void generatePackage(Class<?> baseClass, boolean doDouble) throws IOException {
        final HasherConfig<?> hasherConfig = TypedHasherFactory.hasherConfigForBase(baseClass);
        final String packageName = TypedHasherFactory.packageName(hasherConfig);
        final File sourceRoot = new File("engine/table/src/main/java/");

        final MethodSpec singleDispatch = generateSingleDispatch(baseClass, hasherConfig, packageName, sourceRoot);
        final MethodSpec doubleDispatch;
        if (doDouble) {
            doubleDispatch = generateDoubleDispatch(baseClass, hasherConfig, packageName, sourceRoot);
        } else {
            doubleDispatch = null;
        }

        final MethodSpec.Builder dispatchMethodBuilder = MethodSpec.methodBuilder("dispatch")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC);
        addHasherParametersAndReturnType(dispatchMethodBuilder, baseClass);
        dispatchMethodBuilder.addStatement(
                "final ChunkType[] chunkTypes = $T.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);",
                java.util.Arrays.class);
        dispatchMethodBuilder.beginControlFlow("if (chunkTypes.length == 1)");
        dispatchMethodBuilder.addStatement(
                "return dispatchSingle(chunkTypes[0], tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)");
        dispatchMethodBuilder.endControlFlow();
        if (doDouble) {
            dispatchMethodBuilder.beginControlFlow("if (chunkTypes.length == 2)");
            dispatchMethodBuilder.addStatement(
                    "return dispatchDouble(chunkTypes[0], chunkTypes[1], tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)");
            dispatchMethodBuilder.endControlFlow();
        }
        dispatchMethodBuilder.addStatement("return null");

        final String dispatcherName = "TypedHashDispatcher";
        final TypeSpec.Builder dispatchBuilder =
                TypeSpec.classBuilder(dispatcherName).addModifiers(Modifier.PUBLIC).addJavadoc(
                        "The TypedHashDispatcher returns a pre-generated and precompiled hasher instance suitable for the provided column sources, or null if there is not a precompiled hasher suitable for the specified sources.");
        dispatchBuilder.addMethod(
                MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).addComment("static use only").build());
        dispatchBuilder.addMethod(dispatchMethodBuilder.build());
        dispatchBuilder.addMethod(singleDispatch);
        if (doDouble) {
            dispatchBuilder.addMethod(doubleDispatch);
        }

        final JavaFile.Builder fileBuilder = JavaFile.builder(packageName, dispatchBuilder.build()).indent("    ");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending\n");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY\n");
        fileBuilder.addFileComment("****** Run " + ReplicateTypedHashers.class.getSimpleName()
                + " or ./gradlew replicateTypedHashers to regenerate\n");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("@formatter:off");

        final JavaFile dispatcherFile = fileBuilder.build();
        dispatcherFile.writeTo(sourceRoot);
    }

    @NotNull
    private static MethodSpec generateSingleDispatch(Class<?> baseClass,
            HasherConfig<?> hasherConfig, String packageName, File sourceRoot) throws IOException {
        final MethodSpec.Builder singleDispatchBuilder = MethodSpec.methodBuilder("dispatchSingle")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                .addParameter(ChunkType.class, "chunkType");
        addHasherParametersAndReturnType(singleDispatchBuilder, baseClass);

        singleDispatchBuilder.beginControlFlow("switch (chunkType)");
        singleDispatchBuilder.addCode("default: ");
        singleDispatchBuilder.addStatement("throw new UnsupportedOperationException($S + chunkType)",
                "Invalid chunk type for typed hashers: ");

        final ChunkType[] array = new ChunkType[1];
        for (ChunkType chunkType : ChunkType.values()) {
            if (chunkType == ChunkType.Boolean) {
                continue;
            }
            array[0] = chunkType;
            final String name = TypedHasherFactory.hasherName(hasherConfig, array);
            final JavaFile javaFile =
                    TypedHasherFactory.generateHasher(hasherConfig, array, name, Optional.empty());

            System.out.println("Generating " + name + " to " + sourceRoot);
            javaFile.writeTo(sourceRoot);
            singleDispatchBuilder.addCode("case " + chunkType.name() + ": ");
            singleDispatchBuilder.addStatement(
                    "return new $T(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)",
                    ClassName.get(packageName, name));
        }

        singleDispatchBuilder.endControlFlow();
        final MethodSpec singleDispatch = singleDispatchBuilder.build();
        return singleDispatch;
    }

    @NotNull
    private static MethodSpec generateDoubleDispatch(Class<?> baseClass,
            HasherConfig<?> hasherConfig, String packageName, File sourceRoot) throws IOException {
        final MethodSpec.Builder doubleDispatchBuilder = MethodSpec.methodBuilder("dispatchDouble")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                .addParameter(ChunkType.class, "chunkType0")
                .addParameter(ChunkType.class, "chunkType1");
        addHasherParametersAndReturnType(doubleDispatchBuilder, baseClass);

        doubleDispatchBuilder.beginControlFlow("switch (chunkType0)");
        doubleDispatchBuilder.addCode("default: ");
        doubleDispatchBuilder.addStatement("throw new UnsupportedOperationException($S + chunkType0)",
                "Invalid chunk type for typed hashers: ");

        final ChunkType[] array2 = new ChunkType[2];
        for (ChunkType chunkType0 : ChunkType.values()) {
            if (chunkType0 == ChunkType.Boolean) {
                continue;
            }
            array2[0] = chunkType0;

            doubleDispatchBuilder.addCode("case " + chunkType0.name() + ":");
            doubleDispatchBuilder.beginControlFlow("switch (chunkType1)");
            doubleDispatchBuilder.addCode("default: ");
            doubleDispatchBuilder.addStatement("throw new UnsupportedOperationException($S + chunkType1)",
                    "Invalid chunk type for typed hashers: ");

            for (ChunkType chunkType1 : ChunkType.values()) {
                if (chunkType1 == ChunkType.Boolean) {
                    continue;
                }

                array2[1] = chunkType1;

                final String name = TypedHasherFactory.hasherName(hasherConfig, array2);
                final JavaFile javaFile =
                        TypedHasherFactory.generateHasher(hasherConfig, array2, name, Optional.empty());

                System.out.println("Generating " + name + " to " + sourceRoot);
                javaFile.writeTo(sourceRoot);

                doubleDispatchBuilder.addCode("case " + chunkType1.name() + ": ");
                doubleDispatchBuilder.addStatement(
                        "return new $T(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)",
                        ClassName.get(packageName, name));
            }
            doubleDispatchBuilder.endControlFlow();
        }
        doubleDispatchBuilder.endControlFlow();
        MethodSpec doubleDispatch = doubleDispatchBuilder.build();
        return doubleDispatch;
    }

    private static void addHasherParametersAndReturnType(MethodSpec.Builder dispatchBuilder,
            Class<?> returnType) {
        dispatchBuilder
                .returns(returnType)
                .addParameter(ColumnSource[].class, "tableKeySources")
                .addParameter(ColumnSource[].class, "originalTableKeySources")
                .addParameter(int.class, "tableSize")
                .addParameter(double.class, "maximumLoadFactor")
                .addParameter(double.class, "targetLoadFactor");
    }
}
